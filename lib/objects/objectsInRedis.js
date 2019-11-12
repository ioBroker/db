/**
 * Object DB in REDIS - Client
 *
 * Copyright (c) 2018 ioBroker GmbH - All rights reserved.
 *
 * You may not to use, modify or distribute this package in any form without explicit agreement from ioBroker GmbH.
 *
 * Unauthorized using, modifying or copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential
 * Written by bluefox <dogafox@gmail.com>, 2014-2018
 *
 */
/* jshint -W097 */
/* jshint strict: false */
/* jslint node: true */
/* jshint -W061 */
'use strict';

const extend      = require('node.extend');
const Redis       = require('ioredis');
const tools       = require('../tools');
const fs          = require('fs');
const path        = require('path');
let   crypto      = require('crypto');

const utils       = require(path.join(getControllerDir() || __dirname, 'objectsUtils.js'));

/* @@tools.js@@ */
const scriptFiles = {};
/* @@lua@@ */

function getControllerDir() {
    const possibilities = ['iobroker.js-controller', 'ioBroker.js-controller'];
    let controllerPath = null;
    for (const pkg of possibilities) {
        try {
            const possiblePath = require.resolve(pkg);
            if (fs.existsSync(possiblePath)) {
                controllerPath = possiblePath;
                break;
            }
        }
        catch (_a) {
            /* not found */
        }
    }
    // Apparently, checking vs null/undefined may miss the odd case of controllerPath being ""
    // Thus we check for falsyness, which includes failing on an empty path
    if (!controllerPath) {
        controllerPath = path.join(__dirname, '..', '..', 'lib', 'objects');
        if (!fs.existsSync(controllerPath)) {
            controllerPath = null;
        }
    }
    else {
        controllerPath = path.join(path.dirname(controllerPath), 'lib', 'objects');
    }
    return controllerPath;
}

class ObjectsInRedis {

    constructor(settings) {
        const originalSettings = settings;
        this.settings = settings || {};
        this.redisNamespace = (this.settings.redisNamespace || (this.settings.connection && this.settings.connection.redisNamespace) || 'cfg') + '.';
        this.fileNamespace = this.redisNamespace + 'f.';
        this.fileNamespaceL = this.fileNamespace.length;
        this.objNamespace = this.redisNamespace + 'o.';
        this.objNamespaceL = this.objNamespace.length;
        const ioRegExp = new RegExp('^' + this.objNamespace.replace(/\./g, '\\.') + '[_A-Za-z0-9]+'); // cfg.o.[_A-Za-z0-9]+

        const onChange = this.settings.change; // on change handler

        this.stop = false;
        this.client = null;
        this.sub = null;
        this.preserveSettings = ['custom', 'smartName', 'material', 'habpanel', 'mobile'];
        this.defaultNewAcl = this.settings.defaultNewAcl || null;
        this.namespace = this.settings.namespace || this.settings.hostname || '';
        this.scripts = {};

        this.log = utils.getLogger(this.settings.logger);

        this.settings.connection = this.settings.connection || {};

        // limit max number of log entries in the list
        this.settings.connection.maxQueue = this.settings.connection.maxQueue || 1000;

        this.settings.connection.options = this.settings.connection.options || {};
        const retry_max_delay = this.settings.connection.options.retry_max_delay || 2000;
        const retry_max_count = this.settings.connection.options.retry_max_count || 19;
        this.settings.connection.options.retryStrategy = (reconnectCount) => {
            if (!ready && initError && ignoreErrors) return new Error('No more tries');
            if (this.stop) return new Error('Client has stopped ... no retries anymore');
            if (ready && reconnectCount >=retry_max_count) return new Error('Stop trying to reconnect');
            // A function that receives an options object as parameter including the retry attempt,
            // the total_retry_time indicating how much time passed since the last time connected,
            // the error why the connection was lost and the number of times_connected in total.
            // If you return a number from this function, the retry will happen exactly after that
            // time in milliseconds. If you return a non-number, no further retry will happen and
            // all offline commands are flushed with errors. Return an error to return that
            // specific error to all offline commands.

            if (!ready) return 300;
            return retry_max_delay;
            /*if (options.error.code === 'ECONNREFUSED') {
                // End reconnecting on a specific error and flush all commands with a individual error
                return new Error('The server refused the connection');
            }
            if (options.total_retry_time > 1000 * 60 * 60) {
                // End reconnecting after a specific timeout and flush all commands with a individual error
                return new Error('Retry time exhausted');
            }
            if (options.times_connected > 10) {
                // End reconnecting with built in error
                return undefined;
            }
            // reconnect after
            return Math.max(options.attempt * 100, 3000);*/
        };
        delete this.settings.connection.options.retry_max_delay;
        this.settings.connection.options.enableReadyCheck = true;

        let ready = false;
        let initError = false;
        let ignoreErrors = false;
        let connected = false;
        let reconnectCounter = 0;

        if (this.settings.connection.port === 0) { // Port = 0 means unix socket
            // initiate a unix socket connection
            this.settings.connection.options.path = this.settings.connection.host;
            this.log.debug(this.namespace + ' Redis Objects: Use File Socket for connection: ' + this.settings.connection.options.path);
        } else if (Array.isArray(this.settings.connection.host)) { // Host is an array means we use a sentinel
            const defaultPort = Array.isArray(this.settings.connection.port) ? null : this.settings.connection.port;

            this.settings.connection.options.sentinels = this.settings.connection.host.map((redisNode, idx) => ({
                host: redisNode,
                port: defaultPort || this.settings.connection.port[idx]
            }));

            this.settings.connection.options.name = this.settings.connection.sentinelName ? this.settings.connection.sentinelName : 'mymaster';
            this.log.debug(this.namespace + ' Redis Objects: Use Sentinel for connection: ' + this.settings.connection.options.name + ', ' + JSON.stringify(this.settings.connection.options.sentinels));
        } else {
            this.settings.connection.options.host = this.settings.connection.host;
            this.settings.connection.options.port = this.settings.connection.port;
            this.log.debug(this.namespace + ' Redis Objects: Use Redis connection: ' + this.settings.connection.options.host + ':' + this.settings.connection.options.port);
        }
        if (this.settings.connection.options.db === undefined) {
            this.settings.connection.options.db = 0;
        }
        if (this.settings.connection.options.family === undefined) {
            this.settings.connection.options.family = 0;
        }
        this.settings.connection.options.password = this.settings.connection.pass || null;

        this.client = new Redis(this.settings.connection.options);

        const fallbackToSocketIo = () => {
            this.stop = true;
            this.client.quit();
            ignoreErrors = true;

            const objectsFile = path.join(getControllerDir() || __dirname, 'objectsInMemClientSocketIo.js');
            this.log.silly(this.namespace + ' Initiate Fallback to socket.io Objects (' + objectsFile + ')');
            const ObjectsSocketIo = require(objectsFile);
            const _newObjects = new ObjectsSocketIo(originalSettings);
        };

        this.client.on('error', error => {
            if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' Redis ERROR Objects: (' + ignoreErrors + '/' + this.stop + ') ' + error.message + ' / ' + error.stack);
            if (this.stop) return;
            if (!ready) {
                initError = true;
                // Seems we have a socket.io server
                if (!ignoreErrors && error.message.startsWith('Protocol error, got "H" as reply type byte.')) {
                    fallbackToSocketIo();
                }
                return;
            }
            this.log.error(this.settings.namespace + ' ' + error.message);
        });

        this.client.on('end', () => {
            if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' Objects-Redis Event end (stop=' + this.stop + ')');
            if (ready && typeof this.settings.disconnected === 'function') this.settings.disconnected();
        });

        this.client.on('connect', () => {
            if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' Objects-Redis Event connect (stop=' + this.stop + ')');
            connected = true;
        });

        this.client.on('close', () => {
            if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' Objects-Redis Event close (stop=' + this.stop + ')');
            //if (ready && typeof this.settings.disconnected === 'function') this.settings.disconnected();
        });

        this.client.on('reconnecting', () => {
            if (connected && !ready && !initError && !ignoreErrors) reconnectCounter++;
            if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' Objects-Redis Event reconnect (reconnectCounter=' + reconnectCounter + ', stop=' + this.stop + ')');
            if (reconnectCounter > 2) { // fallback logic for nodejs <10
                fallbackToSocketIo();
                return;
            }
            connected = false;
            initError = false;
        });

        this.client.on('ready', () => {
            if (this.stop) return;
            initError = false;
            ignoreErrors = false;

            this.log.debug(this.namespace + ' Objects client ready ... initialize now');
            this.client.config('set', ['lua-time-limit', 10000], (err) => { // increase LUA timeout TODO needs better fix
                if (err) {
                    this.log.warn('Unable to increase LUA script timeout: ' + err);
                }
                if (!this.sub) {
                    this.log.debug(this.namespace + ' Objects create PubSub Client');
                    this.sub = new Redis(this.settings.connection.options);

                    if (typeof onChange === 'function') {
                        this.sub.on('pmessage', (pattern, channel, message) => {
                            setImmediate(() => {
                                this.log.silly(this.namespace + ' Objects redis pmessage ' + pattern + '/' + channel + ':' + message);
                                try {
                                    if (ioRegExp.test(channel)) {
                                        const id = channel.substring(this.objNamespaceL);
                                        try {
                                            const obj = message ? JSON.parse(message) : null;

                                            if (this.settings.controller &&
                                                id === 'system.config' &&
                                                obj &&
                                                obj.common &&
                                                obj.common.defaultNewAcl &&
                                                JSON.stringify(obj.common.defaultNewAcl) !== JSON.stringify(this.defaultNewAcl)) {
                                                this.defaultNewAcl = JSON.parse(JSON.stringify(obj.common.defaultNewAcl));
                                                this.setDefaultAcl(this.defaultNewAcl);
                                            }

                                            onChange(id, obj);
                                        } catch (e) {
                                            this.log.warn(`${this.namespace} Objects Cannot process pmessage ${id} - ${message}: ${e.message}`);
                                            this.log.warn(`${this.namespace} ${e.stack}`);
                                        }
                                    } else {
                                        this.log.warn(`${this.namespace} Objects Received unexpected pmessage: ${channel}`);
                                    }
                                } catch (e) {
                                    this.log.warn(this.namespace + ' Objects pmessage ' + channel + ' ' + JSON.stringify(message) + ' ' + e.message);
                                    this.log.warn(this.namespace + ' ' + e.stack);
                                }
                            });
                        });
                    }

                    this.sub.on('end', () => {
                        if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' Objects-Redis Event end sub (stop=' + this.stop + ')');
                        if (ready && typeof this.settings.disconnected === 'function') this.settings.disconnected();
                    });

                    this.sub.on('error', error => {
                        if (this.stop) return;
                        if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' PubSub client Objects No redis connection: ' + JSON.stringify(error));
                    });

                    if (this.settings.connection.enhancedLogging) {
                        this.sub.on('connect', () => {
                            this.log.silly(this.namespace + ' PubSub client Objects-Redis Event connect (stop=' + this.stop + ')');
                        });

                        this.client.on('close', () => {
                            this.log.silly(this.namespace + ' PubSub client Objects-Redis Event close (stop=' + this.stop + ')');
                        });

                        this.client.on('reconnecting', (reconnectCounter) => {
                            this.log.silly(this.namespace + ' PubSub client Objects-Redis Event reconnect (reconnectCounter=' + reconnectCounter + ', stop=' + this.stop + ')');
                        });
                    }

                    this.sub.on('ready', () => {
                        if (this.settings.connection.port === 0) {
                            this.log.debug(this.namespace + ' Objects connected to redis: ' + this.settings.connection.host);
                        } else {
                            this.log.debug(this.namespace + ' Objects connected to redis: ' + this.settings.connection.host + ':' + this.settings.connection.port);
                        }
                        // subscribe on system.config only if js-controller
                        if (this.settings.controller) {
                            this.sub.psubscribe(this.objNamespace + 'system.config');
                        }
                    });
                }

                this.log.debug(this.namespace + ' Objects client initialize lua scripts');
                this.loadLuaScripts(() => {
                    // init default new acl
                    this.client.get(this.objNamespace + 'system.config', (err, obj) => {
                        if (obj) {
                            try {
                                obj = JSON.parse(obj);
                            } catch (e) {
                                this.log.error(`${this.namespace} Cannot parse JSON system.config: ${obj}`);
                                obj = null;
                            }
                            if (obj && obj.common && obj.common.defaultNewAcl) {
                                this.defaultNewAcl = obj.common.defaultNewAcl;
                            }
                        }
                        else {
                            this.log.error(`${this.namespace} Cannot read system.config: ${obj} (OK when migrating or restoring)`);
                        }
                        !ready && typeof this.settings.connected === 'function' && this.settings.connected(this);
                        ready = true;
                    });
                });
            });
        });
    }

    getStatus() {
        return {type: 'redis', server: false};
    }

    normalizeFilename(name) {
        return name ? name.replace(/[\/]+/g, '/') : name;
    }

    // -------------- FILE FUNCTIONS -------------------------------------------
    _setBinaryState(id, data, callback) {
        if (!Buffer.isBuffer(data)) {
            data = Buffer.from(data);
        }
        this.client.set(id, data, callback);
    }
    _getBinaryState(id, callback) {
        if (typeof callback !== 'function') {
            return this.log.error(this.namespace + ' no callback found in _getBinaryState');
        }

        this.client.getBuffer(id, (err, data) => {
            if (!err && data) {
                callback(err, data);
            } else {
                callback(err);
            }
        });
    }
    _delBinaryState(id, callback) {
        this.client.del(id, () => typeof callback === 'function' && callback());
    }

    getFileId(id, name, isMeta) {
        name = this.normalizeFilename(name);
        // e.g. ekey.admin and admin/ekey.png
        if (id.match(/\.admin$/)) {
            if (name.match(/^admin\//)) {
                name = name.replace(/^admin\//, '');
            } else
            // e.g. ekey.admin and iobroker.ekey/admin/ekey.png
            if (name.match(/^iobroker.[-\d\w]\/admin\//i)) {
                name = name.replace(/^iobroker.[-\d\w]\/admin\//i, '');
            }
        }
        const normalized = utils.sanitizePath(id, name);
        if (!normalized) {
            this.log.debug(this.namespace + ' Invalid file path ' + id + '/' + name);
            return '';
        }
        id = normalized.id;
        name = normalized.name;

        return this.fileNamespace + id + '$%$' + name + (isMeta !== undefined ? (isMeta ? '$%$meta' : '$%$data') : '');
    }

    checkFile(id, name, options, flag, callback) {
        // read file settings from redis
        const fileId = this.getFileId(id, name, true);
        if (!fileId) {
            const fileOptions = {"notExists": true};
            if (utils.checkFile(fileOptions, options, flag, this.defaultNewAcl)) {
                return callback && callback(false, options, fileOptions); // NO error
            } else {
                return callback && callback(true, options); // error
            }
        }
        this.client.get(fileId, (err, fileOptions) => {
            fileOptions = fileOptions || '{"notExists": true}';
            try {
                fileOptions = JSON.parse(fileOptions);
            } catch (e) {
                this.log.error(`${this.namespace} Cannot parse JSON ${id}: ${fileOptions}`);
                fileOptions = {notExists: true};
            }

            if (utils.checkFile(fileOptions, options, flag, this.defaultNewAcl)) {
                return callback && callback(false, options, fileOptions); // NO error
            } else {
                return callback && callback(true, options); // error
            }
        });
    }

    checkFileRights(id, name, options, flag, callback) {
        return utils.checkFileRights(this, id, name, options, flag, callback);
    }

    _setDefaultAcl(ids, defaultAcl) {
        if (ids && ids.length) {
            const id = ids.shift();
            this.getObject(id, (err, obj) => {
                if (obj && !obj.acl) {
                    obj.acl = defaultAcl;
                    this.setObject(id, obj, null, () =>
                        setImmediate(() =>
                            this._setDefaultAcl(ids, defaultAcl)));
                } else {
                    setImmediate(() =>
                        this._setDefaultAcl(ids, defaultAcl));
                }
            });
        }
    }

    setDefaultAcl(defaultNewAcl) {
        this.defaultNewAcl = defaultNewAcl || {
            owner: utils.CONSTS.SYSTEM_ADMIN_USER,
            ownerGroup: utils.CONSTS.SYSTEM_ADMIN_GROUP,
            object: 0x664,
            state: 0x664,
            file: 0x664
        };
        // Get ALL Objects
        this.getKeys('*', (err, ids) => this._setDefaultAcl(ids, this.defaultNewAcl));
    }

    getUserGroup(user, callback) {
        return utils.getUserGroup(this, user, (error, user, userGroups, userAcl) => {
            if (error) this.log.error(this.namespace + ' ' + error);
            callback.call(this, user, userGroups, userAcl);
        });
    }

    insert(id, attName, ignore, options, obj, callback) {
        return utils.insert(this, id, attName, ignore, options, obj, callback);
    }

    _writeFile(id, name, data, options, callback, meta) {
        const ext         = name.match(/\.[^.]+$/);
        const mime        = utils.getMimeType(ext);
        const _mimeType   = mime.mimeType;
        const isBinary    = mime.isBinary;

        const metaID = this.getFileId(id, name, true);
        // virtual files only get Meta objects
        if (options.virtualFile) {
            meta = {
                notExists: true,
                virtualFile: true
            }; // Store file with flags as it would not exist
            this.client.set(metaID, JSON.stringify(meta), _err => callback && callback(_err));
        } else {
            if (!meta) {
                meta = {createdAt: Date.now()};
            }
            if (!meta.acl) {
                meta.acl = {
                    owner: options.user || (this.defaultNewAcl && this.defaultNewAcl.owner) || utils.CONSTS.SYSTEM_ADMIN_USER,
                    ownerGroup: options.group || (this.defaultNewAcl && this.defaultNewAcl.ownerGroup) || utils.CONSTS.SYSTEM_ADMIN_GROUP,
                    permissions: options.mode || (this.defaultNewAcl && this.defaultNewAcl.file) || 0x644
                };
            }
            meta.stats = {
                size: data.length
            };
            if (meta.hasOwnProperty('notExists')) {
                delete meta.notExists;
            }

            meta.mimeType = options.mimeType || _mimeType;
            meta.binary = isBinary;
            meta.acl.ownerGroup = meta.acl.ownerGroup || (this.defaultNewAcl && this.defaultNewAcl.ownerGroup) || utils.CONSTS.SYSTEM_ADMIN_GROUP;
            meta.modifiedAt = Date.now();

            this._setBinaryState(this.getFileId(id, name, false), data, err => this.client.set(metaID, JSON.stringify(meta), _err => callback && callback(err)));
        }
    }
    writeFile(id, name, data, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (typeof options === 'string') {
            options = {mimeType: options};
        }

        if (options && options.acl) {
            options.acl = null;
        }
        
        if (!name) {
            return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
        }

        if (name[0] === '/') {
            name = name.substring(1);
        }

        if (!callback) {
            return new Promise((resolve, reject) =>
                this.writeFile(id, name, data, options, err =>
                    err ? reject(err) : resolve()));
        }

        // If file yet exists => check the permissions
        return this.checkFileRights(id, name, options, utils.CONSTS.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                return this._writeFile(id, name, data, options, callback, meta);
            }
        });
    }

    _readFile(id, name, options, callback, meta) {
        if (meta.notExists) {
            return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
        }
        this._getBinaryState(this.getFileId(id, name, false), (err, buffer) => {
            const mimeType = meta && meta.mimeType;
            if (meta && !meta.binary && buffer) {
                buffer = buffer.toString();
            }
            callback(err, buffer, mimeType);
        });
    }
    readFile(id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options  = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }

        if (name[0] === '/') name = name.substring(1);

        if (!callback) {
            return new Promise((resolve, reject) => {
                this.readFile(id, name, options, (err, res, mimeType) =>
                    err ? reject(err) : resolve({data: res, mimeType: mimeType}));
            });
        }

        options = options || {};
        this.checkFileRights(id, name, options, utils.CONSTS.ACCESS_READ, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                return this._readFile(id, name, options, callback, meta);
            }
        });
    }

    _unlink(id, name, options, callback, meta) {
        if (meta && meta.notExists) {
            this._rm(id, name, options, callback);
            //typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
        } else {
            const metaID = this.getFileId(id, name, true);
            const dataID = this.getFileId(id, name, false);
            this._delBinaryState(dataID, _err => this.client.del(metaID, callback));
        }
    }
    unlink(id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options  = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        if (!name) {
            return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
        }
        if (name[0] === '/') {
            name = name.substring(1);
        }

        this.checkFileRights(id, name, options, utils.CONSTS.ACCESS_DELETE, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file['delete']) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    return this._unlink(id, name, options, callback, meta);
                }
            }
        });
    }
    delFile(id, name, options, callback) {
        return this.unlink(id, name, options, callback);
    }

    _readDir(id, name, options, callback) {
        name = this.normalizeFilename(name);
        if (id === '') { // special case for "root"
            const dirID = this.getFileId('*', '*');
            this.client.keys(dirID, (err, keys) => {
                if (!this.client) {
                    return callback(utils.ERRORS.ERROR_DB_CLOSED);
                }

                const result = [];
                if (!keys || !keys.length) {
                    callback(null, result);
                    return;
                }
                let lastDir;
                keys.sort().forEach(dir => {
                    dir = dir.substring(this.fileNamespaceL, dir.indexOf('$%$'));
                    if (dir !== lastDir) {
                        result.push({
                            file: dir,
                            stats: {},
                            isDir: true
                        });
                    }
                    lastDir = dir;
                });
                callback(err, result);
            });
            return;
        }
        const dirID = this.getFileId(id, name + (name.length ? '/' : '') + '*');
        this.client.keys(dirID, (err, keys) => {
            if (!this.client) {
                return callback(utils.ERRORS.ERROR_DB_CLOSED);
            }

            const start = dirID.indexOf('$%$') + 3;
            const end = '$%$meta'.length;

            const baseName = name + (name.length ? '/' : '');
            const dirs = [];
            const deepLevel = baseName.split('/').length;
            if (!keys || !keys.length) {
                return callback(utils.ERRORS.ERROR_NOT_FOUND, []);
            }
            keys = keys
                .sort()
                .filter(key => {
                    if (key.match(/\$%\$meta$/)) {
                        const parts = key.substr(start, key.length - end).split('/');
                        if (parts.length === deepLevel) {
                            return !key.includes('/_data.json$%$') && key !== '_data.json'; // sort out "virtual" files that are used to mark directories
                        } else {
                            const dir = parts[deepLevel - 1];
                            if (dirs.indexOf(dir) === -1) {
                                dirs.push(dir);
                            }
                        }
                    }
                });
            if (!keys.length) {
                const result = [];
                while (dirs.length) {
                    result.push({
                        file: dirs.shift(),
                        stats: {},
                        isDir: true
                    });
                }
                return callback(err, result);
            }

            // Check permissions
            this.client.mget(keys, (err, objs) => {
                if (err) {
                    return callback(err, objs);
                }
                const result = [];
                const dontCheck =
                    options.user === utils.CONSTS.SYSTEM_ADMIN_USER ||
                    options.group !== utils.CONSTS.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) !== -1);

                objs = objs || [];
                for (let i = 0; i < keys.length; i++) {
                    const file = keys[i].substring(start + baseName.length, keys[i].length - end);
                    while (dirs.length && dirs[0] < file) {
                        result.push({
                            file: dirs.shift(),
                            stats: {},
                            isDir: true
                        });
                    }

                    try {
                        objs[i] = JSON.parse(objs[i]);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON ${keys[i]}: ${objs[i]}`);
                        continue;
                    }
                    if (dontCheck || utils.checkObject(objs[i], options, utils.CONSTS.ACCESS_READ)) {
                        if (!objs[i] || objs[i].virtualFile) continue; // virtual file, ignore
                        objs[i].acl = objs[i].acl || {};
                        if (options.user !== utils.CONSTS.SYSTEM_ADMIN_USER && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) === -1) {
                            objs[i].acl.read  = !!(objs[i].acl.permissions & utils.CONSTS.ACCESS_EVERY_READ);
                            objs[i].acl.write = !!(objs[i].acl.permissions & utils.CONSTS.ACCESS_EVERY_WRITE);
                        }
                        else {
                            objs[i].acl.read  = true;
                            objs[i].acl.write = true;
                        }
                        result.push({
                            file: file,
                            stats: objs[i].stats,
                            isDir: false,
                            acl: objs[i].acl,
                            modifiedAt: objs[i].modifiedAt,
                            createdAt:  objs[i].createdAt
                        });
                    }
                }
                while (dirs.length) {
                    result.push({
                        file: dirs.shift(),
                        stats: {},
                        isDir: true
                    });
                }
                callback(null, result);
            });
        });
    }
    readDir(id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        // remove first and last
        if (name[0] === '/') name = name.substring(1);
        if (name[name.length - 1] === '/') name = name.substring(0, name.length - 1);

        this.checkFileRights(id, name, options, utils.CONSTS.ACCESS_READ, (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file.list) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    this._readDir(id, name, options, callback);
                }
            }
        });
    }

    _renameHelper(keys, oldBase, newBase, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id = keys.shift();
            this.client.rename(id.replace(/\$%\$meta$/, '$%$data'), id.replace(oldBase, newBase).replace(/\$%\$meta$/, '$%$data'), _err =>
                this.client.rename(id, id.replace(oldBase, newBase), _err =>
                    setImmediate(() => this._renameHelper(keys, oldBase, newBase, callback))));
        }
    }
    _rename(id, oldName, newName, options, callback, meta) {
        const oldMetaID = this.getFileId(id, oldName, true);
        const oldDataID = this.getFileId(id, oldName, false);
        const newMetaID = this.getFileId(id, newName, true);
        const newDataID = this.getFileId(id, newName, false);
        if (!meta) {
            callback && callback(utils.ERRORS.ERROR_DB_CLOSED);
        } else if (meta.notExists) {
            oldName = this.normalizeFilename(oldName);
            newName = this.normalizeFilename(newName);

            // it could be dir
            if (!oldName.endsWith('/*')) {
                oldName += '/*';
            } else if (oldName.endsWith('/')) {
                oldName += '*';
            }

            if (!newName.endsWith('/*')) {
                newName += '/*';
            } else if (newName.endsWith('/')) {
                newName += '*';
            }

            const oldBase = oldName.substring(0, oldName.length - 1);
            const newBase = newName.substring(0, newName.length - 1);
            const dirID = this.getFileId(id, oldName);
            this.client.keys(dirID, (err, keys) => {
                if (!this.client) {
                    return callback(utils.ERRORS.ERROR_DB_CLOSED);
                }
                if (err || !keys) {
                    callback && callback(utils.ERRORS.ERROR_NOT_FOUND);
                    return;
                }

                keys = keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                if (!keys.length) {
                    return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
                }
                // Check permissions
                this.client.mget(keys, (err, objs) => {
                    let result;
                    const dontCheck =
                        options.user === utils.CONSTS.SYSTEM_ADMIN_USER ||
                        options.group !== utils.CONSTS.SYSTEM_ADMIN_GROUP ||
                        (options.groups && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) !== -1);

                    objs = objs || [];
                    if (!dontCheck) {
                        result = [];
                        for (let i = 0; i < keys.length; i++) {
                            try {
                                objs[i] = JSON.parse(objs[i]);
                            } catch (e) {
                                this.log.error(`${this.namespace} Cannot parse JSON ${keys[i]}: ${objs[i]}`);
                                continue;
                            }
                            if (utils.checkObject(objs[i], options, utils.CONSTS.ACCESS_READ)) {
                                result.push(keys[i]);
                            }
                        }
                    } else {
                        result = keys;
                    }
                    this._renameHelper(result, oldBase, newBase, callback);
                });
            });
        } else {
            this.client.rename(oldDataID, newDataID, _err =>
                this.client.rename(oldMetaID, newMetaID, callback));
        }
    }
    rename(id, oldName, newName, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        if (!oldName.length || !newName.length) {
            callback && callback(utils.ERRORS.ERROR_NOT_FOUND);
            return;
        }
        if (oldName[0] === '/') oldName = oldName.substring(1);
        if (newName[0] === '/') newName = newName.substring(1);
        if (oldName[oldName.length - 1] === '/') oldName = oldName.substring(0, oldName.length - 1);
        if (newName[newName.length - 1] === '/') newName = newName.substring(0, newName.length - 1);

        this.checkFileRights(id, oldName, options, utils.CONSTS.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file.write) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    this._rename(id, oldName, newName, options, callback, meta);
                }
            }
        });
    }

    _touch(id, name, options, callback, meta) {
        const metaID = this.getFileId(id, name, true);
        if (!meta || meta.notExists) {
            callback && callback(utils.ERRORS.ERROR_NOT_FOUND);
        } else {
            meta.modifiedAt = Date.now();
            this.client.set(metaID, JSON.stringify(meta), callback);
        }
    }
    touch(id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        this.checkFileRights(id, name, options, utils.CONSTS.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                return this._touch(id, name, options, callback, meta);
            }
        });
    }

    _rmHelper(keys, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id = keys.shift();
            this._delBinaryState(id.replace(/\$%\$meta$/, '$%$data'), _err =>
                this.client.del(id, _err =>
                    setImmediate(() => this._rmHelper(keys, callback))));
        }
    }
    _rm(id, name, options, callback, meta) {
        if (meta && !meta.isDir) {
            // it is file
            const metaID = this.getFileId(id, name, true);
            const dataID = this.getFileId(id, name, false);
            this.delObject(dataID, _err => this.delObject(metaID, callback));
        } else {
            name = this.normalizeFilename(name);
            // it could be dir
            if (! name.endsWith('/*')) {
                name += '/*';
            }
            else if (name.endsWith('/')) {
                name += '*';
            }
            const dirID = this.getFileId(id, name);
            this.client.keys(dirID, (err, keys) => {
                if (!this.client) {
                    return callback(utils.ERRORS.ERROR_DB_CLOSED);
                }
                if (err || !keys) {
                    callback && callback(utils.ERRORS.ERROR_NOT_FOUND);
                    return;
                }

                keys = keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                if (!keys.length) {
                    return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
                }
                // Check permissions
                this.client.mget(keys, (err, objs) => {
                    let result;
                    const dontCheck =
                        options.user === utils.CONSTS.SYSTEM_ADMIN_USER ||
                        options.group !== utils.CONSTS.SYSTEM_ADMIN_GROUP ||
                        (options.groups && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) !== -1);

                    objs = objs || [];
                    if (!dontCheck) {
                        result = [];
                        for (let i = 0; i < keys.length; i++) {
                            try {
                                objs[i] = JSON.parse(objs[i]);
                            } catch (e) {
                                this.log.error(`${this.namespace} Cannot parse JSON ${keys[i]}: ${objs[i]}`);
                                continue;
                            }
                            if (utils.checkObject(objs[i], options, utils.CONSTS.ACCESS_READ)) {
                                result.push(keys[i]);
                            }
                        }
                    } else {
                        result = keys;
                    }
                    const files = result.map(key => {
                        const name = key.substring(this.fileNamespaceL + id.length + 3, key.length - 7);
                        const pos = name.lastIndexOf('/');
                        if (pos !== -1) {
                            return {file: name.substring(pos + 1), path: name.substring(0, pos)};
                        } else {
                            return {file: id, path: ''};
                        }
                    });
                    this._rmHelper(result, () => callback(null, files));
                });
            });
        }
    }
    rm(id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        this.checkFileRights(id, null, options, utils.CONSTS.ACCESS_DELETE, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file['delete']) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    return this._rm(id, name, options, callback, meta && meta.notExists ? null : meta);
                }
            }
        });
    }

    // simulate. redis has no dirs
    mkdir(id, dirName, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        dirName = this.normalizeFilename(dirName);
        if (dirName[0] === '/') dirName = dirName.substring(1);
        this.checkFileRights(id, dirName, options, utils.CONSTS.ACCESS_WRITE, (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file.write) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    // we create a dummy file (for file this file exists to store meta data)
                    options = options || {};
                    options.virtualFile = true; // this is a virtual File
                    const realName = dirName + (dirName.endsWith('/') ? '' : '/');
                    this.writeFile(id, realName + '_data.json', '', options, callback);
                }
            }
        });
    }

    _chownFileHelper(keys, metas, options, callback) {
        if (!keys || !keys.length) {
            callback && callback();
        } else {
            const id  = keys.shift();
            const meta = metas.shift();
            meta.acl.owner      = options.owner;
            meta.acl.ownerGroup = options.ownerGroup;
            this.client.set(id, JSON.stringify(meta), _err =>
                setImmediate(() => this._chownFileHelper(keys, metas, options, callback)));
        }
    }
    _chownFile(id, name, options, callback, meta) {
        if (!meta) {
            return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
        }
        name = this.normalizeFilename(name);
        if (!meta.isDir && !meta.notExists) {
            // it is file
            const metaID = this.getFileId(id, name, true);
            meta.acl.owner = options.owner;
            meta.acl.ownerGroup = options.ownerGroup;
            this.client.set(metaID, JSON.stringify(meta), err => {
                const nameArr = name.split('/');
                const file = nameArr.pop();
                const res = [{
                    path:       nameArr.join('/'),
                    file:       file,
                    stats:      meta.stats,
                    isDir:      false,
                    acl:        meta.acl || {},
                    modifiedAt: meta.modifiedAt,
                    createdAt:  meta.createdAt
                }];
                callback && callback(err, res);
            });
            return;
        }
        // it could be dir
        if (! name.endsWith('/*')) {
            name += '/*';
        }
        else if (name.endsWith('/')) {
            name += '*';
        }
        const dirID = this.getFileId(id, name);
        this.client.keys(dirID, (err, keys) => {
            if (!this.client) {
                return callback(utils.ERRORS.ERROR_DB_CLOSED);
            }
            if (err || !keys) {
                callback && callback(utils.ERRORS.ERROR_NOT_FOUND);
                return;
            }

            keys = keys
                .sort()
                .filter(key => key.match(/\$%\$meta$/));

            // Check permissions
            this.client.mget(keys, (err, metas) => {
                const dontCheck = options.user === utils.CONSTS.SYSTEM_ADMIN_USER ||
                    options.group !== utils.CONSTS.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) !== -1);
                const keysFiltered = [];
                const objsFiltered = [];
                const processed = [];
                const start = dirID.indexOf('$%$') + 3;
                const end = '$%$meta'.length;

                metas = metas || [];
                for (let i = 0; i < keys.length; i++) {
                    try {
                        metas[i] = JSON.parse(metas[i]);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON ${keys[i]}: ${metas[i]}`);
                        continue;
                    }
                    if (dontCheck || utils.checkObject(metas[i], options, utils.CONSTS.ACCESS_WRITE)) {
                        if (!metas[i] || metas[i].virtualFile) continue; // virtual file, ignore
                        keysFiltered.push(keys[i]);
                        objsFiltered.push(metas[i]);

                        const name = keys[i].substring(start, keys[i].length - end);
                        const nameArr = name.split('/');
                        const file = nameArr.pop();
                        processed.push({
                            path:       nameArr.join('/'),
                            file:       file,
                            stats:      metas[i].stats || {},
                            isDir:      false,
                            acl:        metas[i].acl || {},
                            modifiedAt: metas[i].modifiedAt,
                            createdAt:  metas[i].createdAt
                        });
                    }
                }
                this._chownFileHelper(keysFiltered, objsFiltered, options, err => {
                    callback && callback(err, processed);
                });
            });
        });
    }
    chownFile(id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        options = options || {};
        if (typeof options !== 'object') {
            options = {owner: options};
        }

        if (name[0] === '/') name = name.substring(1);

        if (!options.ownerGroup && options.group) options.ownerGroup = options.group;
        if (!options.owner      && options.user)  options.owner      = options.user;

        if (!options.owner) {
            this.log.error(this.namespace + ' user is not defined');
            typeof callback === 'function' && callback('invalid parameter');
            return;
        }

        if (!options.ownerGroup) {
            // get user group
            this.getUserGroup(options.owner, (user, groups /* , permissions */) => {
                if (!groups || !groups[0]) {
                    typeof callback === 'function' && callback('user "' + options.owner + '" belongs to no group');
                    return;
                } else {
                    options.ownerGroup = groups[0];
                }
                this.chownFile(id, name, options, callback);
            });
            return;
        }

        this.checkFileRights(id, name, options, utils.CONSTS.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file.write) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    return this._chownFile(id, name, options, callback, meta);
                }
            }
        });
    }

    _chmodFileHelper(keys, metas, options, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id   = keys.shift();
            const meta = metas.shift();
            meta.acl.permissions = options.mode;
            this.client.set(id, JSON.stringify(meta), _err =>
                setImmediate(() => this._chmodFileHelper(keys, metas, options, callback)));
        }
    }
    _chmodFile(id, name, options, callback, meta) {
        if (!meta) {
            return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
        }
        name = this.normalizeFilename(name);
        if (!meta.isDir && !meta.notExists) {
            // it is file
            const metaID = this.getFileId(id, name, true);
            meta.acl.permissions = options.mode;
            this.client.set(metaID, JSON.stringify(meta), err => {
                const nameArr = name.split('/');
                const file = nameArr.pop();
                const res = [{
                    path:       nameArr.join('/'),
                    file:       file,
                    stats:      meta.stats,
                    isDir:      false,
                    acl:        meta.acl || {},
                    modifiedAt: meta.modifiedAt,
                    createdAt:  meta.createdAt
                }];
                callback && callback(err, res);
            });
            return;
        }
        // it could be dir
        if (! name.endsWith('/*')) {
            name += '/*';
        }
        else if (name.endsWith('/')) {
            name += '*';
        }
        const dirID = this.getFileId(id, name);
        this.client.keys(dirID, (err, keys) => {
            if (!this.client) {
                return callback(utils.ERRORS.ERROR_DB_CLOSED);
            }
            if (err || !keys) {
                callback && callback(utils.ERRORS.ERROR_NOT_FOUND);
                return;
            }

            keys = keys
                .sort()
                .filter(key => key.match(/\$%\$meta$/));

            // Check permissions
            this.client.mget(keys, (err, objs) => {
                const dontCheck =
                    options.user === utils.CONSTS.SYSTEM_ADMIN_USER ||
                    options.group !== utils.CONSTS.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) !== -1);

                const keysFiltered = [];
                const objsFiltered = [];
                const processed = [];
                const start = dirID.indexOf('$%$') + 3;
                const end = '$%$meta'.length;

                objs = objs || [];
                for (let i = 0; i < keys.length; i++) {
                    try {
                        objs[i] = JSON.parse(objs[i]);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON ${keys[i]}: ${objs[i]}`);
                        continue;
                    }
                    if (dontCheck || utils.checkObject(objs[i], options, utils.CONSTS.ACCESS_WRITE)) {
                        if (!objs[i] || objs[i].virtualFile) continue; // virtual file, ignore
                        keysFiltered.push(keys[i]);
                        objsFiltered.push(objs[i]);

                        const name = keys[i].substring(start, keys[i].length - end);
                        const nameArr = name.split('/');
                        const file = nameArr.pop();
                        processed.push({
                            path:       nameArr.join('/'),
                            file:       file,
                            stats:      objs[i].stats,
                            isDir:      false,
                            acl:        objs[i].acl || {},
                            modifiedAt: objs[i].modifiedAt,
                            createdAt:  objs[i].createdAt
                        });
                    }
                }
                this._chmodFileHelper(keysFiltered, objsFiltered, options, err => {
                    callback && callback(err, processed);
                });
            });
        });
    }
    chmodFile(id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        options = options || {};

        if (name[0] === '/') name = name.substring(1);

        if (typeof options !== 'object') {
            options = {mode: options};
        }

        if (options.mode === undefined) {
            this.log.error(this.namespace + ' mode is not defined');
            typeof callback === 'function' && callback('invalid parameter');
            return;
        } else if (typeof options.mode === 'string') {
            options.mode = parseInt(options.mode, 16);
        }

        this.checkFileRights(id, name, options, utils.CONSTS.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file.write) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    return this._chmodFile(id, name, options, callback, meta);
                }
            }
        });
    }

    enableFileCache(enabled, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }

        if (options && options.acl) {
            options.acl = null;
        }

        utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_WRITE, (err, _options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else if (typeof callback === 'function') {
                // cache cannot be enabled
                setImmediate(() => callback(null, false));
            }
        });
    }

    // -------------- OBJECT FUNCTIONS -------------------------------------------
    _subscribe(pattern, options, callback) {
        if (Array.isArray(pattern)) {
            let count = pattern.length;
            pattern.forEach(pattern => {
                this.log.silly(this.namespace + ' redis psubscribe ' + this.objNamespace + pattern);
                this.sub.psubscribe(this.objNamespace + pattern, err =>
                    !--count && (typeof callback === 'function') && callback(err));
            });
        } else {
            this.log.silly(this.namespace + ' redis psubscribe ' + this.objNamespace + pattern);
            this.sub.psubscribe(this.objNamespace + pattern, err =>
                (typeof callback === 'function') && callback(err));
        }
    }
    subscribeConfig(pattern, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        utils.checkObjectRights(this, null, null, options, 'list', (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                return this._subscribe(pattern, options, callback);
            }
        });
    }
    subscribe(pattern, options, callback) {
        return this.subscribeConfig(pattern, options, callback);
    }

    _unsubscribe(pattern, options, callback) {
        if (Array.isArray(pattern)) {
            let count = pattern.length;
            pattern.forEach(pattern => {
                this.log.silly(this.namespace + ' redis punsubscribe ' + this.objNamespace + pattern);
                this.sub.punsubscribe(this.objNamespace + pattern, err =>
                    !--count && (typeof callback === 'function') && callback(err));
            });
        } else {
            this.log.silly(this.namespace + ' redis punsubscribe ' + this.objNamespace + pattern);
            this.sub.punsubscribe(this.objNamespace + pattern, err =>
                (typeof callback === 'function') && callback(err));
        }
    }
    unsubscribeConfig(pattern, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        utils.checkObjectRights(this, null, null, options, 'list', (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                return this._unsubscribe(pattern, options, callback);
            }
        });
    }
    unsubscribe(pattern, options, callback) {
        return this.unsubscribeConfig(pattern, options, callback);
    }

    _objectHelper(keys, objs, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id  = keys.shift();
            const obj = objs.shift();
            const message = JSON.stringify(obj);
            this.client.set(id, message, err => {
                !err && this.client.publish(id, message).catch(_err => {});
                setImmediate(() => this._objectHelper(keys, objs, callback));
            });
        }
    }
    _chownObject(pattern, options, callback) {
        this.getConfigKeys(pattern, options, (err, keys) => {
            if (err) {
                typeof callback === 'function' && callback(err);
                return;
            }
            this.client.mget(keys, (err, objects) => {
                const filteredKeys = [];
                const filteredObjs = [];
                objects = objects || [];
                for (let k = 0; k < keys.length; k++) {
                    try {
                        objects[k] = JSON.parse(objects[k]);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON ${keys[k]}: ${objects[k]}`);
                        continue;
                    }
                    if (!utils.checkObject(objects[k], options, utils.CONSTS.ACCESS_WRITE)) continue;
                    if (!objects[k].acl) {
                        objects[k].acl = {
                            owner:      (this.defaultNewAcl && this.defaultNewAcl.owner)      || utils.CONSTS.SYSTEM_ADMIN_USER,
                            ownerGroup: (this.defaultNewAcl && this.defaultNewAcl.ownerGroup) || utils.CONSTS.SYSTEM_ADMIN_GROUP,
                            object:     (this.defaultNewAcl && this.defaultNewAcl.object)     || (utils.CONSTS.ACCESS_USER_RW | utils.CONSTS.ACCESS_GROUP_READ | utils.CONSTS.ACCESS_EVERY_READ) // '0644'
                        };
                        if (objects[k].type === 'state') {
                            objects[k].acl.state = (this.defaultNewAcl && this.defaultNewAcl.state) || (utils.CONSTS.ACCESS_USER_RW | utils.CONSTS.ACCESS_GROUP_READ | utils.CONSTS.ACCESS_EVERY_READ); // '0644'
                        }
                    }
                    objects[k].acl.owner      = options.owner;
                    objects[k].acl.ownerGroup = options.ownerGroup;
                    filteredKeys.push(keys[k]);
                    filteredObjs.push(objects[k]);
                }
                this._objectHelper(filteredKeys, filteredObjs, () =>
                    typeof callback === 'function' && callback(null, filteredObjs));
            });
        }, true);
    }
    chownObject(pattern, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        options = options || {};
        options.acl = null;

        if (typeof options !== 'object') {
            options = {owner: options};
        }

        if (!options.ownerGroup && options.group) options.ownerGroup = options.group;
        if (!options.owner && options.user)  options.owner = options.user;

        if (!options.owner) {
            this.log.error(this.namespace + ' user is not defined');
            typeof callback === 'function' && callback('invalid parameter');
            return;
        }

        if (!options.ownerGroup) {
            // get user group
            this.getUserGroup(options.owner, (user, groups /* , permissions*/) => {
                if (!groups || !groups[0]) {
                    typeof callback === 'function' && callback('user "' + options.owner + '" belongs to no group');
                    return;
                } else {
                    options.ownerGroup = groups[0];
                }
                this.chownObject(pattern, options, callback);
            });
            return;
        }

        utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_WRITE, (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.object || !options.acl.object.write) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    return this._chownObject(pattern, options, callback);
                }
            }
        });
    }

    _chmodObject(pattern, options, callback) {
        this.getConfigKeys(pattern, options, (err, keys) => {
            if (err) {
                typeof callback === 'function' && callback(err);
                return;
            }
            this.client.mget(keys, (err, objects) => {
                const filteredKeys = [];
                const filteredObjs = [];
                objects = objects || [];
                for (let k = 0; k < keys.length; k++) {
                    try {
                        objects[k] = JSON.parse(objects[k]);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON ${keys[k]}: ${objects[k]}`);
                        continue;
                    }
                    if (!utils.checkObject(objects[k], options, utils.CONSTS.ACCESS_WRITE)) continue;
                    if (!objects[k].acl) {
                        objects[k].acl = {
                            owner:      (this.defaultNewAcl && this.defaultNewAcl.owner)      || utils.CONSTS.SYSTEM_ADMIN_USER,
                            ownerGroup: (this.defaultNewAcl && this.defaultNewAcl.ownerGroup) || utils.CONSTS.SYSTEM_ADMIN_GROUP,
                            object:     (this.defaultNewAcl && this.defaultNewAcl.object)     || (utils.CONSTS.ACCESS_USER_RW | utils.CONSTS.ACCESS_GROUP_READ | utils.CONSTS.ACCESS_EVERY_READ) // '0644'
                        };
                        if (objects[k].type === 'state') {
                            objects[k].acl.state = (this.defaultNewAcl && this.defaultNewAcl.state) || (utils.CONSTS.ACCESS_USER_RW | utils.CONSTS.ACCESS_GROUP_READ | utils.CONSTS.ACCESS_EVERY_READ); // '0644'
                        }
                    }
                    if (options.object !== undefined) objects[k].acl.object = options.object;
                    if (options.state !== undefined) objects[k].acl.state = options.state;
                    filteredKeys.push(keys[k]);
                    filteredObjs.push(objects[k]);
                }
                this._objectHelper(filteredKeys, filteredObjs, () =>
                    typeof callback === 'function' && callback(null, filteredObjs));
            });
        }, true);
    }
    chmodObject(pattern, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        options = options || {};
        options.acl = null;

        if (typeof options !== 'object') {
            options = {object: options};
        }

        if (options.mode && !options.object) options.object = options.mode;

        if (options.object === undefined) {
            this.log.error(this.namespace + ' mode is not defined');
            return typeof callback === 'function' && callback('invalid parameter');
        } else if (typeof options.mode === 'string') {
            options.mode = parseInt(options.mode, 16);
        }

        utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_WRITE, (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file.write) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    return this._chmodObject(pattern, options, callback);
                }
            }
        });
    }

    _getObject(id, options, callback) {
        if (!this.client) {
            return callback && callback(utils.ERRORS.ERROR_DB_CLOSED);
        }
        if (!id || typeof id !== 'string') {
            typeof callback === 'function' && callback('invalid id ' + JSON.stringify(id));
            return;
        }

        this.client.get(this.objNamespace + id, (err, obj) => {
            if (err) {
                this.log.debug(this.namespace + ' redis get ' + id + ', error - ' + err);
            } else {
                //if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' redis get ' + id + ' ok: ' + obj);
            }
            try {
                obj = obj ? JSON.parse(obj) : null;
            } catch (e) {
                this.log.warn(`${this.namespace} Cannot parse ${id} - ${obj}: ${e.message}`);
            }
            if (obj) {
                // Check permissions
                if (utils.checkObject(obj, options, utils.CONSTS.ACCESS_READ)) {
                    callback(null, obj);
                } else {
                    callback(utils.ERRORS.ERROR_PERMISSION);
                }

            } else {
                callback(err, obj);
            }
        });
    }
    getObject(id, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.getObject(id, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                });
            });
        }

        if (typeof callback === 'function') {
            if (options && options.acl) {
                options.acl = null;
            }
            utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_READ, (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._getObject(id, options, callback);
                }
            });
        }
    }

    getObjectAsync(id, options) {
        return new Promise((resolve, reject) => {
            this.getObject(id, options, (err, obj) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(obj);
                }
            });
        });
    }

    _getKeys(pattern, options, callback, dontModify) {
        if (!this.client) {
            return callback(utils.ERRORS.ERROR_DB_CLOSED);
        }
        if (!pattern || typeof pattern !== 'string') {
            typeof callback === 'function' && callback('invalid pattern ' + JSON.stringify(pattern));
            return;
        }

        const r = new RegExp(tools.pattern2RegEx(pattern));
        this.client.keys(this.objNamespace + pattern, (err, keys) => {
            if (!this.client) {
                return callback(utils.ERRORS.ERROR_DB_CLOSED);
            }

            const result = [];
            if (keys) {
                keys.sort();
                const result = [];
                const dontCheck =
                    options.user === utils.CONSTS.SYSTEM_ADMIN_USER ||
                    options.group !== utils.CONSTS.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) !== -1);

                if (dontCheck) {
                    for (let i = 0; i < keys.length; i++) {
                        const id = keys[i].substring(this.objNamespaceL);
                        if (r.test(id)) {
                            if (!dontModify) {
                                result.push(id);
                            } else {
                                result.push(keys[i]);
                            }
                        }
                    }
                    callback(err, result);
                } else {
                    // Check permissions
                    this.client.mget(keys, (err, metas) => {
                        metas = metas || [];
                        for (let i = 0; i < keys.length; i++) {
                            try {
                                metas[i] = JSON.parse(metas[i]);
                            } catch (e) {
                                this.log.error(`${this.namespace} Cannot parse JSON ${keys[i]}: ${metas[i]}`);
                                continue;
                            }

                            if (r.test(keys[i]) && utils.checkObject(metas[i], options, utils.CONSTS.ACCESS_READ)) {
                                if (!dontModify) {
                                    result.push(keys[i].substring(this.objNamespaceL));
                                } else {
                                    result.push(keys[i]);
                                }
                            }
                        }
                        callback(err, result);
                    });

                }
            } else {
                callback(err, result);
            }
        });
    }
    getKeys(pattern, options, callback, dontModify) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.getKeys(pattern, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                }, dontModify);
            });
        }
        if (typeof callback === 'function') {
            utils.checkObjectRights(this, null, null, options, 'list', (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._getKeys(pattern, options, callback, dontModify);
                }
            });
        }
    }
    getConfigKeys(pattern, options, callback, dontModify) {
        return this.getKeys(pattern, options, callback, dontModify);
    }

    _getObjects(keys, options, callback, dontModify) {
        if (!keys) {
            typeof callback === 'function' && callback('no keys', null);
            return;
        }
        if (!keys.length) {
            typeof callback === 'function' && callback(null, []);
            return;
        }

        let _keys;
        if (!dontModify) {
            _keys = [];
            for (let i = 0; i < keys.length; i++) {
                _keys[i] = this.objNamespace + keys[i];
            }
        } else {
            _keys = keys;
        }

        if (!this.client) {
            return callback(utils.ERRORS.ERROR_DB_CLOSED);
        }

        this.client.mget(_keys, (err, objs) => {
            let result = [];
            if (err) {
                this.log.warn(this.namespace + ' redis mget ' + (!objs ? 0 :  objs.length) + ' ' + _keys.length + ', err: ' + err);
            } else {
                if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' redis mget ' + (!objs ? 0 : objs.length) + ' ' + _keys.length);
            }
            if (objs) {
                const dontCheck =
                    options.user === utils.CONSTS.SYSTEM_ADMIN_USER ||
                    options.group !== utils.CONSTS.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.CONSTS.SYSTEM_ADMIN_GROUP) !== -1);

                if (!dontCheck) {
                    for (let i = 0; i < objs.length; i++) {
                        try {
                            objs[i] = JSON.parse(objs[i]);
                        } catch (e) {
                            this.log.error(`${this.namespace} Cannot parse JSON ${_keys[i]}: ${objs[i]}`);
                            result.push({error: utils.ERRORS.ERROR_PERMISSION});
                            continue;
                        }
                        if (utils.checkObject(objs[i], options, utils.CONSTS.ACCESS_READ)) {
                            result.push(objs[i]);
                        } else {
                            result.push({error: utils.ERRORS.ERROR_PERMISSION});
                        }
                    }
                } else {
                    result = objs.map((obj, i) => {
                        try {
                            return JSON.parse(obj);
                        } catch (e) {
                            this.log.error(`${this.namespace} Cannot parse JSON ${_keys[i]}: ${obj}`);
                            return null;
                        }
                    });
                }
            }
            callback(null, result);
        });
    }
    getObjects(keys, options, callback, dontModify) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.getObjects(keys, options, (err, objs) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(objs);
                    }
                }, dontModify);
            });
        }

        if (options && options.acl) options.acl = null;
        if (typeof callback === 'function') {
            utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_READ, (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._getObjects(keys, options, callback, dontModify);
                }
            });
        }
    }

    _getObjectsByPattern(pattern, options, callback) {
        if (!pattern || typeof pattern !== 'string') {
            typeof callback === 'function' && callback('invalid pattern ' + JSON.stringify(pattern));
            return;
        }

        this.client.keys(this.objNamespace + pattern, (err, keys) => {
            if (!this.client) {
                return callback(utils.ERRORS.ERROR_DB_CLOSED);
            }

            if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' redis keys ' + keys.length + ' ' + pattern);
            this._getObjects(keys, options, callback, true);
        });
    }
    getObjectsByPattern(pattern, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.getObjectsByPattern(pattern, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                });
            });
        }
        if (options && options.acl) options.acl = null;
        if (typeof callback === 'function') {
            utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_READ, (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._getObjectsByPattern(pattern, options, callback);
                }
            });
        }
    }

    _setObject(id, obj, options, callback) {
        if (!id || typeof id !== 'string' || utils.regCheckId.test(id)) {
            if (typeof callback === 'function') {
                callback(`Invalid ID: ${id}`);
            }
            return;
        }

        if (!obj) {
            this.log.warn(this.namespace + ' setObject: Argument object is null');
            typeof callback === 'function' && callback('obj is null');
            return;
        }
        if (typeof obj !== 'object') {
            this.log.warn(this.namespace + ' setObject: Argument object is no object: ' + obj);
            typeof callback === 'function' && callback('obj is no object');
            return;
        }

        obj._id = id;
        this.client.get(this.objNamespace + id, (err, oldObj) => {
            try {
                oldObj = oldObj ? JSON.parse(oldObj) : null;
            } catch (e) {
                this.log.error(`${this.namespace} Cannot parse ${id} - ${oldObj}: ${e.message}`);
                typeof callback === 'function' && callback(`${this.namespace} Cannot parse ${id} - ${oldObj}: ${e.message}`);
                return;
            }

            if (!tools.checkNonEditable(oldObj, obj)) {
                typeof callback === 'function' && callback('Invalid password for update of vendor information');
                return;
            }

            // do not delete common settings, like "history" or "mobile". It can be erased only with "null"
            if (oldObj && oldObj.common) {
                for (let i = 0; i < this.preserveSettings.length; i++) {
                    // remove settings if desired
                    if (obj.common && obj.common[this.preserveSettings[i]] === null) {
                        delete obj.common[this.preserveSettings[i]];
                        continue;
                    }

                    if (oldObj.common[this.preserveSettings[i]] !== undefined && (!obj.common || obj.common[this.preserveSettings[i]] === undefined)) {
                        if (!obj.common) obj.common = {};
                        obj.common[this.preserveSettings[i]] = oldObj.common[this.preserveSettings[i]];
                    }
                }
            }

            if (obj.common && obj.common.alias && obj.common.alias.id && obj.common.alias.id.startsWith('alias.')) {
                return typeof callback === 'function' && callback('Cannot make alias on alias');
            }

            if (oldObj && oldObj.acl && !obj.acl) {
                obj.acl = oldObj.acl;
            }

            // add user default rights
            if (this.defaultNewAcl && !obj.acl) {
                obj.acl = JSON.parse(JSON.stringify(this.defaultNewAcl));
                delete obj.acl.file;
                if (obj.type !== 'state') {
                    delete obj.acl.state;
                }
                if (options.owner) {
                    obj.acl.owner = options.owner;

                    if (!options.ownerGroup) {
                        obj.acl.ownerGroup = null;
                        return this.getUserGroup(options.owner, (user, groups /* , permissions */) => {
                            if (!groups || !groups[0]) {
                                options.ownerGroup = (this.defaultNewAcl && this.defaultNewAcl.ownerGroup) || utils.CONSTS.SYSTEM_ADMIN_GROUP;
                            } else {
                                options.ownerGroup = groups[0];
                            }
                            obj.acl.ownerGroup = options.ownerGroup;

                            const message = JSON.stringify(obj);
                            this.client.set(this.objNamespace + id, message, err => {
                                if (!err) {
                                    //this.settings.connection.enhancedLogging && this.log.silly(this.namespace + ' redis publish ' + this.objNamespace + id + ' ' + message);
                                    this.client.publish(this.objNamespace + id, message).catch(_err => {});
                                }
                                typeof callback === 'function' && callback(err, {id: obj._id});
                            });
                        });
                    }
                }
            }
            if (this.defaultNewAcl && obj.acl && !obj.acl.ownerGroup && options.ownerGroup) {
                obj.acl.ownerGroup = options.ownerGroup;
            }
            const message = JSON.stringify(obj);
            this.client.set(this.objNamespace + id, message, err => {
                if (!err) {
                    //this.settings.connection.enhancedLogging && this.log.silly(this.namespace + ' redis publish ' + this.objNamespace + id + ' ' + message);
                    this.client.publish(this.objNamespace + id, message).catch(_err => {});
                }
                typeof callback === 'function' && callback(err, {id: obj._id});
            });
        });
    }
    /**
     * set anew or update object
     *
     * This function writes the object into DB
     *
     * @alias setObject
     * @memberof objectsInMemServer
     * @param {string} id ID of the object
     * @param {object} obj
     * @param {object} options options for access control are optional
     * @param {function} callback return function
     */
    setObject(id, obj, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.setObject(id, obj, options, (err, res) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(res);
                    }
                });
            });
        }
        if (options && options.acl) {
            options.acl = null;
        }

        utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_WRITE, (err, options) => {
            if (err) {
                if (typeof callback === 'function') {
                    callback(err);
                }
            } else {
                return this._setObject(id, obj, options, callback);
            }
        });
    }

    setObjectAsync(id, obj, options) {
        return new Promise((resolve, reject) => {
            this.setObject(id, obj, options, (err, res) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(res);
                }
            });
        });
    }

    _delObject(id, options, callback) {
        if (!id || typeof id !== 'string' || utils.regCheckId.test(id)) {
            return typeof callback === 'function' && callback(`Invalid ID: ${id}`);
        }

        // read object
        this.client.get(this.objNamespace + id, (err, oldObj) => {
            if (err) {
                this.log.warn(this.namespace + ' redis get ' + id + ', error - ' + err);
            } else {
                //if (this.settings.connection.enhancedLogging) this.log.silly(this.namespace + ' redis get ' + id + ' ok: ' + oldObj);
            }
            if (!oldObj) {
                return typeof callback === 'function' && callback(utils.ERRORS.ERROR_NOT_FOUND);
            }

            try {
                oldObj = oldObj ? JSON.parse(oldObj) : null;
            } catch (e) {
                this.log.warn(`${this.namespace} Cannot parse ${id} - ${oldObj}: ${e.message}`);
            }

            if (!utils.checkObject(oldObj, options, utils.CONSTS.ACCESS_WRITE)) {
                typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
            } else {
                this.client.del(this.objNamespace + id, (err) => {
                    if (!err) {
                        //this.settings.connection.enhancedLogging && this.log.silly(this.namespace + ' redis publish ' + this.objNamespace + id + ' null');
                        this.client.publish(this.objNamespace + id, 'null').catch(_err => {});
                    }
                    typeof callback === 'function' && callback(err);
                });
            }
        });
    }
    delObject(id, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.delObject(id, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                });
            });
        }

        if (options && options.acl) {
            options.acl = null;
        }
        utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_DELETE, (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                return this._delObject(id, options, callback);
            }
        });
    }

    delObjectAsync(id, options) {
        return new Promise((resolve, reject) => {
            this.delObject(id, options, err => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    // this function is very ineffective. Because reads all objects and then process them
    _applyViewFunc(func, params, options, callback) {
        if (!this.client) {
            return callback && callback(utils.ERRORS.ERROR_DB_CLOSED);
        }
        const result = {
            rows: []
        };

        params = params || {};
        params.startkey = params.startkey || '';
        params.endkey   = params.endkey   || '\u9999';
        const wildcardPos = params.endkey.indexOf('\u9999');
        let wildCardLastPos = true;
        if (wildcardPos !== -1 && wildcardPos !== params.endkey.length - 1) {
            wildCardLastPos = false; // TODO do in LUA
        }
        let m;

        // if start and and end keys are equal modify end key
        if (params.startkey === params.endkey) {
            params.endkey = params.endkey + '\u0000';
        }

        // filter by type
        if (wildCardLastPos && func && func.map && this.scripts.filter && (m = func.map.match(/if\s\(doc\.type\s?===?\s?'(\w+)'\)\semit\(([^,]+),\s?doc\s?\)/))) {
            this.client.evalsha([this.scripts.filter, 4, this.objNamespace, params.startkey, params.endkey, m[1]], (err, objs) => {
                err && this.log.warn(`${this.namespace} Cannot get view: ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    try {
                        obj = JSON.parse(obj);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON: ${obj}`);
                        return {id: 'parseError', value: null};
                    }
                    if (m[2] && m[2].trim() === 'doc._id') {
                        return {id: obj._id, value: obj};
                    } else if (m[2] && m[2].trim() === 'doc.common.name' && obj.common) {
                        return {id: obj.common.name, value: obj};
                    }
                    else {
                        this.log.error(`${this.namespace} Cannot filter "${m[2]}": ${JSON.stringify(obj)}`);
                        return {id: 'parseError', value: null};
                    }
                });
                // Calculate max
                if (func.reduce === '_stats') {
                    let max = null;
                    for (let i = 0; i < result.rows.length; i++) {
                        if (max === null || result.rows[i].value > max) {
                            max = result.rows[i].value;
                        }
                    }
                    if (max !== null) {
                        result.rows = [{id: '_stats', value: {max: max}}];
                    } else {
                        result.rows = [];
                    }
                }

                callback(null, result);
            });
        } else
        // filter by script
        if (wildCardLastPos && func && func.map && this.scripts.script && func.map.indexOf('doc.common.engineType') !== -1) {
            this.client.evalsha([this.scripts.script, 3, this.objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && this.log.warn(`${this.namespace} Cannot get view: ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    try {
                        obj = JSON.parse(obj);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON: ${obj}`);
                        return {id: 'parseError', value: null};
                    }
                    return {id: obj._id, value: obj};
                });
                callback(null, result);
            });
        } else
        // filter by hm-rega programs
        if (wildCardLastPos && func && func.map && this.scripts.programs && func.map.indexOf('doc.native.TypeName === \'PROGRAM\'') !== -1) {
            this.client.evalsha([this.scripts.programs, 3, this.objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && this.log.warn(`${this.namespace} Cannot get view: ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    try {
                        obj = JSON.parse(obj);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON: ${obj}`);
                        return {id: 'parseError', value: null};
                    }
                    return {id: obj._id, value: obj};
                });
                callback(null, result);
            });
        } else
        // filter by hm-rega variables
        if (wildCardLastPos && func && func.map && this.scripts.variables && func.map.indexOf('doc.native.TypeName === \'ALARMDP\'') !== -1) {
            this.client.evalsha([this.scripts.variables, 3, this.objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && this.log.warn(`${this.namespace} Cannot get view ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    try {
                        obj = JSON.parse(obj);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON: ${obj}`);
                        return {id: 'parseError', value: null};
                    }
                    return {id: obj._id, value: obj};
                });
                callback(null, result);
            });
        } else
        // filter by custom, redis also returns if common.custom is not present
        if (wildCardLastPos && func && func.map && this.scripts.custom && func.map.indexOf('doc.common.custom') !== -1) {
            this.client.evalsha([this.scripts.custom, 3, this.objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && this.log.warn(`${this.namespace} Cannot get view: ${err}`);
                objs = objs || [];
                result.rows = [];
                objs.forEach(obj => {
                    try {
                        obj = JSON.parse(obj);
                    } catch (e) {
                        this.log.error(`${this.namespace} Cannot parse JSON: ${obj}`);
                    }
                    if (obj.common && obj.common.custom) {
                        result.rows.push({id: obj._id, value: obj.common.custom});
                    }
                });
                callback(null, result);
            });
        } else {
            console.log(this.namespace + ' UNOPTIMIZED!: ' + func.map);

            let searchKeys = this.objNamespace + '*';
            if (wildcardPos !== -1) { // Wildcard included
                searchKeys = this.objNamespace + params.endkey.replace(/\u9999/g, '*');
            }
            this.client.keys(searchKeys, (err, keys) => {
                if (!this.client) {
                    return callback(utils.ERRORS.ERROR_DB_CLOSED);
                }
                const endAfterWildcard = params.endkey.substr(wildcardPos + 1);
                params.startkey = this.objNamespace + params.startkey;
                params.endkey   = this.objNamespace + params.endkey;

                keys = keys.sort().filter(key => {
                    if (key && !utils.regCheckId.test(key)) {
                        if (params && wildcardPos > 0) {
                            if (params.startkey && key < params.startkey) return false;
                            if (params.endkey   && key > params.endkey)   return false;
                        } else
                        if (params && wildcardPos === 0) {
                            if (!key.endsWith(endAfterWildcard)) return false;
                        }
                        return true;
                    } else {
                        return false;
                    }
                });
                this.client.mget(keys, (err, objs) => {
                    // eslint-disable-next-line no-unused-vars
                    function _emit_(id, obj) {
                        result.rows.push({id: id, value: obj});
                    }

                    const f = eval('(' + func.map.replace(/^function\(([a-z0-9A-Z_]+)\)/g, 'function($1, emit)') + ')');

                    objs = objs || [];
                    for (let i = 0; i < keys.length; i++) {
                        try {
                            objs[i] = JSON.parse(objs[i]);
                        } catch (e) {
                            this.log.error(this.namespace + ' Cannot parse JSON ' + keys[i] + ':  ' + objs[i]);
                            continue;
                        }
                        if (!utils.checkObject(objs[i], options, utils.CONSTS.ACCESS_READ)) continue;

                        if (objs[i]) {
                            try {
                                f(objs[i], _emit_);
                            } catch (e) {
                                this.log.error(this.namespace + ' Cannot execute map: ' + e.message);
                            }
                        }
                    }
                    // Calculate max
                    if (func.reduce === '_stats') {
                        let max = null;
                        for (let i = 0; i < result.rows.length; i++) {
                            if (max === null || result.rows[i].value > max) {
                                max = result.rows[i].value;
                            }
                        }
                        if (max !== null) {
                            result.rows = [{id: '_stats', value: {max: max}}];
                        } else {
                            result.rows = [];
                        }
                    }
                    callback(null, result);
                });
            });
        }
    }
    _applyView(func, params, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this._applyView(func, params, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                });
            });
        }

        if (options && options.acl) options.acl = null;

        if (typeof callback === 'function') {
            utils.checkObjectRights(this, null, null, options, 'list', (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._applyViewFunc(func, params, options, callback);
                }
            });
        }
    }

    _getObjectView(design, search, params, options, callback) {
        this.client.get(this.objNamespace + '_design/' + design, (err, obj) => {
            if (obj) {
                try {
                    obj = JSON.parse(obj);
                } catch (e) {
                    this.log.error(`${this.namespace} Cannot parse JSON: ${obj}`);
                    return callback(new Error('Cannot parse JSON: "' + '_design/' + design + '" / "' + obj + '"'));
                }
                if (obj.views && obj.views[search]) {
                    this._applyViewFunc(obj.views[search], params, options, callback);
                } else {
                    this.log.error(`${this.namespace} Cannot find search "${search}" in "${design}"`);
                    callback(new Error('Cannot find search "' + search + '" in "' + design + '"'));
                }
            } else {
                this.log.error(`${this.namespace} Cannot find view "${design}" for search "${search}" : ${err}`);
                callback(new Error('Cannot find view "' + design + '"'));
            }
        });
    }
    getObjectView(design, search, params, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.getObjectView(design, search, params, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                });
            });
        }

        if (options && options.acl) options.acl = null;

        if (typeof callback === 'function') {
            utils.checkObjectRights(this, null, null, options, 'list', (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._getObjectView(design, search, params, options, callback);
                }
            });
        }
    }

    _getObjectList(params, options, callback) {
        //params = {startkey, endkey, include_docs}
        params = params || {};
        params.startkey = params.startkey || '';
        params.endkey = params.endkey || '\u9999';
        const pattern = (params.endkey.substring(0, params.startkey.length) === params.startkey) ? this.objNamespace + params.startkey + '*' : this.objNamespace + '*';

        // todo: use lua script for this
        this.client.keys(pattern, (err, keys) => {
            if (!this.client) {
                return callback(utils.ERRORS.ERROR_DB_CLOSED);
            }
            const _keys = [];
            for (let i = 0; i < keys.length; i++) {
                const id = keys[i].substring(this.objNamespaceL);
                if (params.startkey && id < params.startkey) continue;
                if (params.endkey && id > params.endkey) continue;
                if (!id || utils.regCheckId.test(id) || id.match(/\|file\$%\$/)) continue;
                if (!params.include_docs && id[0] === '_') continue;
                _keys.push(keys[i]);
            }
            _keys.sort();
            this.client.mget(_keys, (err, objs) => {
                // return rows with id and doc
                const result = {
                    rows: []
                };
                if (objs) {
                    for (let r = 0; r < objs.length; r++) {
                        try {
                            objs[r] = JSON.parse(objs[r]);
                        } catch (e) {
                            this.log.error(`${this.namespace} Cannot parse JSON ${_keys[r]}: ${objs[r]}`);
                            continue;
                        }

                        if (!utils.checkObject(objs[r], options, utils.CONSTS.ACCESS_READ)) continue;
                        result.rows.push({id: objs[r]._id, value: objs[r], doc: objs[r]});
                    }
                }
                callback(null, result);
            });
        });
    }
    getObjectList(params, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.getObjectList(params, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                });
            });
        }

        if (options && options.acl) options.acl = null;

        if (typeof callback === 'function') {
            utils.checkObjectRights(this, null, null, options, 'list', (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._getObjectList(params, options, callback);
                }
            });
        }
    }

    getObjectListAsync(params, options) {
        return new Promise((resolve, reject) => {
            this.getObjectList(params, options, (err, arr) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(arr);
                }
            });
        });
    }

    // could be optimised, to read object only once. Now it will read 3 times
    _extendObject(id, obj, options, callback, _iteration) {
        if (!id || typeof id !== 'string' || utils.regCheckId.test(id)) {
            typeof callback === 'function' && callback(`Invalid ID: ${id}`);
        } else {
            this.client.get(this.objNamespace + id, (err, oldObj) => {
                try {
                    oldObj = oldObj ? JSON.parse(oldObj) : null;
                } catch (e) {
                    this.log.error(`${this.namespace} Cannot parse JSON ${id}: ${oldObj}`);
                    oldObj = null;
                    return typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                }
                if (!utils.checkObject(oldObj, options, utils.CONSTS.ACCESS_WRITE)) {
                    return typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                }

                let _oldObj;
                if (oldObj && oldObj.nonEdit) {
                    // copy object
                    _oldObj = JSON.parse(JSON.stringify(oldObj));
                }

                oldObj = oldObj || {};
                oldObj = extend(true, oldObj, obj);
                oldObj._id = id;

                // add user default rights
                if (this.defaultNewAcl && !oldObj.acl) {
                    oldObj.acl = JSON.parse(JSON.stringify(this.defaultNewAcl));
                    delete oldObj.acl.file;
                    if (oldObj.type !== 'state') {
                        delete oldObj.acl.state;
                    }

                    if (options.owner) {
                        oldObj.acl.owner = options.owner;

                        if (!options.ownerGroup) {
                            oldObj.acl.ownerGroup = null;
                            return this.getUserGroup(options.owner, (user, groups /*, permissions */) => {
                                if (!groups || !groups[0]) {
                                    options.ownerGroup = (this.defaultNewAcl && this.defaultNewAcl.ownerGroup) || utils.CONSTS.SYSTEM_ADMIN_GROUP;
                                } else {
                                    options.ownerGroup = groups[0];
                                }
                                this._extendObject(id, obj, options, callback);
                            });
                        }
                    }
                }

                if (this.defaultNewAcl && options.ownerGroup && oldObj.acl && !oldObj.acl.ownerGroup) {
                    oldObj.acl.ownerGroup = options.ownerGroup;
                }

                if (obj.common && obj.common.alias && obj.common.alias.id && obj.common.alias.id.startsWith('aliases.')) {
                    return typeof callback === 'function' && callback('Cannot make alias on alias');
                }

                if (_oldObj && !tools.checkNonEditable(_oldObj, oldObj)) {
                    return typeof callback === 'function' && callback('Invalid password for update of vendor information');
                }
                const message = JSON.stringify(oldObj);
                this.client.set(this.objNamespace + id, message, err => {
                    if (!err) {
                        //this.settings.connection.enhancedLogging && this.log.silly(this.namespace + ' redis publish ' + this.objNamespace + id + ' ' + message);
                        this.client.publish(this.objNamespace + id, message).catch(_err => {});
                    }
                    typeof callback === 'function' && callback(err, {id: id, value: oldObj}, id);
                });
            });
        }
    }
    extendObject(id, obj, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.extendObject(id, obj, options, (err, obj) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(obj);
                    }
                });
            });
        }

        if (options && options.acl) options.acl = null;

        utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_WRITE, (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                return this._extendObject(id, obj, options, callback);
            }
        });
    }

    extendObjectAsync(id, obj, options) {
        return new Promise((resolve, reject) => {
            this.extendObject(id, obj, options, err => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    setConfig(id, obj, options, callback) {
        return this.setObject(id, obj, options, callback);
    }

    delConfig(id, options, callback) {
        return this.delObject(id, options, callback);
    }

    getConfig(id, options, callback) {
        return this.getObject(id, options, callback);
    }

    getConfigs(keys, options, callback, dontModify) {
        return this.getObjects(keys, options, callback, dontModify);
    }

    _findObject(idOrName, type, options, callback) {
        this._getObject(idOrName, options, (err, obj) => {
            // Assume it is ID
            if (obj && utils.checkObject(obj, options, utils.CONSTS.ACCESS_READ) && (!type || (obj.common && obj.common.type === type))) {
                callback(null, idOrName, obj.common.name);
            } else {
                this._getKeys('*', options, (err, keys) => {
                    this.client.mget(keys, (err, objs) => {
                        objs = objs || [];
                        // Assume it is name
                        for (let i = 0; i < keys.length; i++) {
                            try {
                                objs[i] = JSON.parse(objs[i]);
                            } catch (e) {
                                this.log.error(`${this.namespace} Cannot parse JSON ${keys[i]}: ${objs[i]}`);
                                continue;
                            }
                            if (objs[i].common &&
                                objs[i].common.name === idOrName &&
                                (!type || (objs[i].common && objs[i].common.type === type))) {
                                typeof callback === 'function' && callback(null, objs[i]._id, idOrName);
                                return;
                            }
                        }
                        typeof callback === 'function' && callback(null, null, idOrName);
                    });
                }, true);
            }
        });
    }
    findObject(idOrName, type, options, callback) {
        if (typeof type === 'function') {
            callback = type;
            options = null;
            type = null;
        }
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (!callback) {
            return new Promise((resolve, reject) => {
                this.findObject(idOrName, type, options, (err, id, _idOrName) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(id);
                    }
                });
            });
        }

        if (options && options.acl) options.acl = null;

        if (typeof callback === 'function') {
            utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_LIST, (err, options) => {
                if (err) {
                    typeof callback === 'function' && callback(err);
                } else {
                    return this._findObject(idOrName, type, options, callback);
                }
            });
        }
    }

    // can be called only from js-controller
    addPreserveSettings(settings) {
        if (!Array.isArray(settings)) settings = [settings];

        for (let s = 0; s < settings.length; s++) {
            if (this.preserveSettings.indexOf(settings[s]) === -1) this.preserveSettings.push(settings[s]);
        }
    }

    _destroyDBHelper(keys, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id = keys.shift();
            this.client.del(id, _err =>
                setImmediate(() => this._destroyDBHelper(keys, callback)));
        }
    }
    _destroyDB(options, callback) {
        this.client.keys(this.redisNamespace + '*', (err, keys) => {
            this._destroyDBHelper(keys, callback);
        });
    }
    destroyDB(options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        options = options || {};

        utils.checkObjectRights(this, null, null, options, utils.CONSTS.ACCESS_WRITE, (err, options) => {
            if (err) {
                typeof callback === 'function' && callback(err);
            } else {
                if (!options.acl.file.write || options.user !== utils.CONSTS.SYSTEM_ADMIN_USER) {
                    typeof callback === 'function' && callback(utils.ERRORS.ERROR_PERMISSION);
                } else {
                    return this._destroyDB(options, callback);
                }
            }
        });
    }

    // Destructor of the class. Called by shutting down.
    destroy () {
        this.stop = true;
        if (this.client) {
            try {
                this.client.quit(() => {
                    this.client = null;
                });
            } catch (e) {
                // ignore error
            }

        }
        if (this.sub) {
            try {
                this.sub.quit(() => {
                    this.sub = null;
                });
            } catch (e) {
                // ignore error
            }
        }
    }

    loadLuaScripts(callback, _scripts) {
        if (!_scripts) {
            if (scriptFiles && scriptFiles.filter) {
                _scripts = [];
                for (const name in scriptFiles) {
                    if (!scriptFiles.hasOwnProperty(name)) {
                        continue;
                    }
                    const shasum = crypto.createHash('sha1');
                    const buf = Buffer.from(scriptFiles[name]);
                    shasum.update(buf);
                    _scripts.push({
                        name,
                        text: buf,
                        hash: shasum.digest('hex')
                    });
                }
            } else {
                _scripts = fs.readdirSync(__dirname + '/lua/').map(name => {
                    const shasum = crypto.createHash('sha1');
                    const script = fs.readFileSync(__dirname + '/lua/' + name);
                    shasum.update(script);
                    const hash = shasum.digest('hex');
                    return {name: name.replace(/\.lua$/, ''), text: script, hash};
                });
            }
            const hashes = _scripts.map(e => e.hash);
            hashes.unshift('EXISTS');
            return this.client.script(hashes, (err, arr) => {
                _scripts.forEach((e, i) => _scripts[i].loaded = !!arr[i]);
                this.loadLuaScripts(callback, _scripts);
            });
        }
        for (let i = 0; i < _scripts.length; i++) {
            if (!_scripts[i].loaded) {
                const script = _scripts[i];
                return this.client.script(['LOAD', script.text], (err, hash) => {
                    script.hash = hash;
                    script.loaded = !err;
                    err && this.log.error(this.namespace + ' Cannot load "' + script.name + '": ' + err);
                    setImmediate(() => this.loadLuaScripts(callback, _scripts));
                });
            }
        }
        this.scripts = {};
        _scripts.forEach(e => this.scripts[e.name] = e.hash);
        callback();
    }
}

module.exports = ObjectsInRedis;
