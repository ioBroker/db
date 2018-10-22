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
const redis       = require('redis');
const tools       = require('../tools');
const fs          = require('fs');
let crypto        = require('crypto'); // must be let
let utils         = require('./objectsUtils');

/* @@tools.js@@ */
/* @@objectsUtils.js@@ */
const scriptFiles = {};
/* @@lua@@ */

function ObjectsInRedis(settings) {
    settings = settings || {};
    const redisNamespace  = (settings.redisNamespace || (settings.connection && settings.connection.redisNamespace) || 'cfg') + '.';
    const fileNamespace   = redisNamespace + 'f.';
    const fileNamespaceL  = fileNamespace.length;
    const objNamespace    = redisNamespace + 'o.';
    const objNamespaceL   = objNamespace.length;
    const ioRegExp        = new RegExp('^' + objNamespace.replace(/\./g, '\\.'));

    const onChange        = settings.change; // on change handler

    let client;
    let sub;
    let that                = this;
    let preserveSettings    = ['custom'];
    let defaultNewAcl       = settings.defaultNewAcl || null;
    let namespace           = settings.namespace || settings.hostname || '';
    let scripts             = {};
    let clientBin;

    let log = utils.getLogger(settings.logger);

    settings.connection = settings.connection || {};

    // limit max number of log entries in the list
    settings.connection.maxQueue = settings.connection.maxQueue || 1000;

    if (settings.connection.options) {
        if (settings.connection.options.retry_max_delay) {
            const retry_max_delay = settings.connection.options.retry_max_delay;
            // convert redis 0.1 options to redis 3.0
            settings.connection.options.retry_strategy = function (options) {
                // A function that receives an options object as parameter including the retry attempt,
                // the total_retry_time indicating how much time passed since the last time connected,
                // the error why the connection was lost and the number of times_connected in total.
                // If you return a number from this function, the retry will happen exactly after that
                // time in milliseconds. If you return a non-number, no further retry will happen and
                // all offline commands are flushed with errors. Return an error to return that
                // specific error to all offline commands.

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
            delete settings.connection.options.retry_max_delay;
        }
    }

    this.getStatus = function () {
        return {type: 'redis', server: false};
    };

    // -------------- FILE FUNCTIONS -------------------------------------------
    function _createBinaryClient() {
        if (!clientBin) {
            settings.connection.options = settings.connection.options || {};
            let opt = JSON.parse(JSON.stringify(settings.connection.options));
            opt.return_buffers = true;
            if (settings.connection.port === 0) {
                // initiate a unix socket connection using the parameter 'host'
                clientBin = redis.createClient(settings.connection.host, opt);
            } else {
                clientBin = redis.createClient(settings.connection.port, settings.connection.host, opt);
            }
        }
    }

    function _setBinaryState(id, data, callback) {
        if (!clientBin) _createBinaryClient ();
        clientBin.set(id, data, callback);
    }
    function _getBinaryState(id, callback) {
        if (!clientBin) _createBinaryClient ();
        clientBin.get(id, (err, data) => {
            if (!err && data) {
                if (callback) callback(err, new Buffer(data, 'binary'));
            } else {
                if (callback) callback(err);

            }
        });
    }
    function _delBinaryState(id, callback) {
        if (!clientBin) _createBinaryClient ();
        clientBin.del(id, () => (typeof callback === 'function') && callback());
    }

    function getFileId(id, name, isMeta) {
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

        return fileNamespace + id + '$%$' + name + (isMeta !== undefined ? (isMeta ? '$%$meta' : '$%$data') : '');
    }

    this.checkFile = function (id, name, options, flag, callback) {
        // read file settings from redis
        client.get(getFileId(id, name, true), (err, fileOptions) => {
            fileOptions = fileOptions || '{"notExists": true}';
            fileOptions = JSON.parse(fileOptions);
            if (utils.checkFile(fileOptions, options, flag, defaultNewAcl)) {
                return callback && callback(false, options, fileOptions); // NO error
            } else {
                return callback && callback(true, options); // error
            }
        });
    };

    function checkFileRights(id, name, options, flag, callback) {
        return utils.checkFileRights(that, id, name, options, flag, callback);
    }

    function _setDefaultAcl(ids, defaultAcl) {
        if (ids && ids.length) {
            const id = ids.shift();
            that.getObject(id, (err, obj) => {
                if (obj && !obj.acl) {
                    obj.acl = defaultAcl;
                    that.setObject(id, obj, () => setImmediate(_setDefaultAcl, ids, defaultAcl));
                } else {
                    setImmediate(_setDefaultAcl, ids, defaultAcl);
                }
            });
        }
    }

    function setDefaultAcl(defaultNewAcl) {
        defaultNewAcl = defaultNewAcl || {
            owner: utils.consts.SYSTEM_ADMIN_USER,
            ownerGroup: utils.consts.SYSTEM_ADMIN_GROUP,
            object: 0x664,
            state: 0x664,
            file: 0x664
        };
        // Get ALL Objects
        that.getKeys('*', (err, ids) => _setDefaultAcl(ids, defaultNewAcl));
    }

    this.getUserGroup = function (user, callback) {
        return utils.getUserGroup(this, user, (error, user, userGroups, userAcl) => {
            if (error) log.error(namespace + ' ' + error);
            callback.call(this, user, userGroups, userAcl);
        });
    };

    this.insert = function (id, attName, ignore, options, obj, callback) {
        return utils.insert(that, id, attName, ignore, options, obj, callback);
    };

    function _writeFile(id, name, data, options, callback, meta) {
        let isBinary;
        let ext         = name.match(/\.[^.]+$/);
        let mime        = utils.getMimeType(ext);
        let _mimeType   = mime.mimeType;
        isBinary        = mime.isBinary;

        const metaID = getFileId(id, name, true);
        if (!meta) {
            meta = {createdAt: Date.now()};
        }
        if (!meta.acl) {
            meta.acl = {
                owner:       options.user  || (defaultNewAcl && defaultNewAcl.owner)      || utils.consts.SYSTEM_ADMIN_USER,
                ownerGroup:  options.group || (defaultNewAcl && defaultNewAcl.ownerGroup) || utils.consts.SYSTEM_ADMIN_GROUP,
                permissions: options.mode  || (defaultNewAcl && defaultNewAcl.file)       || 0x644
            };
        }
        meta.stats = {
            size: data.length
        };
        if (meta.hasOwnProperty('notExists')) delete meta.notExists;

        meta.mimeType       = options.mimeType || _mimeType;
        meta.binary         = isBinary;
        meta.acl.ownerGroup = meta.acl.ownerGroup || (defaultNewAcl && defaultNewAcl.ownerGroup) || utils.consts.SYSTEM_ADMIN_GROUP;
        meta.modifiedAt     = Date.now();

        client.set(metaID, JSON.stringify(meta), err => _setBinaryState(getFileId(id, name, false), data, err => callback && callback(err)));
    }
    this.writeFile = function (id, name, data, options, callback) {
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

        if (name[0] === '/') name = name.substring(1);

        if (!callback) {
            return new Promise((resolve, reject) => {
                this.writeFile(id, name, data, options, err => {
                    if (!err) {
                        resolve();
                    } else {
                        reject(err);
                    }
                });
            });
        }

        // If file yet exists => check the permissions
        return checkFileRights(id, name, options, utils.consts.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                return _writeFile(id, name, data, options, callback, meta);
            }
        });
    };

    function _readFile(id, name, options, callback, meta) {
        _getBinaryState(getFileId(id, name, false), (err, buffer) => {
            let mimeType = meta && meta.mimeType;
            if (meta && !meta.binary && buffer) {
                buffer = buffer.toString();
            }
            callback(meta.notExists ? utils.errors.ERROR_NOT_EXISTS : err, buffer, mimeType);
        });
    }
    this.readFile = function (id, name, options, callback) {
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
                this.readFile(id, name, options, (err, res, mimeType) => {
                    if (!err) {
                        resolve({data: res, mimeType: mimeType});
                    } else {
                        reject(err);
                    }
                });
            });
        }

        options = options || {};
        checkFileRights(id, name, options, utils.consts.ACCESS_READ, (err, options, meta) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                return _readFile(id, name, options, callback, meta);
            }
        });
    };

    function _unlink(id, name, options, callback, meta) {
        if (meta && meta.notExists) {
            typeof callback === 'function' && callback(utils.errors.ERROR_NOT_EXISTS);
        } else {
            let metaID = getFileId(id, name, true);
            let dataID = getFileId(id, name, false);
            _delBinaryState(dataID, err => client.del(metaID, callback));
        }
    }
    this.unlink = function (id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options  = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        if (name[0] === '/') name = name.substring(1);

        checkFileRights(id, name, options, utils.consts.ACCESS_DELETE, (err, options, meta) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file['delete']) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    return _unlink(id, name, options, callback, meta);
                }
            }
        });
    };
    this.delFile = this.unlink;

    function _readDir(id, name, options, callback) {
        let dirID = getFileId(id, name);
        client.keys(dirID + '/*', (err, keys) => {
            if (!client) {
                return callback(utils.errors.ERROR_DB_CLOSED);
            }

            const start = dirID.length + 1;
            const end = '$%$meta'.length;

            const dirs = [];
            const deepLevel = name.split('/').length + 1;
            keys = keys
                .sort()
                .filter(key => {
                    if (key.match(/\$%\$meta$/)) {
                        const parts = key.split('/');
                        if (parts.length === deepLevel) {
                            return true;
                        } else {
                            const dir = name + '/' + parts[deepLevel];
                            if (dirs.indexOf(dir) === -1) {
                                dirs.push(dir);
                            }
                        }
                    }
                });
            if (!keys || !keys.length) {
                return callback(err, []);
            }
            // Check permissions
            client.mget(keys, (err, objs) => {
                const result = [];
                const dontCheck =
                    options.user === utils.consts.SYSTEM_ADMIN_USER ||
                    options.group !== utils.consts.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.consts.SYSTEM_ADMIN_GROUP) !== -1);

                for (let i = 0; i < keys.length; i++) {
                    const file = keys[i].substring(start, keys[i].length - end);
                    while (dirs.length && dirs[0] < file) {
                        result.push({
                            file: dirs.shift(),
                            stats: {},
                            isDir: true
                        });
                    }
                    objs[i] = JSON.parse(objs[i]);
                    if (dontCheck || utils.checkObject(objs[i], options, utils.consts.ACCESS_READ)) {
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
                callback(err, result);
            });
        });
    }
    this.readDir = function (id, name, options, callback) {
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

        checkFileRights(id, name, options, utils.consts.ACCESS_READ, (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.list) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    _readDir(id, name, options, callback);
                }
            }
        });
    };

    function _rename(id, oldName, newName, options, callback, meta) {
        const oldMetaID = getFileId(id, oldName, true);
        const oldDataID = getFileId(id, oldName, false);
        const newMetaID = getFileId(id, newName, true);
        const newDataID = getFileId(id, newName, false);
        if (!meta) {
            callback && callback('File not found');
        } else {
            client.get(oldDataID, (err, data) =>
                client.set(newMetaID, JSON.stringify(meta), err =>
                    client.set(newDataID, data, err =>
                        client.del(oldMetaID, err =>
                            client.del(oldDataID, callback)))));
        }
    }
    this.rename = function (id, oldName, newName, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        if (oldName[0] === '/') oldName = oldName.substring(1);
        if (newName[0] === '/') newName = newName.substring(1);
        if (oldName[oldName.length - 1] === '/') oldName = oldName.substring(0, oldName.length - 1);
        if (newName[newName.length - 1] === '/') newName = newName.substring(0, newName.length - 1);

        checkFileRights(id, oldName, options, utils.consts.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.write) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    _rename(id, oldName, newName, options, callback, meta);
                }
            }
        });
    };

    function _touch(id, name, options, callback, meta) {
        const metaID = getFileId(id, name, true);
        if (!meta) {
            callback && callback('File not found');
        } else {
            meta.modifiedAt = Date.now();
            client.set(metaID, JSON.stringify(meta), callback);
        }
    }
    this.touch = function (id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        checkFileRights(id, name, options, utils.consts.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                return _touch(id, name, options, callback, meta);
            }
        });
    };

    function _rmHelper(keys, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id = keys.shift();
            client.del(id, err =>
                _delBinaryState(id.replace(/\$%\$meta$/, '$%$data'), () =>
                    setImmediate(_rmHelper, keys, callback)));
        }
    }
    function _rm(id, name, options, callback, meta) {
        if (meta) {
            // it is file
            let metaID = getFileId(id, name, true);
            let dataID = getFileId(id, name, false);
            that.delObject(dataID, err => that.delObject(metaID, callback));
        } else {
            // it could be dir
            let dirID = getFileId(id, name);
            client.keys(dirID, (err, keys) => {
                if (!client) {
                    return callback(utils.errors.ERROR_DB_CLOSED);
                }

                keys = keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                // Check permissions
                client.mget(keys, (err, objs) => {
                    let result;
                    const dontCheck =
                        options.user === utils.consts.SYSTEM_ADMIN_USER ||
                        options.group !== utils.consts.SYSTEM_ADMIN_GROUP ||
                        (options.groups && options.groups.indexOf(utils.consts.SYSTEM_ADMIN_GROUP) !== -1);

                    if (!dontCheck) {
                        result = [];
                        for (let i = 0; i < keys.length; i++) {
                            objs[i] = JSON.parse(objs[i]);
                            if (utils.checkObject(objs[i], options, utils.consts.ACCESS_READ)) {
                                result.push(keys[i]);
                            }
                        }
                    } else {
                        result = keys;
                    }
                    const files = result.map(key => {
                        const name = key.substring(fileNamespaceL + id.length + 3, key.length - 7);
                        const pos = name.lastIndexOf('/');
                        if (pos !== -1) {
                            return {file: name.substring(pos + 1), path: name.substring(0, pos)};
                        } else {
                            return {file: id, path: ''};
                        }
                    });
                    _rmHelper(result, () => callback(null, files));
                });
            });
        }
    }
    this.rm = function (id, name, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (options && options.acl) {
            options.acl = null;
        }
        checkFileRights(id, null, options, utils.consts.ACCESS_DELETE, (err, options, meta) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file['delete']) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    return _rm(id, name, options, callback, meta && meta.notExists ? null : meta);
                }
            }
        });
    };

    // simulate. redis has no dirs
    this.mkdir = function (id, dirName, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        if (dirName[0] === '/') dirName = dirName.substring(1);

        checkFileRights(id, dirName, options, utils.consts.ACCESS_WRITE, function (err, options) {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.write) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    typeof callback === 'function' && callback();
                }
            }
        });
    };

    function _chownFileHelper(keys, metas, options, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id  = keys.shift();
            const meta = metas.shift();
            meta.acl.owner      = options.owner;
            meta.acl.ownerGroup = options.ownerGroup;
            client.set(id, JSON.stringify(meta), err => {
                setImmediate(_chownFileHelper, keys, metas, options, callback);
            });
        }
    }
    function _chownFile(id, name, options, callback, meta) {
        if (meta) {
            // it is file
            let metaID = getFileId(id, name, true);
            meta.acl.owner      = options.owner;
            meta.acl.ownerGroup = options.ownerGroup;
            that.setObject(metaID, meta, callback);
        } else {
            // it could be dir
            let dirID = getFileId(id, name);
            client.keys(dirID + '/*', (err, keys) => {
                if (!client) {
                    return callback(utils.errors.ERROR_DB_CLOSED);
                }

                keys = keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                // Check permissions
                client.mget(keys, (err, metas) => {
                    const dontCheck = options.user === utils.consts.SYSTEM_ADMIN_USER ||
                        options.group !== utils.consts.SYSTEM_ADMIN_GROUP ||
                        (options.groups && options.groups.indexOf(utils.consts.SYSTEM_ADMIN_GROUP) !== -1);
                    const keysFiltered = [];
                    const objsFiltered = [];
                    const processed = [];
                    const start = fileNamespaceL + dirID.length + 1;
                    const end = '$%$meta'.length;

                    for (let i = 0; i < keys.length; i++) {
                        metas[i] = JSON.parse(metas[i]);
                        if (dontCheck || utils.checkObject(metas[i], options, utils.consts.ACCESS_WRITE)) {
                            keysFiltered.push(keys[i]);
                            objsFiltered.push(metas[i]);

                            const name = keys[i].substring(start, keys[i].length - end);
                            processed.push({
                                path:       name,
                                file:       name.split('/').pop(),
                                stats:      metas[i].stats,
                                isDir:      false,
                                acl:        metas[i].acl || {},
                                modifiedAt: metas[i].modifiedAt,
                                createdAt:  metas[i].createdAt
                            });
                        }
                    }
                    _chownFileHelper(keysFiltered, objsFiltered, options, err => {
                        callback && callback(err, processed)
                    });
                });
            });
        }
    }
    this.chownFile = function (id, name, options, callback) {
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
            log.error(namespace + ' user is not defined');
            if (typeof callback === 'function') callback('invalid parameter');
            return;
        }

        if (!options.ownerGroup) {
            // get user group
            this.getUserGroup(options.owner, (user, groups /* , permissions */) => {
                if (!groups || !groups[0]) {
                    if (typeof callback === 'function') callback('user "' + options.owner + '" belongs to no group');
                    return;
                } else {
                    options.ownerGroup = groups[0];
                }
                this.chownFile(id, name, options, callback);
            });
            return;
        }

        checkFileRights(id, null, options, utils.consts.ACCESS_WRITE, (err, options, meta) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.write) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    return _chownFile(id, name, options, callback, meta);
                }
            }
        });
    };

    function _chmodFileHelper(keys, metas, options, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id   = keys.shift();
            const meta = metas.shift();
            meta.acl.permissions = options.mode;
            client.set(id, JSON.stringify(meta), err => {
                setImmediate(_chmodFileHelper, keys, metas, options, callback);
            });
        }
    }
    function _chmodFile(id, name, options, callback, meta) {
        if (meta) {
            // it is file
            let metaID = getFileId(id, name, true);
            meta.acl.permissions = options.mode;
            that.setObject(metaID, meta, callback);
        } else {
            // it could be dir
            let dirID = getFileId(id, name);
            client.keys(dirID + '/*', (err, keys) => {
                if (!client) {
                    return callback(utils.errors.ERROR_DB_CLOSED);
                }

                keys = keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                // Check permissions
                client.mget(keys, (err, objs) => {
                    const dontCheck =
                        options.user === utils.consts.SYSTEM_ADMIN_USER ||
                        options.group !== utils.consts.SYSTEM_ADMIN_GROUP ||
                        (options.groups && options.groups.indexOf(utils.consts.SYSTEM_ADMIN_GROUP) !== -1);

                    const keysFiltered = [];
                    const objsFiltered = [];
                    const processed = [];
                    const start = fileNamespaceL + dirID.length + 1;
                    const end = '$%$meta'.length;

                    for (let i = 0; i < keys.length; i++) {
                        objs[i] = JSON.parse(objs[i]);
                        if (dontCheck || utils.checkObject(objs[i], options, utils.consts.ACCESS_WRITE)) {
                            keysFiltered.push(keys[i]);
                            objsFiltered.push(objs[i]);

                            const name = keys[i].substring(start, keys[i].length - end);
                            processed.push({
                                path:       name,
                                file:       name.split('/').pop(),
                                stats:      objs[i].stats,
                                isDir:      false,
                                acl:        objs[i].acl || {},
                                modifiedAt: objs[i].modifiedAt,
                                createdAt:  objs[i].createdAt
                            });
                        }
                    }
                    _chmodFileHelper(keysFiltered, objsFiltered, options, err => {
                        callback && callback(err, processed)
                    });
                });
            });
        }
    }
    this.chmodFile = function (id, name, options, callback) {
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
            log.error(namespace + ' mode is not defined');
            if (typeof callback === 'function') callback('invalid parameter');
            return;
        } else if (typeof options.mode === 'string') {
            options.mode = parseInt(options.mode, 16);
        }

        checkFileRights(id, null, options, utils.consts.ACCESS_WRITE, function (err, options) {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.write) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    return _chmodFile(id, name, options, callback);
                }
            }
        });
    };

    this.enableFileCache = function (enabled, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }

        if (options && options.acl) {
            options.acl = null;
        }

        utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_WRITE, (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else if (typeof callback === 'function') {
                // cache cannot be enabled
                setImmediate(() => callback(null, false));
            }
        });
    };

    // -------------- OBJECT FUNCTIONS -------------------------------------------
    function _subscribe(pattern, options, callback) {
        log.silly(namespace + ' redis psubscribe ' + objNamespace + pattern);
        sub.psubscribe(objNamespace + pattern, err =>
            (typeof callback === 'function') && callback(err));
    }
    this.subscribeConfig = function (pattern, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        utils.checkObjectRights(that, null, null, options, 'list', (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                return _subscribe(pattern, options, callback);
            }
        });
    };
    this.subscribe   = this.subscribeConfig;

    function _unsubscribe(pattern, options, callback) {
        log.silly(namespace + ' redis punsubscribe ' + objNamespace + pattern);
        sub.punsubscribe(objNamespace + pattern, err =>
            (typeof callback === 'function') && callback(err));
    }
    this.unsubscribeConfig = function (pattern, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        utils.checkObjectRights(that, null, null, options, 'list', (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                return _unsubscribe(pattern, options, callback);
            }
        });
    };
    this.unsubscribe = this.unsubscribeConfig;

    function _objectHelper(keys, objs, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id  = keys.shift();
            const obj = objs.shift();
            const message = JSON.stringify(obj);
            client.set(id, message, err => {
                client.publish(id, message);
                setImmediate(_objectHelper, keys, objs, callback);
            });
        }
    }
    function _chownObject(pattern, options, callback) {
        that.getConfigKeys(pattern, options, (err, keys) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
                return;
            }
            client.mget(keys, (err, objects) => {
                const filteredKeys = [];
                const filteredObjs = [];
                for (let k = 0; k < keys.length; k++) {
                    objects[k] = JSON.parse(objects[k]);
                    if (!utils.checkObject(objects[k], options, utils.consts.ACCESS_WRITE)) continue;
                    if (!objects[k].acl) {
                        objects[k].acl = {
                            owner:      (defaultNewAcl && defaultNewAcl.owner)      || utils.consts.SYSTEM_ADMIN_USER,
                            ownerGroup: (defaultNewAcl && defaultNewAcl.ownerGroup) || utils.consts.SYSTEM_ADMIN_GROUP,
                            object:     (defaultNewAcl && defaultNewAcl.object)     || (utils.consts.ACCESS_USER_RW | utils.consts.ACCESS_GROUP_READ | utils.consts.ACCESS_EVERY_READ) // '0644'
                        };
                        if (objects[k].type === 'state') {
                            objects[k].acl.state = (defaultNewAcl && defaultNewAcl.state) || (utils.consts.ACCESS_USER_RW | utils.consts.ACCESS_GROUP_READ | utils.consts.ACCESS_EVERY_READ); // '0644'
                        }
                    }
                    objects[k].acl.owner      = options.owner;
                    objects[k].acl.ownerGroup = options.ownerGroup;
                    filteredKeys.push(keys[k]);
                    filteredObjs.push(objects[k]);
                }
                _objectHelper(filteredKeys, filteredObjs, () =>
                    typeof callback === 'function' && callback(null, filteredObjs));
            });
        }, true);
    }
    this.chownObject = function (pattern, options, callback) {
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
            log.error(namespace + ' user is not defined');
            if (typeof callback === 'function') callback('invalid parameter');
            return;
        }

        if (!options.ownerGroup) {
            // get user group
            this.getUserGroup(options.owner, (user, groups /* , permissions*/) => {
                if (!groups || !groups[0]) {
                    if (typeof callback === 'function') callback('user "' + options.owner + '" belongs to no group');
                    return;
                } else {
                    options.ownerGroup = groups[0];
                }
                this.chownObject(pattern, options, callback);
            });
            return;
        }

        utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_WRITE, (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.object || !options.acl.object.write) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    return _chownObject(pattern, options, callback);
                }
            }
        });
    };

    function _chmodObject(pattern, options, callback) {
        that.getConfigKeys(pattern, options, (err, keys) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
                return;
            }
            client.mget(keys, (err, objects) => {
                const filteredKeys = [];
                const filteredObjs = [];
                for (let k = 0; k < keys.length; k++) {
                    objects[k] = JSON.parse(objects[k]);
                    if (!utils.checkObject(objects[k], options, utils.consts.ACCESS_WRITE)) continue;
                    if (!objects[k].acl) {
                        objects[k].acl = {
                            owner:      (defaultNewAcl && defaultNewAcl.owner)      || utils.consts.SYSTEM_ADMIN_USER,
                            ownerGroup: (defaultNewAcl && defaultNewAcl.ownerGroup) || utils.consts.SYSTEM_ADMIN_GROUP,
                            object:     (defaultNewAcl && defaultNewAcl.object)     || (utils.consts.ACCESS_USER_RW | utils.consts.ACCESS_GROUP_READ | utils.consts.ACCESS_EVERY_READ) // '0644'
                        };
                        if (objects[k].type === 'state') {
                            objects[k].acl.state = (defaultNewAcl && defaultNewAcl.state) || (utils.consts.ACCESS_USER_RW | utils.consts.ACCESS_GROUP_READ | utils.consts.ACCESS_EVERY_READ); // '0644'
                        }
                    }
                    if (options.object !== undefined) objects[k].acl.object = options.object;
                    if (options.state !== undefined) objects[k].acl.state = options.state;
                    filteredKeys.push(keys[k]);
                    filteredObjs.push(objects[k]);
                }
                _objectHelper(filteredKeys, filteredObjs, () =>
                    typeof callback === 'function' && callback(null, filteredObjs));
            });
        }, true);
    }
    this.chmodObject = function (pattern, options, callback) {
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
            log.error(namespace + ' mode is not defined');
            if (typeof callback === 'function') callback('invalid parameter');
            return;
        } else if (typeof options.mode === 'string') {
            options.mode = parseInt(options.mode, 16);
        }

        utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_WRITE, (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.write) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    return _chmodObject(pattern, options, callback);
                }
            }
        });
    };

    function _getObject(id, options, callback) {
        client.get(objNamespace + id, (err, obj) => {
            if (err) {
                log.warn(namespace + ' redis get ' + id + ', error - ' + err);
            } else {
                log.silly(namespace + ' redis get ' + id + ' ok: ' + obj);
            }
            try {
                obj = obj ? JSON.parse(obj) : null;
            } catch (e) {
                log.error(`${namespace} Cannot parse ${id} - ${obj}: ${JSON.stringify(e)}`);
            }
            if (obj) {
                // Check permissions
                if (utils.checkObject(obj, options, utils.consts.ACCESS_READ)) {
                    callback(null, obj);
                } else {
                    callback(utils.errors.ERROR_PERMISSION);
                }

            } else {
                callback(err, obj);
            }
        });
    }
    this.getObject = function (id, options, callback) {
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
            utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_READ, (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _getObject(id, options, callback);
                }
            });
        }
    };

    this.getObjectAsync = function (id, options) {
        return new Promise((resolve, reject) => {
            this.getObject(id, options, (err, obj) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(obj);
                }
            });
        });
    };

    function _getKeys(pattern, options, callback, dontModify) {
        let r = new RegExp(tools.pattern2RegEx(pattern));
        client.keys(objNamespace + pattern, (err, keys) => {
            if (!client) {
                return callback(utils.errors.ERROR_DB_CLOSED);
            }

            log.silly(namespace + ' redis keys ' + keys.length + ' ' + pattern);
            let result = [];
            if (keys) {
                keys.sort();
                const result = [];
                const dontCheck =
                    options.user === utils.consts.SYSTEM_ADMIN_USER ||
                    options.group !== utils.consts.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.consts.SYSTEM_ADMIN_GROUP) !== -1);


                if (dontCheck) {
                    for (let i = 0; i < keys.length; i++) {
                        const id = keys[i].substring(objNamespaceL);
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
                    client.mget(keys, (err, metas) => {
                        for (let i = 0; i < keys.length; i++) {
                            metas[i] = JSON.parse(metas[i]);
                            if (r.test(keys[i]) && utils.checkObject(metas[i], options, utils.consts.ACCESS_READ)) {
                                if (!dontModify) {
                                    result.push(keys[i].substring(objNamespaceL));
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
    this.getKeys = function (pattern, options, callback, dontModify) {
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
            utils.checkObjectRights(that, null, null, options, 'list', (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _getKeys(pattern, options, callback, dontModify);
                }
            });
        }
    };
    this.getConfigKeys = this.getKeys;

    function _getObjects(keys, options, callback, dontModify) {
        if (!keys) {
            if (typeof callback === 'function') callback('no keys', null);
            return;
        }
        if (!keys.length) {
            if (typeof callback === 'function') callback(null, []);
            return;
        }

        let _keys;
        if (!dontModify) {
            _keys = [];
            for (let i = 0; i < keys.length; i++) {
                _keys[i] = objNamespace + keys[i];
            }
        } else {
            _keys = keys;
        }
        client.mget(_keys, (err, objs) => {
            let result = [];
            if (err) {
                log.warn(namespace + ' redis mget ' + (!objs ? 0 :  objs.length) + ' ' + _keys.length + ', err: ' + err);
            } else {
                log.silly(namespace + ' redis mget ' + (!objs ? 0 : objs.length) + ' ' + _keys.length);
            }
            if (objs) {
                const dontCheck =
                    options.user === utils.consts.SYSTEM_ADMIN_USER ||
                    options.group !== utils.consts.SYSTEM_ADMIN_GROUP ||
                    (options.groups && options.groups.indexOf(utils.consts.SYSTEM_ADMIN_GROUP) !== -1);

                if (!dontCheck) {
                    for (let i = 0; i < objs.length; i++) {
                        objs[i] = JSON.parse(objs[i]);
                        if (utils.checkObject(objs[i], options, utils.consts.ACCESS_READ)) {
                            result.push(objs[i]);
                        } else {
                            result.push({error: utils.errors.ERROR_PERMISSION});
                        }
                    }
                } else {
                    result = objs.map(obj => JSON.parse(obj));
                }
            }
            callback(null, result);
        });
    }
    this.getObjects = function (keys, options, callback, dontModify) {
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
            utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_READ, (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _getObjects(keys, options, callback, dontModify);
                }
            });
        }
    };

    function _getObjectsByPattern(pattern, options, callback) {
        client.keys(objNamespace + pattern, (err, keys) => {
            if (!client) {
                return callback(utils.errors.ERROR_DB_CLOSED);
            }

            log.silly(namespace + ' redis keys ' + keys.length + ' ' + pattern);
            _getObjects(keys, options, callback, true);
        });
    }
    this.getObjectsByPattern = (pattern, options, callback) => {
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
            utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_READ, (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _getObjectsByPattern(pattern, options, callback);
                }
            });
        }
    };

    function _setObject(id, obj, options, callback) {
        if (!id || utils.regCheckId.test(id)) {
            if (typeof callback === 'function') {
                callback(`Invalid ID: ${id}`);
            }
            return;
        }

        if (!obj) {
            log.error(namespace + ' setObject: Argument object is null');
            if (typeof callback === 'function') {
                callback('obj is null');
            }
            return;
        }

        obj._id = id;
        client.get(objNamespace + id, (err, oldObj) => {
            oldObj = oldObj && JSON.parse(oldObj);

            if (!tools.checkNonEditable(oldObj, obj)) {
                if (typeof callback === 'function') {
                    callback('Invalid password for update of vendor information');
                }
                return;
            }

            // do not delete common settings, like "history" or "mobile". It can be erased only with "null"
            if (oldObj && oldObj.common) {
                for (let i = 0; i < preserveSettings.length; i++) {
                    // remove settings if desired
                    if (obj.common && obj.common[preserveSettings[i]] === null) {
                        delete obj.common[preserveSettings[i]];
                        continue;
                    }

                    if (oldObj.common[preserveSettings[i]] !== undefined && (!obj.common || obj.common[preserveSettings[i]] === undefined)) {
                        if (!obj.common) obj.common = {};
                        obj.common[preserveSettings[i]] = oldObj.common[preserveSettings[i]];
                    }
                }
            }

            if (oldObj && oldObj.acl && !obj.acl) {
                obj.acl = oldObj.acl;
            }

            // add user default rights
            if (defaultNewAcl && !obj.acl) {
                obj.acl = JSON.parse(JSON.stringify(defaultNewAcl));
                delete obj.acl.file;
                if (obj.type !== 'state') {
                    delete obj.acl.state;
                }
                if (options.owner) {
                    obj.acl.owner = options.owner;

                    if (!options.ownerGroup) {
                        obj.acl.ownerGroup = null;
                        return that.getUserGroup(options.owner, (user, groups /* , permissions */) => {
                            if (!groups || !groups[0]) {
                                options.ownerGroup = (defaultNewAcl && defaultNewAcl.ownerGroup) || utils.consts.SYSTEM_ADMIN_GROUP;
                            } else {
                                options.ownerGroup = groups[0];
                            }
                            obj.acl.ownerGroup = options.ownerGroup;
                            const message = JSON.stringify(obj);
                            client.set(objNamespace + id, message, err => {
                                client.publish(objNamespace + id, message);
                                typeof callback === 'function' && callback(err);
                            });
                        });
                    }
                }
            }
            if (defaultNewAcl && obj.acl && !obj.acl.ownerGroup && options.ownerGroup) {
                obj.acl.ownerGroup = options.ownerGroup;
            }
            const message = JSON.stringify(obj);
            client.set(objNamespace + id, message, err => {
                client.publish(objNamespace + id, message);
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
    this.setObject = function (id, obj, options, callback) {
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

        utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_WRITE, (err, options) => {
            if (err) {
                if (typeof callback === 'function') {
                    callback(err);
                }
            } else {
                return _setObject(id, obj, options, callback);
            }
        });
    };

    this.setObjectAsync = (id, obj, options) => {
        return new Promise((resolve, reject) => {
            this.setObject(id, obj, options, (err, res) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(res);
                }
            })
        });
    };

    function _delObject(id, options, callback) {
        if (!id || utils.regCheckId.test(id)) {
            return typeof callback === 'function' && callback(`Invalid ID: ${id}`);
        }

        // read object
        client.get(objNamespace + id, (err, oldObj) => {
            if (err) {
                log.warn(namespace + ' redis get ' + id + ', error - ' + err);
            } else {
                log.silly(namespace + ' redis get ' + id + ' ok: ' + oldObj);
            }
            if (!oldObj) {
                return typeof callback === 'function' && callback(utils.errors.ERROR_NOT_EXISTS);
            }

            try {
                oldObj = oldObj ? JSON.parse(oldObj) : null;
            } catch (e) {
                log.error(`${namespace} Cannot parse ${id} - ${oldObj}: ${JSON.stringify(e)}`);
            }

            if (!utils.checkObject(oldObj, options, utils.consts.ACCESS_WRITE)) {
                typeof callback === 'function' && callback({error: utils.errors.ERROR_PERMISSION});
            } else {
                client.del(objNamespace + id, function (err) {
                    client.publish(objNamespace + id, 'null');
                    typeof callback === 'function' && callback(err);
                });
            }
        });
    }
    this.delObject = function (id, options, callback) {
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
        utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_DELETE, (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                return _delObject(id, options, callback);
            }
        });
    };

    this.delObjectAsync = function (id, options) {
        return new Promise((resolve, reject) => {
            this.delObject(id, options, err => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    };

    // this function is very ineffective. Because reads all objects and then process them
    function _applyView(func, params, options, callback) {
        let result = {
            rows: []
        };

        params = params || {};
        params.startkey = params.startkey || '';
        params.endkey   = params.endkey    || '\u9999';
        let m;
        // filter by type
        if (func && func.map && scripts.filter && (m = func.map.match(/if\s\(doc\.type\s?===?\s?'(\w+)'\)\semit/))) {
            client.evalsha([scripts.filter, 4, objNamespace, params.startkey, params.endkey, m[1]], (err, objs) => {
                err && log.error(`${namespace} Cannot get view ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    obj = JSON.parse(obj);
                    return {id: obj._id, value: obj};
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
        if (func && func.map && scripts.script && func.map.indexOf('doc.common.engineType') !== -1) {
            client.evalsha([scripts.script, 3, objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && log.error(`${namespace} Cannot get view ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    obj = JSON.parse(obj);
                    return {id: obj._id, value: obj};
                });
                callback(null, result);
            });
        } else
        // filter by hm-rega programs
        if (func && func.map && scripts.programs && func.map.indexOf('doc.native.TypeName === \'PROGRAM\'') !== -1) {
            client.evalsha([scripts.programs, 3, objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && log.error(`${namespace} Cannot get view ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    obj = JSON.parse(obj);
                    return {id: obj._id, value: obj};
                });
                callback(null, result);
            });
        } else
        // filter by hm-rega variables
        if (func && func.map && scripts.variables && func.map.indexOf('doc.native.TypeName === \'ALARMDP\'') !== -1) {
            client.evalsha([scripts.variables, 3, objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && log.error(`${namespace} Cannot get view ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    obj = JSON.parse(obj);
                    return {id: obj._id, value: obj};
                });
                callback(null, result);
            });
        } else
        // filter by custom
        if (func && func.map && scripts.custom && func.map.indexOf('doc.common.custom') !== -1) {
            client.evalsha([scripts.custom, 3, objNamespace, params.startkey, params.endkey], (err, objs) => {
                err && log.error(`${namespace} Cannot get view ${err}`);
                objs = objs || [];
                result.rows = objs.map(obj => {
                    obj = JSON.parse(obj);
                    return {id: obj._id, value: obj.common.custom};
                });
                callback(null, result);
            });
        } else {
            console.log('UNOPTIMIZED!: ' + func.map);

            const _emit_ = function (id, obj) {
                result.rows.push({id: id, value: obj});
            };

            client.keys(objNamespace + '*', (err, keys) => {
                if (!client) {
                    return callback(utils.errors.ERROR_DB_CLOSED);
                }
                params.startkey = objNamespace + params.startkey;
                params.endkey   = objNamespace + params.endkey;

                keys = keys.sort().filter(key => {
                    if (key && !utils.regCheckId.test(key)) {
                        if (params) {
                            if (params.startkey && key < params.startkey) return false;
                            if (params.endkey   && key > params.endkey)   return false;
                        }
                        return true;
                    } else {
                        return false;
                    }
                });

                client.mget(keys, (err, objs) => {
                    let f = eval('(' + func.map.replace(/emit/g, '_emit_') + ')');

                    for (let i = 0; i < keys.length; i++) {
                        objs[i] = JSON.parse(objs[i]);
                        if (!utils.checkObject(objs[i], options, utils.consts.ACCESS_READ)) continue;

                        if (objs[i]) {
                            try {
                                f(objs[i]);
                            } catch (e) {
                                console.log('Cannot execute map: ' + e.message);
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
    this._applyView = function (func, params, options, callback) {
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
            utils.checkObjectRights(that, null, null, options, 'list', (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _applyView(func, params, options, callback);
                }
            });
        }
    };

    function _getObjectView(design, search, params, options, callback) {
        client.get(objNamespace + '_design/' + design, (err, obj) => {
            if (obj) {
                obj = JSON.parse(obj);
                if (obj.views && obj.views[search]) {
                    _applyView(obj.views[search], params, options, callback);
                } else {
                    console.log('Cannot find search "' + search + '" in "' + design + '"');
                    callback({status_code: 404, status_text: 'Cannot find search "' + search + '" in "' + design + '"'});
                }
            } else {
                console.log('Cannot find view "' + design + '"');
                callback({status_code: 404, status_text: 'Cannot find view "' + design + '"'});
            }
        });
    }
    this.getObjectView = function (design, search, params, options, callback) {
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
            utils.checkObjectRights(that, null, null, options, 'list', (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _getObjectView(design, search, params, options, callback);
                }
            });
        }
    };

    function _getObjectList(params, options, callback) {
        //params = {startkey, endkey, include_docs}
        params = params || {};
        params.startkey = params.startkey || '';
        params.endkey = params.endkey || '\u9999';
        let pattern = (params.endkey.substring(0, params.startkey.length) === params.startkey) ? objNamespace + params.startkey + '*' : objNamespace + '*';

        // todo: use lua script for that
        client.keys(pattern, (err, keys) => {
            if (!client) {
                return callback(utils.errors.ERROR_DB_CLOSED);
            }

            let _keys = [];
            for (let i = 0; i < keys.length; i++) {
                const id = keys[i].substring(objNamespaceL);
                if (params.startkey && id < params.startkey) continue;
                if (params.endkey && id > params.endkey) continue;
                if (!id || utils.regCheckId.test(id) || id.match(/\|file\$%\$/)) continue;
                if (!params.include_docs && id[0] === '_') continue;
                _keys.push(keys[i]);
            }
            keys.sort();
            client.mget(_keys, (err, objs) => {
                // return rows with id and doc
                let result = {
                    rows: []
                };
                if (objs) {
                    for (let r = 0; r < objs.length; r++) {
                        objs[r] = JSON.parse(objs[r]);
                        if (!utils.checkObject(objs[r], options, utils.consts.ACCESS_READ)) continue;
                        result.rows.push({id: objs[r]._id, value: objs[r], doc: objs[r]});
                    }
                }
                callback(null, result);
            });
        });
    }
    this.getObjectList = function (params, options, callback) {
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
            utils.checkObjectRights(that, null, null, options, 'list', (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _getObjectList(params, options, callback);
                }
            });
        }
    };

    this.getObjectListAsync = (params, options) => {
        return new Promise((resolve, reject) => {
            this.getObjectList(params, options, (err, arr) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(arr);
                }
            })
        });
    };

    // could be optimised, to read object only once. Now it will read 3 times
    function _extendObject(id, obj, options, callback, iteration) {
        if (!id || utils.regCheckId.test(id)) {
            typeof callback === 'function' && callback(`Invalid ID: ${id}`);
        } else {
            client.get(objNamespace + id, (err, oldObj) => {
                oldObj = oldObj && JSON.parse(oldObj);
                if (!utils.checkObject(oldObj, options, utils.consts.ACCESS_WRITE)) {
                    return typeof callback === 'function' && callback(utils.errors.ERROR_PERMISSION);
                }

                let _oldObj;
                if (oldObj && oldObj.nonEdit) {
                    _oldObj = JSON.parse(JSON.stringify(oldObj));
                }

                oldObj = oldObj || {};
                oldObj = extend(true, oldObj, obj);
                oldObj._id = id;

                // add user default rights
                if (defaultNewAcl && !oldObj.acl) {
                    oldObj.acl = JSON.parse(JSON.stringify(defaultNewAcl));
                    delete oldObj.acl.file;
                    if (oldObj.type !== 'state') {
                        delete oldObj.acl.state;
                    }

                    if (options.owner) {
                        oldObj.acl.owner = options.owner;

                        if (!options.ownerGroup) {
                            oldObj.acl.ownerGroup = null;
                            return that.getUserGroup(options.owner, (user, groups /*, permissions */) => {
                                if (!groups || !groups[0]) {
                                    options.ownerGroup = (defaultNewAcl && defaultNewAcl.ownerGroup) || utils.consts.SYSTEM_ADMIN_GROUP;
                                } else {
                                    options.ownerGroup = groups[0];
                                }
                                _extendObject(id, obj, options, callback);
                            });
                        }
                    }
                }

                if (defaultNewAcl && options.ownerGroup && oldObj.acl && !oldObj.acl.ownerGroup) {
                    oldObj.acl.ownerGroup = options.ownerGroup;
                }

                if (_oldObj && !tools.checkNonEditable(_oldObj, oldObj)) {
                    return typeof callback === 'function' && callback('Invalid password for update of vendor information');
                }
                const message = JSON.stringify(oldObj);
                client.set(objNamespace + id, message, err => {
                    if (!err) {
                        client.publish(objNamespace + id, message);
                        callback(null, {id: id, value: oldObj}, id);
                    } else {
                        callback(err);
                    }
                });
            });
        }
    }
    this.extendObject = function (id, obj, options, callback) {
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

        utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_WRITE, (err, options) => {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                return _extendObject(id, obj, options, callback);
            }
        });
    };

    this.extendObjectAsync = function (id, obj, options) {
        return new Promise((resolve, reject) => {
            this.extendObject(id, obj, options, err => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    };

    this.setConfig = this.setObject;

    this.delConfig = this.delObject;

    this.getConfig = this.getObject;

    this.getConfigs = this.getObjects;

    function _findObject(idOrName, type, options, callback) {
        _getObject(idOrName, options, (err, obj) => {
            // Assume it is ID
            if (obj && utils.checkObject(obj, options, utils.consts.ACCESS_READ) && (!type || (obj.common && obj.common.type === type))) {
                callback(null, idOrName, obj.common.name);
            } else {
                _getKeys('*', options, (err, keys) => {
                    client.mget(keys, (err, objs) => {
                        // Assume it is name
                        for (let i = 0; i < keys.length; i++) {
                            objs[i] = JSON.parse(objs[i]);
                            if (objs[i].common &&
                                objs[i].common.name === idOrName &&
                                (!type || (objs[i].common && objs[i].common.type === type))) {
                                if (typeof callback === 'function') callback(null, objs[i]._id, idOrName);
                                return;
                            }
                        }
                        if (typeof callback === 'function') callback(null, null, idOrName);
                    });
                }, true);
            }
        });
    }
    this.findObject = function (idOrName, type, options, callback) {
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
                this.findObject(idOrName, type, options, (err, id, idOrName) => {
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
            utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_LIST, (err, options) => {
                if (err) {
                    if (typeof callback === 'function') callback(err);
                } else {
                    return _findObject(idOrName, type, options, callback);
                }
            });
        }
    };

    // can be called only from js-controller
    this.addPreserveSettings = function (settings) {
        if (typeof settings !== 'object') settings = [settings];

        for (let s = 0; s < settings.length; s++) {
            if (preserveSettings.indexOf(settings[s]) === -1) preserveSettings.push(settings[s]);
        }
    };

    function _destroyDBHelper(keys, callback) {
        if (!keys || !keys.length) {
            callback();
        } else {
            const id = keys.shift();
            client.del(id, err =>
                setImmediate(_destroyDBHelper, keys, callback));
        }
    }
    function _destroyDB(options, callback) {
        client.keys(redisNamespace + '*', (err, keys) => {
            _destroyDBHelper(keys, callback);
        });
    }
    this.destroyDB = function (options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options = null;
        }
        options = options || {};

        utils.checkObjectRights(that, null, null, options, utils.consts.ACCESS_WRITE, function (err, options) {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.write || options.user !== utils.consts.SYSTEM_ADMIN_USER) {
                    if (typeof callback === 'function') callback(utils.errors.ERROR_PERMISSION);
                } else {
                    return _destroyDB(options, callback);
                }
            }
        });
    };

    // Destructor of the class. Called by shutting down.
    this.destroy = () => {
        if (client) {
            client.quit();
            client = null;
        }
        if (clientBin) {
            clientBin.quit();
            clientBin = null;
        }
        if (sub) {
            sub.quit();
            sub = null;
        }
    };

    function loadLuaScripts(callback, _scripts) {
        if (!_scripts) {
            if (scriptFiles && scriptFiles.filter) {
                _scripts = [];
                for (const name in scriptFiles) {
                    if (!scriptFiles.hasOwnProperty(name)) continue;
                    const shasum = crypto.createHash('sha1');
                    const buf = new Buffer(scriptFiles[name]);
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
            return client.send_command('SCRIPT', hashes, (err, arr) => {
                _scripts.forEach((e, i) => {
                    _scripts[i].loaded = !!arr[i];
                });
                loadLuaScripts(callback, _scripts);
            });
        }
        for (let i = 0; i < _scripts.length; i++) {
            if (!_scripts[i].loaded) {
                const script = _scripts[i];
                return client.send_command('SCRIPT', ['LOAD', script.text], (err, hash) => {
                    script.hash = hash;
                    script.loaded = true;
                    err && log.error(namespace + ' Cannot load "' + script.name + '": ' + err);
                    setImmediate(loadLuaScripts, callback, _scripts);
                });
            }
        }
        scripts = {};
        _scripts.forEach(e => scripts[e.name] = e.hash);
        callback();
    }

    (function __construct() {
        if (settings.connection.port === 0) {
            // initiate a unix socket connection using the parameter 'host'
            client = redis.createClient(settings.connection.host, settings.connection.options);
            sub    = redis.createClient(settings.connection.host, settings.connection.options);
        } else {
            client = redis.createClient(settings.connection.port, settings.connection.host, settings.connection.options);
            sub    = redis.createClient(settings.connection.port, settings.connection.host, settings.connection.options);
        }

        if (typeof onChange === 'function') {
            sub.on('pmessage', (pattern, channel, message) => {
                log.debug(namespace + ' redis pmessage ', pattern, channel, message);

                try {
                    if (ioRegExp.test(channel)) {
                        const id = channel.substring(objNamespaceL);
                        try {
                            const obj = message ? JSON.parse(message) : null;

                            if (settings.controller &&
                                id === 'system.config' &&
                                obj &&
                                obj.common &&
                                JSON.stringify(obj.common.defaultNewAcl) !== JSON.stringify(defaultNewAcl)) {
                                defaultNewAcl = JSON.parse(JSON.stringify(obj.common.defaultNewAcl));
                                setDefaultAcl(defaultNewAcl);
                            }

                            onChange(id, obj);
                        } catch (e) {
                            log.error(`${namespace} Cannot parse ${id} - ${message}: ${JSON.stringify(e)}`);
                        }
                    } else {
                        log.error(`${namespace} Received unexpected pmessage: ${channel}`);
                    }
                } catch (e) {
                    log.error(namespace + ' pmessage ' + channel + ' ' + message + ' ' + e.message);
                    log.error(namespace + ' ' + e.stack);
                }
            });
        }

        client.on('error', error => {
            if (typeof settings.disconnected === 'function') {
                settings.disconnected(error);
            } else {
                log.error(namespace + ' ' + error.message);
                log.error(namespace + ' ' + error.stack);
            }
        });

        sub.on('error', error => {
            log.error(namespace + ' No redis connection!');
        });

        sub.on('connect', error => {
            if (settings.connection.port === 0) {
                log.info(namespace + ' Objects connected to redis: ' + settings.connection.host);
            } else {
                log.info(namespace + ' Objects connected to redis: ' + settings.connection.host + ':' + settings.connection.port);
            }
            // subscribe on system.config only if js-controller
            if (settings.controller) {
                sub.psubscribe(objNamespace + 'system.config');
            }
        });

        client.on('end', () =>
            typeof settings.disconnected === 'function' && settings.disconnected());

        client.on('connect', error => {
            loadLuaScripts(() => {
                // init default new acl
                client.get(objNamespace + 'system.config', (err, obj) => {
                    if (obj) {
                        obj = JSON.parse(obj);
                        if (obj.common && obj.common.defaultNewAcl) {
                            defaultNewAcl = obj.common.defaultNewAcl;
                        }
                    }
                    typeof settings.connected === 'function' && settings.connected();
                });
            });
        });
    })();
    return this;
}

module.exports = ObjectsInRedis;