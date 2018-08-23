/**
 *      Object DB in REDIS - Client
 *
 *      Copyright 2014-2018 bluefox <dogafox@gmail.com>
 *
 *      CC BY-NC-ND 4.0
 *      Attribution-NonCommercial-NoDerivatives 4.0 International
 *
 */
/* jshint -W097 */
/* jshint strict:false */
/* jslint node: true */
/* jshint -W061 */
'use strict';

const extend      = require('node.extend');
const tools       = require('../tools');
const redis       = require('redis');
const utils       = require('./objectsUtils');

function ObjectsInRedis(settings) {
    settings = settings || {};
    const redisNamespace  = (settings.redisNamespace || 'config') + '.';
    const ioRegExp        = new RegExp('^' + redisNamespace);
    const onChange        = settings.change; // on change handler
    const redisNamespaceL = redisNamespace.length;

    let client;
    let sub;
    let that                = this;
    let preserveSettings    = ['custom'];
    const filePrefix        = 'file$%$';
    const fileRegEx         = new RegExp('^' + redisNamespace + '\|' + filePrefix.replace(/\$/g, '\\$'));
    let defaultNewAcl       = settings.defaultNewAcl || null;
    let namespace           = settings.namespace || settings.hostname || '';
    let clientBin;

    let log = utils.getLogger(settings.logger);

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
        clientBin.get(id, function (err, data) {
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
        return redisNamespace + '|' + filePrefix + id + '$%$' + name + '$%$' + (isMeta !== undefined ? (isMeta ? 'meta' : 'data') : '');
    }

    function checkFile(id, name, options, flag, callback) {
        // read file settings from redis
        that.getObject(getFileId(id, name, true), (err, fileOptions) => {
            fileOptions = fileOptions || {};
            if (utils.checkFile(fileOptions, options, flag, defaultNewAcl)) {
                return callback && callback(false, options, fileOptions); // NO error
            } else {
                return callback && callback(true, options); // error
            }
        });
    }

    function checkFileRights(id, name, options, flag, callback) {
        options = options || {};
        if (!options.user) {
            // Before files converted, lets think: if no options it is admin
            options = {
                user:    'system.user.admin',
                params:  options,
                group:   'system.group.administrator'
            };
        }

        if (options.checked) {
            return callback(null, options);
        }

        if (!options.acl) {
            that.getUserGroup(options.user, (user, groups, acl) => {
                options.acl    = acl || {};
                options.groups = groups;
                options.group  = groups ? groups[0] : null;
                checkFileRights(id, name, options, flag, callback);
            });
            return;
        }
        // If user may write
        if (flag === utils.consts.ACCESS_WRITE && !options.acl.file.write) {// write
            return callback(utils.consts.PERMISSION_ERROR, options);
        }
        // If user may read
        if (flag === utils.consts.ACCESS_READ && !options.acl.file.read) {// read
            return callback(utils.consts.PERMISSION_ERROR, options);
        }

        options.checked = true;
        checkFile(id, name, options, flag, (err, opt, meta) => {
            if (err) {
                return callback(utils.consts.PERMISSION_ERROR, opt);
            } else {
                return callback(null, options, meta);
            }
        });


        /*if (typeof fileOptions[id][name].acl != 'object') {
         fileOptions[id][name] = {
         mimeType: fileOptions[id][name],
         acl: {
         owner:       'system.user.admin',
         permissions: 0x644,
         ownerGroup:  'system.group.administrator'
         }
         };
         }
         // Set default onwer group
         fileOptions[id][name].acl.ownerGroup = fileOptions[id][name].acl.ownerGroup || 'system.group.administrator';

         if (options.user != 'system.user.admin' &&
         options.groups.indexOf('system.group.administrator') == -1 &&
         fileOptions[id][name].acl) {
         if (fileOptions[id][name].acl.owner != options.user) {
         // Check if the user is in the group
         if (options.groups.indexOf(fileOptions[id][name].acl.ownerGroup) != -1) {
         // Check group rights
         if (!(fileOptions[id][name].acl.permissions & (flag << 4))) {
         return callback(utils.consts.PERMISSION_ERROR, options);
         }
         } else {
         // everybody
         if (!(fileOptions[id][name].acl.permissions & flag)) {
         return callback(utils.consts.PERMISSION_ERROR, options);
         }
         }
         } else {
         // Check user rights
         if (!(fileOptions[id][name].acl.permissions & (flag << 8))) {
         return callback(utils.consts.PERMISSION_ERROR, options);
         }
         }
         }
         return callback(null, options);*/
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
            owner: 'system.user.admin',
            ownerGroup: 'system.group.administrator',
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
                owner:       options.user  || (defaultNewAcl && defaultNewAcl.owner)      || 'system.user.admin',
                ownerGroup:  options.group || (defaultNewAcl && defaultNewAcl.ownerGroup) || 'system.group.administrator',
                permissions: options.mode  || (defaultNewAcl && defaultNewAcl.file)       || 0x644
            };
        }
        meta.stats = {
            size: data.length
        };

        meta.mimeType       = options.mimeType || _mimeType;
        meta.binary         = isBinary;
        meta.acl.ownerGroup = meta.acl.ownerGroup || (defaultNewAcl && defaultNewAcl.ownerGroup) || 'system.group.administrator';
        meta.modifiedAt     = Date.now();

        that.client(metaID, meta, err => _setBinaryState(getFileId(id, name, false), data, err => callback && callback(err)));
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
            let mimeType = meta.mimeType;
            if (!meta.binary) {
                buffer = buffer.toString();
            }
            callback(err, buffer, mimeType);
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
                this.readFile(id, name, options, (err, res, mimeType) =>{
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
        let metaID = getFileId(id, name, true);
        let dataID = getFileId(id, name, false);
        _delBinaryState(dataID, err => that.delObject(metaID, callback));
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
                } else {
                    return _unlink(id, name, options, callback, meta);
                }
            }
        });
    };
    this.delFile = this.unlink;

    function _readDir(id, name, options, callback) {
        let dirID = getFileId(id, name);
        client.keys(redisNamespace + dirID + '/*', (err, keys) => {
            const start = redisNamespace.length + dirID.length - name.length;
            const end = '$%$meta'.length;

            const dirs = [];
            const deepLevel = name.split('/').length + 1;
            keys
                .sort()
                .filter(key => {
                    if (key.match(/\$%\$meta$/)) {
                        const parts = key.split('/');
                        if (parts.split('/').length === deepLevel) {
                            return true;
                        } else {
                            const dir = name + '/' + parts[deepLevel];
                            if (dirs.indexOf(dir) === -1) {
                                dirs.push(dir);
                            }
                        }
                    }
                });

            // Check permissions
            client.mget(keys, (err, objs) => {
                const result = [];
                const dontCheck = options.user === 'system.user.admin' || options.group === 'system.group.administrator';

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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
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
            that.getObject(oldDataID, (err, data) => {
                that.setObject(newMetaID, meta, err => that.setObject(newDataID, data, callback));
            });
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
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
            that.setObject(metaID, meta, callback);
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
        checkFileRights(id, null, options, utils.consts.ACCESS_WRITE, (err, options, meta) => {
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
            client.keys(redisNamespace + dirID + '/*', (err, keys) => {
                keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                // Check permissions
                client.mget(keys, (err, objs) => {
                    if (options.user !== 'system.user.admin' && options.group !== 'system.group.administrator') {
                        const result = [];
                        for (let i = 0; i < keys.length; i++) {
                            objs[i] = JSON.parse(objs[i]);
                            if (utils.checkObject(objs[i], options, utils.consts.ACCESS_READ)) {
                                result.push(keys[i]);
                            }
                        }
                        _rmHelper(result, callback);
                    } else {
                        _rmHelper(keys, callback);
                    }
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
                } else {
                    return _rm(id, name, options, callback, meta);
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
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
            client.keys(redisNamespace + dirID + '/*', (err, keys) => {
                keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                // Check permissions
                client.mget(keys, (err, metas) => {
                    const dontCheck = options.user === 'system.user.admin' && options.group === 'system.group.administrator';
                    const keysFiltered = [];
                    const objsFiltered = [];
                    const processed = [];
                    const start = redisNamespace.length + dirID.length + 1;
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
            log.error('user is not defined');
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

        checkFileRights(id, null, options, utils.consts.ACCESS_WRITE, function (err, options, meta) {
            if (err) {
                if (typeof callback === 'function') callback(err);
            } else {
                if (!options.acl.file.write) {
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
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
            client.keys(redisNamespace + dirID + '/*', (err, keys) => {
                keys
                    .sort()
                    .filter(key => key.match(/\$%\$meta$/));

                // Check permissions
                client.mget(keys, (err, objs) => {
                    const dontCheck = options.user === 'system.user.admin' && options.group === 'system.group.administrator';
                    const keysFiltered = [];
                    const objsFiltered = [];
                    const processed = [];
                    const start = redisNamespace.length + dirID.length + 1;
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
            log.error('mode is not defined');
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
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
        log.silly(settings.namespace + ' redis psubscribe ' + redisNamespace + pattern);
        sub.psubscribe(redisNamespace + pattern, err =>
            (typeof callback === 'function') && callback(err));
    }
    this.subscribeConfig = function (pattern, options, callback) {
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
        log.silly(settings.namespace + ' redis punsubscribe ' + redisNamespace + pattern);
        sub.punsubscribe(redisNamespace + pattern, err =>
            (typeof callback === 'function') && callback(err));
    }
    this.unsubscribeConfig = function (pattern, options, callback) {
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
            client.set(id, JSON.stringify(obj), err => {
                client.publish(id, null);
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
                } else {
                    return _chmodObject(pattern, options, callback);
                }
            }
        });
    };

    function _getObject(id, options, callback) {
        client.get(redisNamespace + id, (err, obj) => {
            if (err) {
                log.warn(settings.namespace + ' redis get ' + id + ', error - ' + err);
            } else {
                log.silly(settings.namespace + ' redis get ' + id + ' ok: ' + obj);
            }
            try {
                obj = obj ? JSON.parse(obj) : null;
            } catch (e) {
                log.error(`Cannot parse ${id} - ${obj}: ${JSON.stringify(e)}`);
            }
            if (obj) {
                // Check permissions
                if (utils.checkObject(obj, options, utils.consts.ACCESS_READ)) {
                    callback(null, obj);
                } else {
                    callback(utils.consts.PERMISSION_ERROR);
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

    function _getKeys(pattern, options, callback, dontModify) {
        let r = new RegExp(tools.pattern2RegEx(pattern));
        client.keys(redisNamespace + pattern, (err, keys) => {
            log.silly(settings.namespace + ' redis keys ' + keys.length + ' ' + pattern);
            let result = [];
            if (keys) {
                keys.sort();
                const result = [];
                const dontCheck = options.user === 'system.user.admin' || options.group === 'system.group.administrator';

                if (dontCheck) {
                    for (let i = 0; i < keys.length; i++) {
                        if (r.test(keys[i]) && !fileRegEx.test(keys[i])) {
                            if (!dontModify) {
                                result.push(keys[i].substring(redisNamespaceL));
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
                            if (r.test(keys[i]) && !fileRegEx.test(keys[i]) && utils.checkObject(metas[i], options, utils.consts.ACCESS_READ)) {
                                if (!dontModify) {
                                    result.push(keys[i].substring(redisNamespaceL));
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
                _keys[i] = redisNamespace + keys[i];
            }
        } else {
            _keys = keys;
        }
        client.mget(_keys, (err, objs) => {
            let result = [];
            if (err) {
                log.warn(settings.namespace + ' redis mget ' + (!objs ? 0 :  objs.length) + ' ' + _keys.length + ', err: ' + err);
            } else {
                log.silly(settings.namespace + ' redis mget ' + (!objs ? 0 : objs.length) + ' ' + _keys.length);
            }
            if (objs) {
                if (options.user !== 'system.user.admin' && options.group !== 'system.group.administrator') {
                    for (let i = 0; i < objs.length; i++) {
                        objs[i] = JSON.parse(objs[i]);
                        if (utils.checkObject(objs[i], options, utils.consts.ACCESS_READ)) {
                            result.push(objs[i]);
                        } else {
                            result.push({error: utils.consts.PERMISSION_ERROR});
                        }
                    }
                } else {
                    result = objs;
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
        client.keys(redisNamespace + pattern, (err, keys) => {
            log.silly(settings.namespace + ' redis keys ' + keys.length + ' ' + pattern);
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
        client.get(redisNamespace + id, (err, oldObj) => {
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
                            client.set(redisNamespace + id, JSON.stringify(obj), err => {
                                client.publish(redisNamespace + id, null);
                                typeof callback === 'function' && callback(err);
                            });
                        });
                    }
                }
            }
            if (defaultNewAcl && obj.acl && !obj.acl.ownerGroup && options.ownerGroup) {
                obj.acl.ownerGroup = options.ownerGroup;
            }
            client.set(redisNamespace + id, JSON.stringify(obj), err => {
                client.publish(redisNamespace + id, null);
                typeof callback === 'function' && callback(err);
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
        if (options && options.acl) options.acl = null;

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
        client.get(redisNamespace + id, (err, oldObj) => {
            if (err) {
                log.warn(settings.namespace + ' redis get ' + id + ', error - ' + err);
            } else {
                log.silly(settings.namespace + ' redis get ' + id + ' ok: ' + oldObj);
            }
            try {
                oldObj = oldObj ? JSON.parse(oldObj) : null;
            } catch (e) {
                log.error(`Cannot parse ${id} - ${oldObj}: ${JSON.stringify(e)}`);
            }

            if (!utils.checkObject(oldObj, options, utils.consts.ACCESS_DELETE)) {
                typeof callback === 'function' && callback({error: utils.consts.PERMISSION_ERROR});
            } else {
                client.del(redisNamespace + id, function (err) {
                    client.publish(redisNamespace + id, null);
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

        if (options && options.acl) options.acl = null;
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

        function _emit_(id, obj) {
            result.rows.push({id: id, value: obj});
        }
        params = params || {startkey: '', endkey: '\u9999'};
        client.keys(redisNamespace + '*', (err, keys) => {
            keys = keys.sort().filter(key =>
                !key || utils.regCheckId.test(key) || fileRegEx.test(key));

            client.mget(keys, (err, objs) => {
                let f = eval('(' + func.map.replace(/emit/g, '_emit_') + ')');

                for (let i = 0; i < keys.length; i++) {
                    const id = keys[i];
                    if (params) {
                        if (params.startkey && id < params.startkey) continue;
                        if (params.endkey   && id > params.endkey)   continue;
                    }

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
        client.get(redisNamespace + '_design/' + design, (err, obj) => {
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
        params = params || {startkey: '', endkey: '\u9999'};
        let pattern = (params.endkey.substring(0, params.startkey.length) === params.startkey) ? redisNamespace + params.startkey + '*' : redisNamespace + '*';
        client.keys(pattern, (err, keys) => {
            let _keys = [];
            for (let i = 0; i < keys.length; i++) {
                if (params.startkey && keys[i] < params.startkey) continue;
                if (params.endkey && keys[i] > params.endkey) continue;
                if (!params.include_docs && keys[i][redisNamespaceL] === '_') continue;
                if (!keys[i] || utils.regCheckId.test(keys[i]) || keys[i].match(/\|file\$%\$/)) continue;
                _keys.push(keys[i]);
            }
            keys.sort();
            client.mget(_keys, (err, objs) => {
                // return rows with id and doc
                let result = {
                    rows: []
                };
                for (let r = 0; r < objs.length; r++) {
                    objs[r] = JSON.parse(objs[r]);
                    if (!utils.checkObject(objs[r], options, utils.consts.ACCESS_READ)) continue;
                    result.rows.push({id: keys[r], value: objs[r], doc: objs[r]});
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
            client.get(redisNamespace + id, (err, oldObj) => {
                oldObj = oldObj && JSON.parse(oldObj);
                if (!utils.checkObject(oldObj, options, utils.consts.ACCESS_WRITE)) {
                    return typeof callback === 'function' && callback(utils.consts.PERMISSION_ERROR);
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

                client.set(redisNamespace + id, oldObj, err => {
                    if (!err) {
                        client.publish(redisNamespace + id, null);
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
                            const id = keys[i];
                            objs[i] = JSON.parse(objs[i]);
                            if (objs[i].common &&
                                objs[i].common.name === idOrName &&
                                (!type || (objs[i].common && objs[i].common.type === type))) {
                                if (typeof callback === 'function') callback(null, id, idOrName);
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
        that.keys(redisNamespace + '*', (err, keys) => {
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
                    if (typeof callback === 'function') callback(utils.consts.PERMISSION_ERROR);
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
        if (sub) {
            sub.quit();
            sub = null;
        }
    };

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
                log.debug(settings.namespace + ' redis pmessage ', pattern, channel, message);

                try {
                    if (ioRegExp.test(channel)) {
                        const id = channel.substring(redisNamespaceL);
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
                            log.error(`Cannot parse ${id} - ${message}: ${JSON.stringify(e)}`);
                        }
                    } else {
                        log.error(`Received unexpected pmessage: ${channel}`);
                    }
                } catch (e) {
                    log.error(settings.namespace + ' pmessage ' + channel + ' ' + message + ' ' + e.message);
                    log.error(settings.namespace + ' ' + e.stack);
                }
            });
        }

        client.on('error', error => {
            if (typeof settings.disconnected === 'function') {
                settings.disconnected(error);
            } else {
                log.error(settings.namespace + ' ' + error.message);
                log.error(settings.namespace + ' ' + error.stack);
            }
        });

        sub.on('error', error => {
            log.error(settings.namespace + ' No redis connection!');
        });

        sub.on('connect', error => {
            if (settings.connection.port === 0) {
                log.info(settings.namespace + ' Objects connected to redis: ' + settings.connection.host);
            } else {
                log.info(settings.namespace + ' Objects connected to redis: ' + settings.connection.host + ':' + settings.connection.port);
            }
            // subscribe on system.config only if js-controller
            if (settings.controller) {
                sub.psubscribe(redisNamespace + 'system.config');
            }
        });

        client.on('connect', error => {
            typeof settings.connected === 'function' && settings.connected();

            // init default new acl
            client.get(redisNamespace + 'system.config', (err, obj) => {
                if (obj) {
                    obj = JSON.parse(obj);
                    if (obj.common && obj.common.defaultNewAcl) {
                        defaultNewAcl = obj.common.defaultNewAcl;
                    }
                }
            });
        });
    })();
    return this;
}

module.exports = ObjectsInRedis;