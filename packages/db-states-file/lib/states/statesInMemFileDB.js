/**
 *      States DB in memory - Server
 *
 *      Copyright 2013-2018 bluefox <dogafox@gmail.com>
 *
 *      MIT License
 *
 */

/** @module StatesInMemoryFileDB */

/* jshint -W097 */
/* jshint strict:false */
/* jslint node: true */
'use strict';

const InMemoryFileDB        = require('@iobroker/db-base').inMemoryFileDB;
const tools                 = require('@iobroker/db-base').tools;

// settings = {
//    change:    function (id, state) {},
//    connected: function (nameOfServer) {},
//    logger: {
//           silly: function (msg) {},
//           debug: function (msg) {},
//           info:  function (msg) {},
//           warn:  function (msg) {},
//           error: function (msg) {}
//    },
//    connection: {
//           dataDir: 'relative path'
//    },
//    auth: null, //unused
//    secure: true/false,
//    certificates: as required by createServer
//    port: 9000,
//    host: localhost
// };
//

/**
 * This class inherits InMemoryFileDB class and adds all relevant logic for states
 * including the available methods for use by js-controller directly
 **/
class StatesInMemoryFileDB extends InMemoryFileDB {

    constructor(settings) {
        settings = settings || {};
        settings.fileDB = {
            fileName: 'states.json',
            backupDirName: 'backup-objects'
        };
        super(settings);

        this.logs = {};
        this.session = {};
        this.globalMessageId = Math.round(Math.random() * 100000000);
        this.globalLogId = Math.round(Math.random() * 100000000);

        this.stateExpires = {};
        this.sessionExpires = {};
        this.ONE_DAY_IN_SECS = 24*60*60*1000;
        this.writeFileInterval = this.settings.connection && typeof this.settings.connection.writeFileInterval === 'number' ?
            parseInt(this.settings.connection.writeFileInterval) : 30000;
        this.log.silly(`${this.namespace} States DB uses file write interval of ${this.writeFileInterval} ms`);

        //this.settings.connection.maxQueue = this.settings.connection.maxQueue || 1000;

        // Reset expires, that are still in DB
        this.expireAll();
    }

    // internal functionality
    expireAll() {
        Object.keys(this.stateExpires).forEach( id => {
            clearTimeout(this.stateExpires[id]);
            this.expireState(id);
        });
        // Set as expire all states that could expire
        Object.keys(this.dataset).forEach(id => {
            if (this.dataset[id] === undefined) {
                return;
            }
            if (this.dataset[id].expire) {
                this.expireState(id, true);
            }
        });

        if (!this.stateTimer) {
            this.stateTimer = setTimeout(() => this.saveState(), this.writeFileInterval);
        }
    }

    // internal functionality
    expireState(id, dontPublish) {
        if (this.stateExpires[id] !== undefined) {
            delete this.stateExpires[id];
        }

        if (this.dataset[id] !== undefined) {
            delete this.dataset[id];
            !dontPublish && setImmediate(() => this.publishAll('state', id, null));
        }

        if (!this.stateTimer) {
            this.stateTimer = setTimeout(() => this.saveState(), this.writeFileInterval);
        }
    }

    // internal functionality
    expireSession(id) {
        if (this.sessionExpires[id] && this.sessionExpires[id].timeout) {
            clearTimeout(this.sessionExpires[id].timeout);
            delete this.sessionExpires[id];
        }

        if (this.session[id] !== undefined) {
            delete this.session[id];
        }
    }

    // Destructor of the class. Called by shutting down.
    // internal functionality
    destroy() {
        this.expireAll();

        super.destroy();

        if (this.stateTimer) {
            clearTimeout(this.stateTimer);
            this.stateTimer = null;
        }
    }

    // needed by Server
    getStates(keys, callback, _dontModify) {
        if (!keys) {
            typeof callback === 'function' && setImmediate(() => callback('no keys', null));
            return;
        }
        if (!keys.length) {
            typeof callback === 'function' && setImmediate(() => callback(null, []));
            return;
        }
        const result = [];
        for (let i = 0; i < keys.length; i++) {
            result.push(this.dataset[keys[i]] !== undefined ? this.dataset[keys[i]] : null);
        }
        typeof callback === 'function' && setImmediate(() => callback(null, result));
    }

    // needed by Server
    getState(id, callback) {
        typeof callback === 'function' && setImmediate(state => callback(null, state), this.dataset[id] !== undefined ? this.dataset[id] : null);
    }

    // needed by Server
    _setStateDirect(id, obj, expire, callback) {
        if (typeof expire === 'function') {
            callback = expire;
            expire = undefined;
        }

        if (this.stateExpires[id]) {
            clearTimeout(this.stateExpires[id]);
            delete this.stateExpires[id];
        }

        if (expire) {
            this.stateExpires[id] = setTimeout(() => this.expireState(id), expire * 1000);

            obj.expire = true;
        }
        this.dataset[id] = obj;
        typeof callback === 'function' && setImmediate(() => callback(null, id));

        // If val === undefined, the state was just created and not filled with value
        if (obj.val !== undefined) {
            setImmediate(() => {
            // publish event in states
                this.log.silly(`${this.namespace} memory publish ${id} ${JSON.stringify(obj)}`);
                this.publishAll('state', id, obj);
            });
        }

        if (!this.stateTimer) {
            this.stateTimer = setTimeout(() => this.saveState(), this.writeFileInterval);
        }
    }

    // needed by Server
    delState(id, callback) {
        if (this.stateExpires[id]) {
            clearTimeout(this.stateExpires[id]);
            delete this.stateExpires[id];
        }

        if (this.dataset[id]) {
            const isBinary = Buffer.isBuffer(this.dataset[id]);
            delete this.dataset[id];

            typeof callback === 'function' && setImmediate(callback, null, id);

            !isBinary && setImmediate(() => this.publishAll('state', id, null));
        } else {
            typeof callback === 'function' && setImmediate(callback, null, id);
        }

        if (!this.stateTimer) {
            this.stateTimer = setTimeout(() => this.saveState(), this.writeFileInterval);
        }
    }

    // needed by Server
    getKeys(pattern, callback, _dontModify) {
        // special case because of simulation of redis
        if (pattern.substring(0, 3) === 'io.') {
            pattern = pattern.substring(3);
        }

        const r = new RegExp(tools.pattern2RegEx(pattern));
        const result = [];
        for (const id of Object.keys(this.dataset)) {
            r.test(id) && result.push(id);
        }
        typeof callback === 'function' && setImmediate(() => callback(null, result));
    }

    // needed by Server
    subscribeForClient(client, pattern, cb) {
        this.handleSubscribe(client, 'state', pattern, cb);
    }

    // needed by Server
    unsubscribeForClient(client, pattern, cb) {
        this.handleUnsubscribe(client, 'state', pattern, cb);
    }

    // needed by Server
    subscribeMessageForClient(client, id, cb) {
        this.handleSubscribe(client, 'messagebox', 'messagebox.' + id, cb);
    }

    // needed by Server
    unsubscribeMessageForClient(client, id, cb) {
        this.handleUnsubscribe(client, 'messagebox', 'messagebox.' + id, cb);
    }

    // needed by Server
    subscribeLogForClient(client, id, cb) {
        this.handleSubscribe(client, 'log', 'log.' + id, cb);
    }

    // needed by Server
    unsubscribeLogForClient(client, id, cb) {
        this.handleUnsubscribe(client, 'log', 'log.' + id, cb);
    }

    // needed by Server
    getSession(id, callback) {
        typeof callback === 'function' && setImmediate(session => callback(session), this.session[id]);
    }

    // internal functionality
    handleSessionExpire(id, expireDate) {
        if (this.sessionExpires[id] && this.sessionExpires[id].timeout) {
            clearTimeout(this.sessionExpires[id].timeout);
            delete this.sessionExpires[id];
        }
        const expireDelay = expireDate - Date.now();
        if (expireDelay <= 0) {
            this.expireSession(id);
        } else if (expireDate <= this.ONE_DAY_IN_SECS) {
            this.sessionExpires[id] = {
                sessionEnd: expireDate,
                timeout: setTimeout(() => {
                    this.sessionExpires[id].timeout = null;
                    this.expireSession(id);
                }, expireDate)
            };
        } else {
            this.sessionExpires[id] = {
                sessionEnd: expireDate,
                timeout: setTimeout(() => {
                    this.sessionExpires[id].timeout = null;
                    this.handleSessionExpire(id, expireDate);
                }, this.ONE_DAY_IN_SECS)
            };
        }
    }

    // needed by Server
    setSession(id, expire, obj, callback) {
        this.session[id] = obj || {};

        if (this.sessionExpires[id] && this.sessionExpires[id].timeout) {
            clearTimeout(this.sessionExpires[id].timeout);
            delete this.sessionExpires[id];
        }

        this.handleSessionExpire(id, Date.now() + expire * 1000);
        this.session[id]._expire = true;

        typeof callback === 'function' && setImmediate(() => callback());
    }

    // needed by Server
    destroySession(id, callback) {
        if (this.session[id]) {
            delete this.session[id];
        }
        typeof callback === 'function' && setImmediate(() => callback());
    }

    // needed by Server
    setBinaryState(id, data, callback) {
        if (!Buffer.isBuffer(data)) {
            data = Buffer.from(data);
        }
        this.dataset[id] = data;
        typeof callback === 'function' && setImmediate(() => callback(null, id));

        if (!this.stateTimer) {
            this.stateTimer = setTimeout(() => this.saveState(), this.writeFileInterval);
        }
    }
}

module.exports = StatesInMemoryFileDB;
