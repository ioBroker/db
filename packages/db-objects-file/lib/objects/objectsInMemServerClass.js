/**
 *      States DB in memory - Server with Redis protocol
 *
 *      Copyright 2013-2020 bluefox <dogafox@gmail.com>
 *
 *      MIT License
 *
 */

/** @module statesInMemory */

/* jshint -W097 */
/* jshint strict:false */
/* jslint node: true */
'use strict';

const ObjectsInRedisClient = require('@iobroker/db-objects-redis').Client;
const ObjectsInMemServer = require('./objectsInMemServerRedis');

class ObjectsInMemoryServerClass extends ObjectsInRedisClient {

    constructor(settings) {
        settings.autoConnect = false; // delay Client connection to when we need it
        super(settings);

        const serverSettings = {
            namespace: settings.namespace + '-Server',
            connection: settings.connection,
            logger: settings.logger,
            hostname: settings.hostname,
            connected: () => {
                this.connectDb(); // now that server is connected also connect client
            }
        };
        this.objectsServer = new ObjectsInMemServer(serverSettings);
    }

    async destroy() {
        await super.destroy(); // destroy client first
        this.objectsServer.destroy(); // server afterwards too
    }

    getStatus() {
        return this.objectsServer.getStatus(); // return Status as Server
    }

    destroyDB(options, callback) {
        return this.objectsServer.destroyDB(options, callback);
    }

    syncFileDirectory(limitId, callback) {
        return this.objectsServer.syncFileDirectory(limitId, callback);
    }

    async dirExists(id, name, options) {
        return this.objectsServer.dirExists(id, name, options);
    }

    // Try to increase performance by directly calling server code for real logic
    async _setObject(id, obj, options, callback) {
        return this.objectsServer._setObjectDirect(id, obj, callback);
    }

    async _getObject(id, options, callback) {
        return this.objectsServer._getObject(id, options, callback);
    }

}

module.exports = ObjectsInMemoryServerClass;
