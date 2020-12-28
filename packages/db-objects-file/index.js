module.exports = {
    Client: require('@iobroker/db-objects-redis').Client,
    Server: require('./lib/objects/objectsInMemServerRedis.js'),
    getDefaultObjectsPort: (_host) => {
        return 9001;
    }
};
