module.exports = {
    Client: require('@iobroker/db-objects-redis').Client,
    Server: require('./lib/objects/objectsInMemServerRedis.js')
};
