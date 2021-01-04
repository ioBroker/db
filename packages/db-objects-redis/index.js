module.exports = {
    Client: require('./lib/objects/objectsInRedisClient.js'),
    Server: null,
    objectsUtils: require('./lib/objects/objectsUtils.js'),
    getDefaultObjectsPort: host => {
        return (host.includes(',')) ? 26379 : 6379;
    }
};
