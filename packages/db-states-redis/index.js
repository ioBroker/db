module.exports = {
    Client: require('./lib/states/statesInRedisClient.js'),
    Server: null,
    getDefaultObjectsPort: host => {
        return (host.includes(',')) ? 26379 : 6379;
    }
};
