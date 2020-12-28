module.exports = {
    Client: require('./lib/states/statesInRedis.js'),
    Server: null,
    getDefaultObjectsPort: host => {
        return (host.includes(',')) ? 26379 : 6379;
    }
};
