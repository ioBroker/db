module.exports = {
    Client: require('@iobroker/db-states-redis').Client,
    Server: require('./lib/states/statesInMemServerRedis.js'),
    getDefaultObjectsPort: (_host) => {
        return 9000;
    }
};
