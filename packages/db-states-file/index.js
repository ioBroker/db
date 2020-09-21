module.exports = {
    Client: require('@iobroker/db-states-redis').Client,
    Server: require('./lib/states/statesInMemServerRedis.js')
};
