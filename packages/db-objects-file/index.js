module.exports = {
    Client: require('@iobroker/db-objects-redis').Client,
    Server: require('./lib/objects/objectsInMemServerClass.js'),
    getDefaultObjectsPort: _host => {
        return 9001;
    }
};
