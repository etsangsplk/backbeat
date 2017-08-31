const zookeeper = require('node-zookeeper-client');

/**
 * wrapper around createClient() from node-zookeeper-client module,
 * with the following enhancements:
 *
 *  - emits a 'ready' event when the zookeeper client is ready
 *  - takes an additional option "autoCreateNamespace"
 *
 * @param {string} connectionString - connection string to zookeeper
 *   (e.g. 'localhost:2181/backbeat')
 * @param {object} [options] - an object to set the client
 *   options. Currently available options are:
 * @param {number} [options.sessionTimeout] - session timeout in
 *   milliseconds, defaults to 30 seconds.
 * @param {number} [options.spinDelay] - the delay (in milliseconds)
 *   between each connection attempts.
 * @param {number} [options.retries] - the number of retry attempts
 *   for connection loss exception.
 * @param {boolean} [options.autoCreateNamespace] - when true, ensure
 *   namespace (chroot) base path is created if not exists
 *
 * @return {node-zookeeper-client.Client} a zookeeper client handle
 */
function createClient(connectionString, options) {
    const zkClient = zookeeper.createClient(connectionString, options);
    zkClient.once('connected', () => {
        // for some reason zkClient.exists() does not return
        // NO_NODE when base path does not exist, hence use
        // getData() instead
        zkClient.getData('/', err => {
            if (!err) {
                return zkClient.emit('ready');
            }
            if (err.name === 'NO_NODE' &&
                options && options.autoCreateNamespace) {
                const nsIndex = connectionString.indexOf('/');
                const hostPort = connectionString.slice(0, nsIndex);
                const namespace = connectionString.slice(nsIndex);
                const rootZkClient = zookeeper.createClient(hostPort, options);
                rootZkClient.connect();
                return rootZkClient.mkdirp(namespace, err => {
                    if (!err || err.name === 'NODE_EXISTS') {
                        zkClient.emit('ready');
                    }
                });
            }
            return undefined;
        });
    });
    return zkClient;
}

module.exports = {
    createClient,
};
