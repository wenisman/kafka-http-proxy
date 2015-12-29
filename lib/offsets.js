var kafka       = require('kafka-node');
var config      = require('../config.js');
var logger      = require('../logger.js').logger;

var client = new kafka.Client(config.kafka.zkConnect, config.kafka.clientId.http);
var offset = new kafka.Offset(client);

var commitOffsets = function(groupId, offsetMap, callback) {
    logger.debug({offsetMap: offsetMap}, 'committing offsets');

    if (offsetMap === []) {
        logger.trace('no offsets found, returning');
        return callback({
            name: 'Error',
            message: 'no offsets found to commit'
        }, null);
    }

    logger.trace('committing the offsets to kafka');
    return offset.commit(groupId, offsetMap, callback);
};

var fetchOffsets = function(payloads, callback) {
    logger.debug();

    payloads = payloads.map(function(payload){
        return {
            topic: payload.topic,
            partition: payload.partition,
            time: -1
        }
    });

    offset.fetch(payloads, callback);
};


module.exports = {
    commitOffsets : commitOffsets,
    fetchOffsets : fetchOffsets
};
