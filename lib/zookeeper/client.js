var zookeeper   = require('node-zookeeper-client');
var config      = require('../../config');
var logger      = require('../../logger');

var zkClient    = zk.createClient(config.kafka.zkConnect);

function getConsumerPath(groupId) {
    return '/consumers/' + groupId;
}

function getTopicOffsetPath(groupId, topic) {
    return getConsumerPath + '/' + topic
}

function getPartitionOffsetPath(groupId, topic, partition) {
    return getTopicOffsetPath(groupId, topic) + '/' + partition;
}



function denodify (f, that) {
    return function () {
        var args = Array.prototype.slice.call(arguments);
        return new Promise(function (resolve, reject) {
            f.apply(that, args.concat(function (err, data, status) {
                if (err) return reject(err);
                return resolve({ data: data, status: status, args: args });
            }));
        });
    };
};

var getChildren = denodeify(zkClient.getChildren, zkClient);
var getData = denodeify(zkClient.getData, zkClient);
var remove = denodeify(zkClient.remove, zkClient);
var close = denodeify(zkClient.close, zkClient);


var getOffsets = function(consumer, payloads, callback) {
    logger.trace({payloads: payloads}, 'getting the offsets for the provided payloads');
    return payloads.map(function(payload) {
        getData(getPartitionOffsetPath(consumer, payload.topic, payload.partition)).then(function(result){
            payload.offset = result;
            return payload;
        });
    });
}; 



module.exports = {
    getOffsets: getOffsets
};