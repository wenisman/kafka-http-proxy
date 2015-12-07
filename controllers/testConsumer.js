var request = require('request-promise'),
    Promise = require('promise'),
    now = Date.now(),
    baseUri = 'http://localhost:8085',
    topicUriSuffix = '/topics/test.' + now,
    topicUri = baseUri + topicUriSuffix,
    createConsumerUri = baseUri + '/consumers/test.' + now;

var consumerUri,
    consumerTopicUri;

request.put(topicUri)
    .then(function (r) {
        console.log('created topic ' + r);
        return request.post(createConsumerUri);
    })
    .then(function (r) {
        console.log('created consumer ' + r);
        consumerUri = JSON.parse(r).base_uri;
        consumerTopicUri = consumerUri + topicUriSuffix;
        return request.get(consumerTopicUri);
    })
    .then(function (r) {
        return new Promise(function (res) {
            setTimeout(function() {
                res(r)
            }, 1000);
        });
    })
    .then(function (r) {
        console.log('should not have received messages: ' + r);
        var options = {
            uri: topicUri,
            method: 'POST',
            json: {
                records: [{ key: '123', value: '456' }]
            },
            headers: {
                'Content-Type': 'application/vnd.kafka.v1+json'
            }
        };
        return request.post(options);
    })
    .then(function (r) {
        console.log('published messages: ' + JSON.stringify(r));
        return new Promise(function (res) {
            setTimeout(function() {
                res(r)
            }, 1000);
        });
    })
    .then(function (r) {
        var max = 10,
            i = 0;
        return new Promise(function (res, rej) {
            var poll = function () {
                console.log('get');
                request.get(consumerTopicUri)
                    .then(function (r) {
                        process.stdout.write('.');
                        var result = JSON.parse(r);
                        i++;
                        if (result.length > 0)
                            res(result);
                        else if (i == max)
                            rej('ohnoes');
                        else
                            setTimeout(poll, 1000);
                    })
                    .catch(function (e) {
                        rej(e);
                    });
            };
            setTimeout(poll, 1000);
        });
    })
    .then(function (r) {
        console.log('got messages: ' + JSON.stringify(r));
        return request.post(consumerUri + '/offsets');
    })
    .then(function (r) {
        console.log('committed offsets: ' + r);
    })
    .catch(function (e) {
        console.error(e);
    })
    .done(function () {
        if (consumerUri) {
            request.del(consumerUri);
        }
    });
