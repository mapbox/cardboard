var dynamodb = require('./dynamodb'),
    through = require('through2'),
    Readable = require('readable-stream');

var TABLE_NAME = 'geo';

function getWriteStream(client) {
    var Writable = require('stream').Writable;
    var ws = Writable();
    ws._write = function(chunk, enc, next) {
        dynamodb.putItem(client, chunk, TABLE_NAME, next);
    };
    return ws;
}

module.exports = function(cb) {
    dynamodb.createTable(dynamodb.client, function(err) {
        if (err) throw new Error(err);
        return cb({
            rangeQuery: rangeQuery,
            createWriteStream: createWriteStream,
            getAll: getAll,
            dynamodb: dynamodb
        });
    });

    function rangeQuery(idx, cb) {
        throw new Error('unimplemented');
    }

    function createWriteStream() {
        return getWriteStream(dynamodb.client);
    }

    // http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#scan-property
    function getAll(callback) {
        dynamodb.client.scan({
            TableName: 'geo',
            AttributesToGet: ['val']
        }, function(err, res) {
            if (err) return callback(err);
            callback(null, res.Items);
        });
    }
};
