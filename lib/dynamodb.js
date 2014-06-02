var AWS = require('aws-sdk'),
    geoTable = require('./table.json');

AWS.config.update({
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    region: 'us-east-1'
});

var dynamo = new AWS.DynamoDB({
    endpoint: new AWS.Endpoint('http://localhost:4567')
});

module.exports.client = dynamo;

module.exports.createTable = function(client, cb) {
    createTable(client, geoTable, cb);
};

module.exports.deleteTable = function(client, cb) {
    deleteTable(client, geoTable, cb);
};

module.exports.putItem = function putItem(client, doc, cb) {
    client.putItem({
        TableName: 'geo',
        Item: convertToDynamoTypes(doc)
    }, cb);
};

module.exports.getItem = function(client, layer, id, cb) {
    client.getItem({
        Key: {
            layer: { S: layer },
            id: { S: id }
        },
        TableName: 'geo',
        ConsistentRead: true
    }, onload);

    function onload(err, resp) {
        if (err) return cb(err);
        cb(err, resp);
    }
};

module.exports._convertToDynamoTypes = convertToDynamoTypes;

function convertToDynamoTypes(attrs) {
    var attrUpdates = {};
    for (var key in attrs) {
        var val = attrs[key];
        attrUpdates[key] = { S: val.toString() };
    }
    return attrUpdates;
}

function createTable(client, table, cb) {
    function check() {
        client.describeTable({TableName: table.TableName}, function (err, data) {
            if (err && err.code === 'ResourceNotFoundException') {
                client.createTable(table, function (err) {
                    if (err) return cb(err);
                    setTimeout(check, 0);
                });
            } else if (err) {
                cb(err);
            } else if (data.Table.TableStatus === 'ACTIVE') {
                cb();
            } else {
                setTimeout(check, 1000);
            }
        });
    }
    check();
}

function deleteTable(client, table, cb) {
    function check() {
        client.describeTable({TableName: table.TableName}, function (err, data) {
            if (err && err.code === 'ResourceNotFoundException') {
                cb();
            } else if (err) {
                cb(err);
            } else if (data.Table.TableStatus === 'ACTIVE') {
                client.deleteTable({TableName: table.TableName}, function (err) {
                    if (err) return cb(err);
                    setTimeout(check, 0);
                });
            } else {
                setTimeout(check, 1000);
            }
        });
    }
    check();
}
