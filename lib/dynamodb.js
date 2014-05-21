var AWS = require('aws-sdk');

AWS.config.update({
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    region: 'us-east-1'
});

var dynamo = new AWS.DynamoDB({endpoint: new AWS.Endpoint('http://localhost:4567')});

module.exports.client = dynamo;

var geoTable = {
    'TableName': 'geo',
    'AttributeDefinitions': [
        {'AttributeName': 'layer', 'AttributeType': 'S'},
        {'AttributeName': 'id', 'AttributeType': 'S'}
    ],
    'KeySchema': [
        {'AttributeName': 'layer', 'KeyType': 'HASH'},
        {'AttributeName': 'id', 'KeyType': 'RANGE'}
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 50
    }
};

module.exports.createTable = function(client, cb) {
    createTable(client, geoTable, cb);
};

module.exports.deleteTable = function(client, cb) {
    deleteTable(client, geoTable, cb);
};

module.exports.putItem = function putItem(client, doc, tableName, cb) {
    client.putItem({
        TableName: tableName,
        Item: convertToDynamoTypes(doc)
    }, cb);
};

function convertToDynamoTypes(attrs) {
    var attrUpdates = {};
    for (var key in attrs) {
        var val = attrs[key];
        if (['id', 'layer'].indexOf(key) !== -1) {
            attrUpdates[key] = { S: val.toString() };
        } else {
            attrUpdates[key] = { N: val.toString() };
        }
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
