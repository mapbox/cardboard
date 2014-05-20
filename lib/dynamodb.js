var AWS = require('aws-sdk');

AWS.config.update({
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    region:'us-east-1'
});

var dynamo = new AWS.DynamoDB({endpoint: new AWS.Endpoint('http://localhost:4567')});

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
    client.createTable(geoTable, function (err) {
        console.log(arguments);
        if (err) return cb(err);
        cb(null);
    });
};

module.exports.client = dynamo;
