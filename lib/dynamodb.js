var AWS = require('aws-sdk');

AWS.config.update({
    accessKeyId: 'fake',
    accessSecretKey: 'fake',
    endpoint:new AWS.Endpoint('http://localhost:4567'),
    region:'us-east-1'
});

var dynamo = new AWS.DynamoDB();

var geoTable = {
    'TableName': 'geo',
    'AttributeDefinitions': [
        {'AttributeName': 'id', 'AttributeType': 'S'},
        {'AttributeName': 'data', 'AttributeType': 'S'}
    ],
    'KeySchema': [
        {'AttributeName': 'id', 'KeyType': 'RANGE'}
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 50
    },
    'GlobalSecondaryIndexes': [
        {
            'IndexName': 'geo-index',
            'KeySchema': [
                { 'AttributeName': 'id', 'KeyType': 'RANGE' }
            ],
            'Projection': { 'ProjectionType': 'KEYS_ONLY' },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 50
            }
        }
    ]
};

module.exports.createTable = function(client, cb) {
    client.createTable(geoTable, function (err) {
        console.log(arguments);
        if (err) return cb(err);
        cb(null);
    });
};

module.exports.client = dynamo;
