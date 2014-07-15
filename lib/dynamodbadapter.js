var dyno = require('dyno')({
    awsKey: 'fake',
    awsSecret: 'fake',
    table: 'geo',
    endpoint: 'http://localhost:4567'
});

var geoTable = require('./table.json');

module.exports = function(cb) {
    dyno.createTable(geoTable, function(err) {
        if (err) throw new Error(err);
        return cb({
            rangeQuery: rangeQuery,
            putItem: dyno.putItem,
            getAll: getAll,
            dyno: dyno
        });
    });

    function rangeQuery(idx, cb) {
        var params = {
            TableName: 'geo',
            AttributesToGet: ['val'],
            KeyConditions: {
                id: {
                    ComparisonOperator: 'BETWEEN',
                    AttributeValueList: [{
                        S: 'cell!' + idx[0]
                    }, {
                        S: 'cell!' + idx[1]
                    }]
                },
                layer: {
                    ComparisonOperator: 'EQ',
                    AttributeValueList: [{
                        S: 'default'
                    }]
                }
            }
        };
        dyno.query(params, function(err, res) {
            if (err) return cb(err);
            cb(null, res.Items);
        });
    }

    // http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#scan-property
    function getAll(callback) {
        dyno.scan({
            TableName: 'geo',
            AttributesToGet: ['val']
        }, function(err, res) {
            if (err) return callback(err);
            callback(null, res.Items);
        });
    }
};
