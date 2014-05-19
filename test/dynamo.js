var test = require('tap').test,
    dynamo = require('../lib/dynamodb');

function setup(cb) {
    var dynalite = require('dynalite')({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, function() {
        dynamo.createTable(dynamo.client, function() {
            cb(function(end) {
                dynalite.close(end);
            });
        });
    });
}

test('dynamo', function(t) {
    setup(function(close) {
        var params = { tableName: 'cb' };

        var doc = {
            id: 'testing',
            period: 1234,
            requests: 5
        };

        dynamo.client.putItem(doc, params, function(err) {
            close(function() {
                t.end();
            });
        });
    });
});
