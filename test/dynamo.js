var test = require('tap').test,
    dynamo = require('../lib/dynamodb');

function setup(cb) {
    var dynalite = require('dynalite')({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, cb.bind(cb, dynalite));
}

test('dynamo.createTable', function(t) {
    setup(function(dynalite) {
        dynamo.createTable(dynamo.client, function(err) {
            t.notOk(err, 'no error');
            t.pass('setup works');
            dynalite.close();
            t.end();
        });
    });
});

test('dynamo.deleteTable', function(t) {
    setup(function(dynalite) {
        dynamo.createTable(dynamo.client, function(err) {
            t.notOk(err, 'no error');
            t.pass('setup works');
            dynamo.deleteTable(dynamo.client, function(err) {
                t.notOk(err, 'no error');
                t.pass('delete works');
                dynalite.close();
                t.end();
            });
        });
    });
});

test('dynamo.putItem', function(t) {
    setup(function(dynalite) {
        dynamo.createTable(dynamo.client, function(err) {
            t.notOk(err, 'no error');
            dynamo.putItem(dynamo.client, { id: 'a1', layer: 'b' }, 'geo', function(err) {
                t.notOk(err, 'no error');
                t.pass('putItem');
                dynalite.close();
                t.end();
            });
        });
    });
});

