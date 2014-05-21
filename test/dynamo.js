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
        dynamo.createTable(dynamo.client, created);
        function created(err) {
            t.notOk(err, 'no error');
            dynamo.putItem(dynamo.client,
                { id: 'a1', layer: 'b' }, 'geo',
                inserted);
        }
        function inserted(err) {
            t.notOk(err, 'no error');
            t.pass('putItem');
            dynalite.close();
            t.end();
        }
    });
});


test('dynamo.getItem', function(t) {
    var item = { id: 'a2', layer: 'thisisalayername' };
    setup(function(dynalite) {
        dynamo.createTable(dynamo.client, created);

        function created(err) {
            t.notOk(err, 'no error');
            dynamo.putItem(dynamo.client,
                item, 'geo',
                inserted);
        }
        function inserted(err) {
            t.notOk(err, 'no error');
            t.pass('putItem');
            dynamo.getItem(dynamo.client, item.layer, item.id, gotten);
        }
        function gotten(err, resp) {
            t.notOk(err, 'no error');
            t.deepEqual(resp, {
                Item: dynamo._convertToDynamoTypes(item)
            }, 'response');
            dynalite.close();
            t.end();
        }
    });
});
