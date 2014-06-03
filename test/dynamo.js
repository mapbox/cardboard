var test = require('tap').test,
    dynamo = require('../lib/dynamodb'),
    Dynalite = require('dynalite');

var dynalite;

function setup(t) {
    dynalite = Dynalite({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, function() {
        t.end();
    });
}

function teardown(t) {
    dynalite.close();
    t.end();
}

test('setup', setup);
test('dynamo.createTable', function(t) {
    dynamo.createTable(dynamo.client, function(err) {
        t.notOk(err, 'no error');
        t.pass('table created');
        t.end();
    });
});

test('dynamo.deleteTable', function(t) {
    dynamo.createTable(dynamo.client, function(err) {
        t.notOk(err, 'no error');
        t.pass('setup works');
        dynamo.deleteTable(dynamo.client, function(err) {
            t.notOk(err, 'no error');
            t.pass('deleteTable works');
            t.end();
        });
    });
});
test('teardown', teardown);

test('setup', setup);
test('dynamo.putItem', function(t) {
    dynamo.createTable(dynamo.client, created);
    function created(err) {
        t.notOk(err, 'no error');
        inserted();
        dynamo.putItem(dynamo.client,
            { id: 'a1', layer: 'b' },
            inserted);
    }
    function inserted(err) {
        t.notOk(err, 'no error');
        t.pass('putItem');
        t.end();
    }
});
test('teardown', teardown);

test('setup', setup);
test('dynamo.getItem', function(t) {
    var item = { id: 'a2', layer: 'thisisalayername' };
    dynamo.createTable(dynamo.client, created);

    function created(err) {
        t.notOk(err, 'no error');
        dynamo.putItem(dynamo.client,
            item,
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
        t.end();
    }
});
test('teardown', teardown);
