var test = require('tape');
var queue = require('queue-async');
var _ = require('lodash');
var dynalite = require('dynalite')({
    createTableMs: 0,
    updateTableMs: 0,
    deleteTableMs: 0
});

var config = {
    region: 'fake',
    endpoint: 'http://localhost:4567'
};

var dyno = require('dyno')({
    table: 'fake',
    region: 'fake',
    endpoint: 'http://localhost:4567'
});

function before() {
    test('listening to dynalite', function(assert) {
        dynalite.listen(4567, function(err) {
            assert.ifError(err, 'listening to dynalite');
            assert.end();
        });
    });
}

function after() {
    test('tearing down tables', function(assert) {
        dyno.listTables(function(err, tables) {
            assert.ifError(err, 'found tables');
            if (err) return assert.end(err);
            var q = queue();
            tables.TableNames.forEach(function(name) {
                q.defer(function(done) {
                    dyno.deleteTable({TableName: name}, done);  
                });
            });

            q.awaitAll(function(err) {
                if (err) return assert.end(err);
                dynalite.close(function(err) { assert.end(err); });
            });
        });
    });
}

before();

test('[tables] createTable - match config name', function(assert) {
    var cardboard = require('..')(_.extend({ mainTable: 'features' }, config));
    cardboard.createTable(function(err) {
        assert.ifError(err, 'success');
  
        dyno.listTables(function(err, tables) {
            if (err) throw err;
            assert.deepEqual(tables.TableNames, ['features'], 'created table');
            assert.end();
        });
    });
});

after();
before();

test('[tables] createTable - match config name, with difference names', function(assert) {
    var cardboard = require('..')(_.extend({ mainTable: 'first' }, config));
    cardboard.createTable(function(err) {
        assert.ifError(err, 'success');
  
        dyno.listTables(function(err, tables) {
            if (err) throw err;
            assert.deepEqual(tables.TableNames, ['first'], 'created table');
            assert.end();
        });
    });
});

after();


