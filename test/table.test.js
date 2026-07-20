var test = require('tape');
var queue = require('queue-async');
var _ = require('lodash');
var dynalite = require('dynalite')({
    createTableMs: 0,
    updateTableMs: 0,
    deleteTableMs: 0
});
var DynamoDBClient = require('@aws-sdk/client-dynamodb').DynamoDBClient;
var ListTablesCommand = require('@aws-sdk/client-dynamodb').ListTablesCommand;
var DeleteTableCommand = require('@aws-sdk/client-dynamodb').DeleteTableCommand;

var config = {
    region: 'fake',
    endpoint: 'http://localhost:4567'
};

var client = new DynamoDBClient({
    region: 'fake',
    endpoint: 'http://localhost:4567',
    credentials: { accessKeyId: 'fake', secretAccessKey: 'fake' }
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
        client.send(new ListTablesCommand({})).then(function(tables) {
            var q = queue();
            tables.TableNames.forEach(function(name) {
                q.defer(function(done) {
                    client.send(new DeleteTableCommand({ TableName: name })).then(function() { done(); }, done);
                });
            });

            q.awaitAll(function(err) {
                if (err) return assert.end(err);
                dynalite.close(function(err) { assert.end(err); });
            });
        }, function(err) {
            assert.ifError(err, 'found tables');
            assert.end(err);
        });
    });
}

before();

test('[tables] createTable - match config name', function(assert) {
    var cardboard = require('..')(_.extend({ mainTable: 'features' }, config));
    cardboard.createTable(function(err) {
        assert.ifError(err, 'success');

        client.send(new ListTablesCommand({})).then(function(tables) {
            assert.deepEqual(tables.TableNames, ['features'], 'created table');
            assert.end();
        }, function(err) { throw err; });
    });
});

after();
before();

test('[tables] createTable - match config name, with difference names', function(assert) {
    var cardboard = require('..')(_.extend({ mainTable: 'first' }, config));
    cardboard.createTable(function(err) {
        assert.ifError(err, 'success');

        client.send(new ListTablesCommand({})).then(function(tables) {
            assert.deepEqual(tables.TableNames, ['first'], 'created table');
            assert.end();
        }, function(err) { throw err; });
    });
});

after();


