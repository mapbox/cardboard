var Dynalite = require('dynalite');
var Cardboard = require('../');
var queue = require('queue-async');
var DynamoDBClient = require('@aws-sdk/client-dynamodb').DynamoDBClient;
var ListTablesCommand = require('@aws-sdk/client-dynamodb').ListTablesCommand;
var DeleteTableCommand = require('@aws-sdk/client-dynamodb').DeleteTableCommand;
var dynalite;

var config = module.exports.config = {
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    mainTable: 'features',
    endpoint: 'http://localhost:4567',
    region: 'us-east-1'
};

var client = new DynamoDBClient({
    region: 'us-east-1',
    endpoint: 'http://localhost:4567',
    credentials: { accessKeyId: 'fake', secretAccessKey: 'fake' }
});

module.exports.setup = function(done) {
    dynalite = Dynalite({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, function() {
        var cardboard = Cardboard(config);

        cardboard.createTable(done);
    });
};

module.exports.teardown = function(done) {
    client.send(new ListTablesCommand({})).then(function(tables) {
        var q = queue();
        tables.TableNames.forEach(function(table) {
            q.defer(function(name, cb) {
                client.send(new DeleteTableCommand({ TableName: name })).then(function() { cb(); }, cb);
            }, table);
        });

        q.awaitAll(function(err) {
            if (err) throw err;
            dynalite.close(function(err) {
                done(err);
            });
        });
    }, done);
};
