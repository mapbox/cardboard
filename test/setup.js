var Dynalite = require('dynalite');
var Cardboard = require('../');
var fakeAWS = require('mock-aws-s3');
var queue = require('queue-async');
var dynalite;

var config = module.exports.config = {
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    featureTable: 'features',
    searchTable: 'search',
    endpoint: 'http://localhost:4567',
    bucket: 'test',
    prefix: 'test',
    region: 'us-east-1',
    s3: fakeAWS.S3() // only for mocking s3
};

var dynoConfig = {
    table: 'fake',
    region: 'us-east-1',
    endpoint: 'http://localhost:4567'
};

var dyno  = require('dyno')(dynoConfig);

module.exports.setup = function(done) {
    dynalite = Dynalite({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, function() {
        var cardboard = Cardboard(config);
        var q = queue(1);

        cardboard.createTable(done);
    });
};

module.exports.teardown = function(done) {
    dyno.listTables(function(err, tables) {
        var q = queue();
        tables.TableNames.forEach(function(table) {
            q.defer(dyno.deleteTable, { TableName: table });
        });

        q.awaitAll(function(err) {
            if (err) throw err;
            dynalite.close(function(err) {
                done(err);
            });
        });
    });
};
