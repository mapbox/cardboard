var test = require('tape');
var Dynalite = require('dynalite');
var Cardboard = require('../');
var fakeAWS = require('mock-aws-s3');
var queue = require('queue-async');
var dynalite;

var config = module.exports.config = {
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    table: 'geo',
    endpoint: 'http://localhost:4567',
    bucket: 'test',
    prefix: 'test',
    region: 'us-east-1',
    s3: fakeAWS.S3() // only for mocking s3
};

var dyno = module.exports.dyno = require('dyno')(config);

module.exports.setup = function(t, multi) {
    dynalite = Dynalite({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, function() {
        t.pass('dynalite listening');
        var cardboard = Cardboard(config);
        var q = queue(1);

        q.defer(cardboard.createTable, config.table);
        if (multi) {
            q.defer(cardboard.createTable, 'test-cardboard-read');
            q.defer(cardboard.createTable, 'test-cardboard-write');
        }

        q.awaitAll(function(err, resp) {
            t.notOk(err);
            t.end();
        });
    });
};

module.exports.teardown = function(t) {
    dyno.listTables(function(err, tables) {
        var q = queue();

        tables.TableNames.forEach(function(table) {
            q.defer(dyno.deleteTable, table);
        });

        q.awaitAll(function(err) {
            if (err) throw err;
            dynalite.close(function(err) {
                if (err) throw err;
                t.end();
            });
        });
    });
};
