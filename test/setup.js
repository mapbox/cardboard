var test = require('tap').test,
    Dynalite = require('dynalite'),
    Cardboard = require('../'),
    fakeAWS = require('mock-aws-s3'),
    queue = require('queue-async'),
    dynalite;

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
        q.awaitAll(function(err, resp){
            t.notOk(err);
            t.end();
        });
    });
};

module.exports.teardown = function(t) {
    dynalite.close();
    t.end();
};
