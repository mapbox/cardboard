var test = require('tap').test,
    Dynalite = require('dynalite'),
    Cardboard = require('../'),
    fakeAWS = require('mock-aws-s3'),
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

module.exports.setup = function(t) {
    dynalite = Dynalite({
        createTableMs: 0,
        updateTableMs: 0,
        deleteTableMs: 0
    });
    dynalite.listen(4567, function() {
        t.pass('dynalite listening');
        var cardboard = Cardboard(config);
        cardboard.createTable(config.table, function(err, resp){
            t.pass('table created');
            t.end();
        });
    });
};

module.exports.teardown = function(t) {
    dynalite.close();
    t.end();
};
