var assert = require('assert');
var queue = require('queue-async');
var _ = require('lodash');
var dynalite = require('dynalite')({
    createTableMs: 0,
    updateTableMs: 0,
    deleteTableMs: 0
});

var config = {
    bucket: 'test',
    prefix: 'test',
    region: 'fake',
    endpoint: 'http://localhost:4567',
    s3: require('mock-aws-s3').S3()
};

var dyno = require('dyno')({
    table: 'fake',
    region: 'fake',
    endpoint: 'http://localhost:4567'
});

describe('table creation', function() {
    beforeEach(function(done) {
        dynalite.listen(4567, done);
    });

    afterEach(function(done) {
        dyno.listTables(function(err, tables) {
            if (err) return done(err);
            var q = queue();
            tables.TableNames.forEach(function(name) {
                q.defer(function(done) {
                    dyno.deleteTable({TableName: name}, done);  
                });
            });

            q.awaitAll(function(err) {
                if (err) return done(err);
                dynalite.close(done);
            });
        });
    });

    it('createTable - match config name', function(done) {
        var cardboard = require('..')(_.extend({ mainTable: 'features' }, config));
        cardboard.createTable(function(err) {
            assert.ifError(err, 'success');
  
            dyno.listTables(function(err, tables) {
                if (err) throw err;
                assert.deepEqual(tables.TableNames, ['features'], 'created table');
                done();
            });
        });
    });

    it('createTable - match config name, with difference names', function(done) {
        var cardboard = require('..')(_.extend({ mainTable: 'first' }, config));
        cardboard.createTable(function(err) {
            assert.ifError(err, 'success');
  
            dyno.listTables(function(err, tables) {
                if (err) throw err;
                assert.deepEqual(tables.TableNames, ['first'], 'created table');
                done();
            });
        });
    });
});



