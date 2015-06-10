var test = require('tape');
var _ = require('lodash');
var dynalite = require('dynalite')({
    createTableMs: 0,
    updateTableMs: 0,
    deleteTableMs: 0
});

var config = {
    bucket: 'test',
    prefix: 'test',
    dyno: require('dyno')({
        region: 'fake',
        accessKeyId: 'fake',
        secretAccessKey: 'fake',
        endpoint: 'http://localhost:4567'
    }),
    s3: require('mock-aws-s3').S3()
};

test('[table] start dynalite', function(assert) {
    dynalite.listen(4567, function(err) {
        if (err) throw err;
        assert.end();
    });
});

test('[table] createTable - specified name', function(assert) {
    var cardboard = require('..')(_.extend({ table: 'original' }, config));
    cardboard.createTable(function(err) {
        assert.ifError(err, 'success');

        config.dyno.listTables(function(err, tables) {
            if (err) throw err;
            assert.deepEqual(tables.TableNames, ['original'], 'created table');
            assert.end();
        });
    });
});

test('[table] createTable - another name', function(assert) {
    var cardboard = require('..')(_.extend({ table: 'original' }, config));
    cardboard.createTable('second', function(err) {
        assert.ifError(err, 'success');

        config.dyno.listTables(function(err, tables) {
            if (err) throw err;
            assert.deepEqual(tables.TableNames, ['original', 'second'], 'created second table');
            assert.end();
        });
    });
});

test('[table] stop dynalite', function(assert) {
    dynalite.close(function(err) {
        if (err) throw err;
        assert.end();
    });
});
