Error.stackTraceLimit = Infinity;

var test = require('tape');
var dynamodb = require('@mapbox/dynamodb-test')(test, 'cardboard-cli', require('../lib/table.json'));
var liveDynamo = require('@mapbox/dynamodb-test')(test, 'cardboard-cli', require('../lib/table.json'), 'us-east-1');
var exec = require('child_process').exec;
var path = require('path');
var fs = require('fs');
var crypto = require('crypto');
var cmd = path.resolve(__dirname, '..', 'bin', 'cardboard.js');
var _ = require('lodash');

var statesPath = path.resolve(__dirname, 'data', 'states.geojson');
var states = fs.readFileSync(statesPath, 'utf8');
states = JSON.parse(states);

var config = {
    bucket: 'test',
    prefix: 'test',
    dyno: dynamodb.dyno,
    s3: require('mock-aws-s3').S3()
};

var cardboard = require('..')(config);

dynamodb.test('[cli] config via env', function(assert) {
    var options = {
        env: _.extend({
            CardboardRegion: 'region',
            CardboardTable: 'table',
            CardboardBucket: 'bucket',
            CardboardPrefix: 'prefix',
            CardboardEndpoint: 'http://localhost:4567'
        }, process.env)
    };

    exec(cmd + ' invalid-command', options, function(err, stdout, stderr) {
        // confirms that configuration was not the cause of the error
        assert.equal(stderr, 'invalid-command is not a valid command\n', 'expected error');
        assert.end();
    });
});

dynamodb.test('[cli] config via params', function(assert) {
    var params = [
        cmd,
        '--region', 'region',
        '--table', 'table',
        '--bucket', 'bucket',
        '--prefix', 'prefix',
        '--endpoint', 'http://localhost:4567',
        'invalid-command'
    ];

    exec(params.join(' '), function(err, stdout, stderr) {
        // confirms that configuration was not the cause of the error
        assert.equal(stderr, 'invalid-command is not a valid command\n', 'expected error');
        assert.end();
    });
});

dynamodb.test('[cli] config fail', function(assert) {
    var params = [
        cmd,
        '--table', 'table',
        '--bucket', 'bucket',
        '--prefix', 'prefix',
        '--endpoint', 'http://localhost:4567',
        'invalid-command'
    ];

    exec(params.join(' '), function(err, stdout, stderr) {
        assert.equal(stderr, 'You must provide a region\n', 'expected error');
        assert.end();
    });
});

dynamodb.start();

test('[cli] get', function(assert) {
    cardboard.batch.put(states, 'test', function(err, putResults) {
        if (err) throw err;

        var params = [
            cmd,
            '--region', 'region',
            '--table', dynamodb.tableName,
            '--bucket', 'test',
            '--prefix', 'test',
            '--endpoint', 'http://localhost:4567',
            'get', 'test', '\'' + putResults.features[0].id + '\''
        ];

        exec(params.join(' '), function(err, stdout) {
            assert.ifError(err, 'success');
            var found = JSON.parse(stdout.trim());
            assert.deepEqual(found, putResults.features[0], 'got expected feature');
            assert.end();
        });
    });
});

dynamodb.empty();

test('[cli] list', function(assert) {
    cardboard.batch.put(states, 'test', function(err, putResults) {
        if (err) throw err;

        var params = [
            cmd,
            '--region', 'region',
            '--table', dynamodb.tableName,
            '--bucket', 'test',
            '--prefix', 'test',
            '--endpoint', 'http://localhost:4567',
            'list', 'test'
        ];

        exec(params.join(' '), function(err, stdout) {
            assert.ifError(err, 'success');
            var found = JSON.parse(stdout.trim());
            assert.deepEqual(found, putResults, 'got expected FeatureCollection');
            assert.end();
        });
    });
});

dynamodb.empty();

test('[cli] bbox', function(assert) {
    cardboard.batch.put(states, 'test', function(err, putResults) {
        if (err) throw err;

        var params = [
            cmd,
            '--region', 'region',
            '--table', dynamodb.tableName,
            '--bucket', 'test',
            '--prefix', 'test',
            '--endpoint', 'http://localhost:4567',
            'bbox', 'test', '\'-120,30,-115,35\''
        ];

        var cali = putResults.features.filter(function(state) {
            return state.properties.name === 'California';
        })[0];

        exec(params.join(' '), function(err, stdout) {
            assert.ifError(err, 'success');
            var found = JSON.parse(stdout.trim());
            assert.deepEqual(found, {type: 'FeatureCollection', features: [cali]}, 'found California');
            assert.end();
        });
    });
});

dynamodb.close();

liveDynamo.start();

test('[cli] put', function(assert) {
    var liveConfig = {
        bucket: 'mapbox-sandbox',
        prefix: 'cardboard-test/' + crypto.randomBytes(4).toString('hex'),
        dyno: liveDynamo.dyno
    };

    var live = require('..')(liveConfig);

    var params = [
        cmd,
        '--region', 'us-east-1',
        '--table', liveDynamo.tableName,
        '--bucket', liveConfig.bucket,
        '--prefix', liveConfig.prefix,
        'put', 'test'
    ];

    var proc = exec(params.join(' '), function(err) {
        assert.ifError(err, 'success');

        var found = {type: 'FeatureCollection', features: []};
        live.list('test')
            .on('data', function(feature) {
                found.features.push(feature);
            })
            .on('end', function() {
                assert.equal(found.features.length, states.features.length, 'inserted all the features');
                assert.end();
            });
    });

    fs.createReadStream(statesPath).pipe(proc.stdin);
});

liveDynamo.delete();
