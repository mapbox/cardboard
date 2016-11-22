Error.stackTraceLimit = Infinity;

var assert = require('assert');
var exec = require('child_process').exec;
var path = require('path');
var fs = require('fs');
var crypto = require('crypto');
var cmd = path.resolve(__dirname, '..', 'bin', 'cardboard.js');
var _ = require('lodash');

var statesPath = path.resolve(__dirname, 'data', 'states.geojson');
var states = fs.readFileSync(statesPath, 'utf8');
states = JSON.parse(states);

var setup = require('./setup');
var config = setup.config;

var cardboard = require('..')(config);

describe('cli', function() {
    before(setup.setup);
    after(setup.teardown);
 
    it('[cli] config via env', function(done) {
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
            done();
        });
    });

    it('[cli] config via params', function(done) {
        var params = [
            cmd,
            '--region', 'region',
            '--featuresTable', config.featuresTable,
            '--searchTable', config.searchTable,
            '--bucket', 'bucket',
            '--prefix', 'prefix',
            '--endpoint', 'http://localhost:4567',
            'invalid-command'
        ];
        exec(params.join(' '), function(err, stdout, stderr) {
            // confirms that configuration was not the cause of the error
            assert.equal(stderr, 'invalid-command is not a valid command\n', 'expected error');
            done();
        });
    });

    it('[cli] config fail', function(done) {
        var params = [
            cmd,
            '--featuresTable', config.featuresTable,
            '--searchTable', config.searchTable,
            '--bucket', 'bucket',
            '--prefix', 'prefix',
            '--endpoint', 'http://localhost:4567',
            'invalid-command'
        ];
        exec(params.join(' '), function(err, stdout, stderr) {
            assert.equal(stderr, 'You must provide a region\n', 'expected error');
            done();
        });
    });

    it('[cli] get', function(done) {
        cardboard.batch.put(states, 'test', function(err, putResults) {
            if (err) throw err;
                var params = [
                cmd,
                '--region', 'region',
                '--featuresTable', config.featuresTable,
                '--searchTable', config.searchTable,
                '--bucket', 'test',
                '--prefix', 'test',
                '--endpoint', 'http://localhost:4567',
                'get', 'test', '\'' + putResults.features[0].id + '\''
            ];
            exec(params.join(' '), function(err, stdout) {
                assert.ifError(err, 'success');
                var found = JSON.parse(stdout.trim());
                assert.deepEqual(found, putResults.features[0], 'got expected feature');
                done();
            });
        });
    });

    it('[cli] list', function(done) {
        cardboard.batch.put(states, 'test', function(err, putResults) {
            if (err) throw err;
            var params = [
                cmd,
                '--region', 'region',
                '--featuresTable', config.featuresTable,
                '--searchTable', config.searchTable,
                '--bucket', 'test',
                '--prefix', 'test',
                '--endpoint', 'http://localhost:4567',
                'list', 'test'
            ];
            exec(params.join(' '), function(err, stdout) {
                assert.ifError(err, 'success');
                var found = JSON.parse(stdout.trim());
                assert.deepEqual(found, putResults, 'got expected FeatureCollection');
                done();
            });
        });
    });

    it('[cli] bbox', function(done) {
        cardboard.batch.put(states, 'test', function(err, putResults) {
            if (err) throw err;

            var params = [
                cmd,
                '--region', 'region',
                '--featuresTable', config.featuresTable,
                '--searchTable', config.searchTable,
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
                done();
            });
        });
    });
});

