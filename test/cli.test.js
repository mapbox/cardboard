Error.stackTraceLimit = Infinity;

var queue = require('queue-async');
var assert = require('assert');
var exec = require('child_process').exec;
var path = require('path');
var fs = require('fs');
var cmd = path.resolve(__dirname, '..', 'bin', 'cardboard.js');
var _ = require('lodash');

var statesPath = path.resolve(__dirname, 'data', 'states.geojson');
var states = fs.readFileSync(statesPath, 'utf8');
states = JSON.parse(states);

var setup = require('./setup');
var config = setup.config;

var cardboard = require('..')(config);
var mainTable = config.mainTable.config.params.TableName;

describe('cli', function() {
    var nhFeature = null;
    var features = [];
    before(setup.setup);
    after(setup.teardown);
    before(function(done) {
        var q = queue();
        states.features.map(function(f) {
            f.id = f.properties.name.replace(/ /g, '-').toLowerCase();
            return f;
        }).forEach(function(state) {
            q.defer(function(done) {
                cardboard.put(state, 'test', function(err, fc) {
                    if (err) return done(err);
                    var result = fc.features[0];
                    if (result.id === 'new-hampshire') nhFeature = result;
                    features.push(result);
                    done();
                });
            });
        });
        q.awaitAll(done);
    });
 
    it('[cli] config via env', function(done) {
        var options = {
            env: _.extend({
                CardboardRegion: 'region',
                CardboardMainTable: 'features',
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
            '--mainTable', mainTable,
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
            '--mainTable', mainTable,
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
        var params = [
            cmd,
            '--region', 'region',
            '--mainTable', mainTable,
            '--searchTable', config.searchTable,
            '--bucket', 'test',
            '--prefix', 'test',
            '--endpoint', 'http://localhost:4567',
            'get', 'test', '\'new-hampshire\''
        ];
        exec(params.join(' '), function(err, stdout) {
            assert.ifError(err, 'success');
            var found = JSON.parse(stdout.trim());
            assert.deepEqual(found.features[0], nhFeature);
            done();
        });
    });

});

