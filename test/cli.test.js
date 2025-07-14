Error.stackTraceLimit = Infinity;

var exec = require('child_process').exec;
var path = require('path');
var fs = require('fs');
var cmd = path.resolve(__dirname, '..', 'bin', 'cardboard.js');
var _ = require('lodash');


var mainTable = require('@mapbox/dynamodb-test')(require('tape'), 'cardboard', require('../lib/main-table.json'));

var config = {
    region: 'test',
    mainTable: mainTable.tableName,
    endpoint: 'http://localhost:4567'
};

var utils = require('../lib/utils');

var statesPath = path.resolve(__dirname, 'data', 'states.geojson');
var states = fs.readFileSync(statesPath, 'utf8');
var nhFeature = null
var features = JSON.parse(states).features.map(function(f) {
    f.id = f.properties.name.replace(/ /g, '-').toLowerCase();
    if (f.id === 'new-hampshire') nhFeature = utils.decodeBuffer(utils.encodeFeature(f));
    return f;
}).map(function(f) {
    return utils.toDatabaseRecord(f, 'test');
});

mainTable.test('[cli] config via env', features, function(assert) {
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
        assert.end();
    });
});

mainTable.test('[cli] config via params', features, function(assert) {
    var params = [
        cmd,
        '--region', 'region',
        '--mainTable', config.mainTable,
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

mainTable.test('[cli] config fail', features, function(assert) {
    var params = [
        cmd,
        '--mainTable', config.mainTable,
        '--searchTable', config.searchTable,
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

mainTable.test('[cli] get', features, function(assert) {
    var params = [
        cmd,
        '--region', 'region',
        '--mainTable', config.mainTable,
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
        assert.end();
    });
});

mainTable.close();
