var test = require('tape');
var Cardboard = require('../');
var _ = require('lodash');
var Dyno = require('dyno');

var s = require('./setup');
var config = s.config;

test('setup', function(t) { s.setup(t, true); });

test('pass preconfigured dyno object', function(t) {

    var omitConfig = _.omit(config, ['accessKeyId', 'secretAccessKey', 'table', 'endpoint', 'region']);

    var dynoconfig = {
        accessKeyId: 'fake',
        secretAccessKey: 'fake',
        region: 'us-east-1',
        table: 'test-cardboard-write',
        endpoint: 'http://localhost:4567'
    };

    omitConfig.dyno = Dyno(dynoconfig, dynoconfig);
    var cardboard = Cardboard(omitConfig);
    var geojson = {type: 'Feature', properties: {}, geometry: {type: 'Point', coordinates:[1, 2]}};

    cardboard.put(geojson, 'default', function(err) {
        t.notOk(err);

        cardboard.list('default', function(err, items) {
            t.equal(err, null);
            delete items.features[0].id;
            t.deepEqual(items, { type: 'FeatureCollection', features: [geojson] }, 'one result');
            t.end();
        });
    });
});

test('teardown', s.teardown);
