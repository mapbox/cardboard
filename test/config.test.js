var assert = require('assert');
var Cardboard = require('../');
var _ = require('lodash');
var Dyno = require('dyno');

var setup = require('./setup');
var config = setup.config;

describe('config tests', function() {

    before(setup.setup);
    after(setup.teardown);

    it('pass preconfigured dyno object', function(done) {

        var omitConfig = _.omit(config, ['accessKeyId', 'secretAccessKey', 'table', 'endpoint', 'region']);

        var featuresConfig = {
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            region: 'us-east-1',
            table: 'features',
            endpoint: 'http://localhost:4567'
        };

        var searchConfig = {
            accessKeyId: 'fake',
            secretAccessKey: 'fake',
            region: 'us-east-1',
            table: 'search',
            endpoint: 'http://localhost:4567'
        };

        omitConfig.features = Dyno(featuresConfig, featuresConfig);
        omitConfig.search = Dyno(searchConfig, searchConfig);
        var cardboard = Cardboard(omitConfig);
        var geojson = {type: 'Feature', properties: {}, geometry: {type: 'Point', coordinates:[1, 2]}};

        cardboard.put(geojson, 'default', function(err) {
            assert.ifError(err);

            cardboard.list('default', function(err, fc) {
                assert.ifError(err);
                assert.equal(fc.features.length, 1);
                delete fc.features[0].id;
                assert.deepEqual(fc, { type: 'FeatureCollection', features: [geojson] }, 'one result');
                done();
            });
        });
    });

});
