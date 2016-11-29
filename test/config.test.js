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

        omitConfig.mainTable = Dyno(featuresConfig, featuresConfig);
        var cardboard = Cardboard(omitConfig);
        var geojson = {type: 'Feature', properties: {}, geometry: {type: 'Point', coordinates:[1, 2]}};

        cardboard.put(geojson, 'default', function(err, feature) {
            assert.ifError(err);
            assert.deepEqual(geojson.geometry, feature.geometry);
            done();
        });
    });

});
