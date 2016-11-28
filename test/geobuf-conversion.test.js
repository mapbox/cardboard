var assert = require('assert');
var path = require('path');
var fs = require('fs');
var utils = require('../lib/utils');
var fixture = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'data', 'numeric-properties.geojson')));

describe('geobuf-conversion', function() {
    it('cardboard round-trip does not change numeric properties', function(done) {
        var config = { MAX_GEOMETRY_SIZE: 99999999 };
        var record = utils(config).toDatabaseRecord(fixture, 'dataset').feature;
        utils(config).resolveFeatures([record], function(err, collection) {
            if (err) return done(err);
            var roundtrip = collection.features[0];
            assert.equal(
                roundtrip.properties['@changeset'],
                fixture.properties['@changeset'],
                'large numeric feature properties are not adjusted by cardboard roundtrip'
            );
            done();
        });
    });
});

