var test = require('tape');
var path = require('path');
var fs = require('fs');
var utils = require('../lib/utils');
var geobuf = require('geobuf');
var fixture = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'data', 'numeric-properties.geojson')));

test('[geobuf conversion] geobuf round-trip changes numeric properties', function(assert) {
    var buf = geobuf.featureToGeobuf(fixture).toBuffer();
    var roundtrip = geobuf.geobufToFeature(buf);

    assert.notEqual(
        roundtrip.properties['@changeset'],
        fixture.properties['@changeset'],
        'large numeric feature properties are adjusted by straight geobuf conversion'
    );
    assert.end();
});

test('[geobuf conversion] cardboard round-trip does not change numeric properties', function(assert) {
    var config = { MAX_GEOMETRY_SIZE: 99999999 };
    var record = utils(config).toDatabaseRecord(fixture, 'dataset')[0];
    utils(config).resolveFeatures([record], function(err, collection) {
        if (err) return assert.end(err);
        var roundtrip = collection.features[0];
        assert.equal(
            roundtrip.properties['@changeset'],
            fixture.properties['@changeset'],
            'large numeric feature properties are not adjusted by cardboard roundtrip'
        );
        assert.end();
    });
});
