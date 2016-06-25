var test = require('tape');
var path = require('path');
var fs = require('fs');
// var utils = require('../lib/utils');
var geobuf = require('geobuf');

test('[geobuf conversion] does not affect numeric properties', function(assert) {
    var fixture = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'data', 'numeric-properties.geojson')));

    var buf = geobuf.featureToGeobuf(fixture).toBuffer();
    var roundtrip = geobuf.geobufToFeature(buf);

    assert.deepEqual(roundtrip.properties, fixture.properties, 'feature properties unaffected');
    assert.end();
});
