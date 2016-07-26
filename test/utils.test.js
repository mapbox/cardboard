var test = require('tape');
var dynamodb = require('dynamodb-test')(test, 'cardboard', require('../lib/table.json'));
var fs = require('fs');
var path = require('path');
var _ = require('lodash');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');
var url = require('url');

var states = fs.readFileSync(path.resolve(__dirname, 'data', 'states.geojson'), 'utf8');
states = JSON.parse(states);

var config = {
    bucket: 'test',
    prefix: 'test',
    dyno: dynamodb.dyno,
    s3: require('mock-aws-s3').S3()
};

var cardboard = require('..')(config);
var utils = require('../lib/utils')(config);

dynamodb.start();

test('[utils] resolveFeatures', function(assert) {
    cardboard.batch.put(states, 'test', function(err, putResults) {
        if (err) throw err;

        var items = [];
        dynamodb.dyno.scanStream().on('data', function(d) { items.push(d); }).on('end', function() {
            utils.resolveFeatures(items, function(err, resolveResults) {
                assert.ifError(err, 'success');

                putResults = _.indexBy(putResults.features, 'id');
                resolveResults = _.indexBy(resolveResults.features, 'id');

                _.forOwn(resolveResults, function(found, id) {
                    var expected = putResults[id];
                    assert.deepEqual(found, expected, 'expected feature');
                });

                assert.end();
            });
        });
    });
});

test('[utils] resolveFeatures - large feature', function(assert) {

    var feature = {
        type: 'Feature',
        quadkey: '1222222212112112222210310',
        properties: {
            data: (new Buffer(15 * 1024)).toString('hex')
        },
        geometry: {
            type: 'Point',
            coordinates: [1, 1]
        }
    };

    cardboard.put(feature, 'large', function(err, putResults) {
        if (err) throw err;
        var key = { dataset: 'large', id: 'id!' + putResults.id };
        dynamodb.dyno.getItem({Key: key}, function(err, data) {
            if (err) throw err;
            utils.resolveFeatures([data.Item], function(err, resolveResults) {
                assert.ifError(err, 'success');
                delete resolveResults.features[0].id;
                assert.deepEqual(resolveResults.features[0], feature, 'expected feature');
                assert.end();
            });
        });
    });
});

test('[utils] resolveFeatures - large, corrupt feature', function(assert) {

    var feature = {
        type: 'Feature',
        properties: {
            data: (new Buffer(15 * 1024)).toString('hex')
        },
        geometry: {
            type: 'Point',
            coordinates: [1, 1]
        }
    };

    cardboard.put(feature, 'large', function(err, putResults) {
        if (err) throw err;
        var key = { dataset: 'large', id: 'id!' + putResults.id };
        dynamodb.dyno.getItem({Key: key}, function(err, data) {
            if (err) throw err;
            var uri = url.parse(data.Item.s3url);
            config.s3.putObject({
                Bucket: uri.host,
                Key: uri.pathname.substr(1),
                Body: new Buffer('this is not a valid protobuf')
            }, function(err) {
                if (err) throw err;
                utils.resolveFeatures([data.Item], function(err) {
                    assert.equal(err.message, 'Illegal group end indicator for Message .featurecollection: 14 (not a group)');
                    assert.end();
                });
            });
        });
    });
});

test('[utils] toDatabaseRecord - no ID', function(assert) {
    var noId = {
        type: 'Feature',
        properties: {
            hasNo: 'id'
        },
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    var encoded = utils.toDatabaseRecord(noId, 'dataset');
    var item = encoded[0];

    assert.notOk(encoded[1], 'no S3 data stored for a small item');
    assert.ok(item.id, 'an id was assigned');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');
    assert.notOk(item.s3url, 's3url was not assigned to a small feature');
    assert.ok(item.val, 'geobuf was stored in the item');

    noId.id = utils.idFromRecord(item);
    assert.deepEqual(geobuf.geobufToFeature(item.val), noId, 'geobuf encoded as expected');

    assert.end();
});

test('[utils] toDatabaseRecord - large feature', function(assert) {
    var noId = {
        type: 'Feature',
        properties: {
            hasNo: 'id',
            biggie: (new Buffer(15 * 1024)).toString('hex')
        },
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    var encoded = utils.toDatabaseRecord(noId, 'dataset');
    var item = encoded[0];
    var s3params = encoded[1];

    assert.notOk(item.val, 'geobuf was not stored in the item');
    assert.ok(s3params, 'S3 data stored for a large item');
    assert.ok(item.id, 'an id was assigned');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');
    assert.ok(item.s3url.indexOf('s3://test/test/dataset/' + utils.idFromRecord(item)) === 0, 's3url was assigned correctly');

    noId.id = utils.idFromRecord(item);
    assert.deepEqual(geobuf.geobufToFeature(s3params.Body), noId, 'geobuf encoded as expected');
    assert.equal(s3params.Bucket, config.bucket, 'S3 params include proper bucket');
    assert.equal(s3params.Key, item.s3url.split('s3://test/')[1], 'S3 params include proper key');

    assert.end();
});

test('[utils] toDatabaseRecord - with ID', function(assert) {
    var hasId = {
        id: 'bacon-lettuce-tomato',
        type: 'Feature',
        properties: {
            hasAn: 'id'
        },
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    var encoded = utils.toDatabaseRecord(hasId, 'dataset');
    var item = encoded[0];

    assert.notOk(encoded[1], 'no S3 data stored for a small item');
    assert.equal(utils.idFromRecord(item), hasId.id, 'used user-assigned id');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');
    assert.notOk(item.s3url, 's3url was not assigned to a small feature');
    assert.ok(item.val, 'geobuf was stored in the item');
    assert.deepEqual(geobuf.geobufToFeature(item.val), hasId, 'geobuf encoded as expected');

    assert.end();
});

test('[utils] toDatabaseRecord - numeric IDs become strings', function(assert) {
    var numericId = {
        id: 12,
        type: 'Feature',
        properties: {
            hasAn: 'id'
        },
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    var encoded = utils.toDatabaseRecord(numericId, 'dataset');
    var item = encoded[0];

    assert.notOk(encoded[1], 'no S3 data stored for a small item');
    assert.equal(utils.idFromRecord(item), numericId.id.toString(), 'used numeric user-assigned id as a string');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');
    assert.notOk(item.s3url, 's3url was not assigned to a small feature');
    assert.ok(item.val, 'geobuf was stored in the item');

    numericId.id = numericId.id.toString();
    assert.deepEqual(geobuf.geobufToFeature(item.val), numericId, 'geobuf encoded as expected');

    assert.end();
});

test('[utils] toDatabaseRecord - zero is an acceptable ID', function(assert) {
    var zeroId = {
        id: 0,
        type: 'Feature',
        properties: {
            hasAn: 'id'
        },
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    var encoded = utils.toDatabaseRecord(zeroId, 'dataset');
    var item = encoded[0];

    assert.notOk(encoded[1], 'no S3 data stored for a small item');
    assert.equal(utils.idFromRecord(item), zeroId.id.toString(), 'used zero (as a string) for id');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');
    assert.notOk(item.s3url, 's3url was not assigned to a small feature');
    assert.ok(item.val, 'geobuf was stored in the item');

    zeroId.id = utils.idFromRecord(item);
    assert.deepEqual(geobuf.geobufToFeature(item.val), zeroId, 'geobuf encoded as expected');

    assert.end();
});

test('[utils] toDatabaseRecord - null ID', function(assert) {
    var nullId = {
        id: null,
        type: 'Feature',
        properties: {
            hasAn: 'id'
        },
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    var encoded = utils.toDatabaseRecord(nullId, 'dataset');
    var item = encoded[0];

    assert.notOk(encoded[1], 'no S3 data stored for a small item');
    assert.notEqual(item.id, 'id!null', 'null id was treated as undefined');
    assert.ok(item.id, 'an id was assigned');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');
    assert.notOk(item.s3url, 's3url was not assigned to a small feature');
    assert.ok(item.val, 'geobuf was stored in the item');

    nullId.id = utils.idFromRecord(item);
    assert.deepEqual(geobuf.geobufToFeature(item.val), nullId, 'geobuf encoded as expected');

    assert.end();
});

test('[utils] toDatabaseRecord - no geometry', function(assert) {
    var noGeom = {
        type: 'Feature',
        properties: {
            hasNo: 'geometry'
        }
    };

    try { utils.toDatabaseRecord(noGeom, 'dataset'); }
    catch (err) {
        assert.equal(err.message, 'Unlocated features can not be stored.', 'expected error message');
        return assert.end();
    }

    assert.fail('encoded feature without geometry');
    assert.end();
});

test('[utils] toDatabaseRecord - large feature', function(assert) {
    var large = fixtures.random(1, 100000).features[0];
    large.id = 'biggie-fries';

    var encoded = utils.toDatabaseRecord(large, 'dataset');
    var item = encoded[0];

    assert.notOk(item.val, 'large geobuf not stored in database record');
    assert.end();
});

test('[utils] idFromRecord - no ! in the id', function(assert) {
    var record = { id: 'id!123456' };
    assert.equal(utils.idFromRecord(record), '123456', 'expected value');
    assert.end();
});

test('[utils] idFromRecord - has ! in the id', function(assert) {
    var record = { id: 'id!123456!654321' };
    assert.equal(utils.idFromRecord(record), '123456!654321', 'expected value');
    assert.end();
});

test('[utils] idFromRecord - emoji', function(assert) {
    var record = { id: 'id!\u1F471' };
    assert.equal(utils.idFromRecord(record), '\u1F471', 'expected value');
    assert.end();
});

test('[utils] getCenterpoint - bbox deep equal accross meridians' , function(assert) {
    var bbox = [-1,-1,1,1];
    var expected = [0,0];
    assert.deepEqual(utils.getCenterpoint(bbox), expected, 'expected value');
    assert.end();
});

test('[utils] getCenterpoint - bbox deep equal' , function(assert) {
    var bbox = [-110.3,32.5,-90.5,33.2];
    var expected = [-100.4, 32.85];
    assert.deepEqual(utils.getCenterpoint(bbox), expected, 'expected value');
    assert.end();
});


test('[utils] getCenterpoint - bbox as floating exponent' , function(assert) {
    var bbox = [-1E-6, 1 - 1E-6, 1E+6, 1 + 1E+6];
    var expected = [499999.9999995, 500000.9999995];
    assert.deepEqual(utils.getCenterpoint(bbox), expected, 'expected value');
    assert.end();
});

test('[utils] getCenterpoint - bbox of number strings' , function(assert) {
    var bbox = ['10','-10','20','0'];
    var expected = [15,-5];
    assert.deepEqual(utils.getCenterpoint(bbox), expected, 'expected value');
    assert.end();
});

test('[utils] getCenterpoint - bbox of non-number strings are NaN' , function(assert) {
    var bbox = ['w','s','e','n'];
    var expected = true;
    assert.equal(isNaN(utils.getCenterpoint(bbox)[0]), expected, 'expected value');
    assert.equal(isNaN(utils.getCenterpoint(bbox)[1]), expected, 'expected value');
    assert.end();
});

test('[utils] getQuadkey - to be string', function(assert) {
    var center = [0,0];
    var expected = 'string';
    var expectedLen = 25;
    assert.equal(typeof utils.getQuadkey(center[0], center[1]), expected, 'expected type');
    assert.equal(utils.getQuadkey(center[0], center[1]).length, expectedLen, 'expected length');
    assert.end();
});

test('[utils] getQuadkey - to be quadkey', function(assert) {
    var center = [0,0];
    var expected = '3000000000000000000000000';
    assert.equal(utils.getQuadkey(center[0], center[1]), expected, 'expected value');
    assert.end();
});

test('[utils] calcQuadkeyRange - to be object', function(assert) {
    var bbox = [-1,-1,1,1];
    var expected = 'object';
    var type = 'string';
    var range = utils.calcQuadkeyRange(bbox);
    assert.equal(typeof range, expected, 'expected type');
    assert.equal(typeof range.nw, type, 'expected property type');
    assert.equal(typeof range.se, type, 'expected property type');
    assert.end();
});

test('[utils] calcQuadkeyRange - to be range', function(assert) {
    var bbox = [-1,-1,1,1];
    var range = utils.calcQuadkeyRange(bbox);
    var value = range.nw < range.se;
    var expected = true;
    assert.deepEqual(value, expected, 'expected true');
    assert.end();
});

test('[utils] calcQuadkeyRange - to be range', function(assert) {
    var bbox = [-120,30,-110,40];
    var range = utils.calcQuadkeyRange(bbox);
    var value = range.nw < range.se;
    var expected = true;
    assert.deepEqual(value, expected, 'expected true');
    assert.end();
});

test('[utils] calcQuadkeyRange - with anti-meridian lon', function(assert) {
    var bbox = [-185,30,-110,40];
    var range = utils.calcQuadkeyRange(bbox);
    var value = range.nw < range.se;
    var expected = true;
    assert.deepEqual(value, expected, 'expected true');
    assert.end();
});

test('[utils] calcQuadkeyRange - with anti-meridian lat', function(assert) {
    var bbox = [-120,-100,-110,40];
    var range = utils.calcQuadkeyRange(bbox);
    var value = range.nw < range.se;
    var expected = true;
    assert.deepEqual(value, expected, 'expected true');
    assert.end();
});

dynamodb.close();
