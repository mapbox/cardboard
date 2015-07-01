var test = require('tape');
var dynamodb = require('dynamodb-test')(test, 'cardboard', require('../lib/table.json'));
var utils = require('../lib/utils');
var fs = require('fs');
var path = require('path');
var _ = require('lodash');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');

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
        dynamodb.dyno.scan(function(err, items) {
            if (err) throw err;
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
    var buf = encoded[1];

    assert.ok(item.id, 'an id was assigned');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');

    assert.ok(item.s3url.indexOf('s3://test/test/dataset/' + item.id.split('!')[1]) === 0, 's3url was assigned correctly');

    assert.deepEqual(item.val, buf.Body, 'geobuf was stored in the item');
    assert.equal(buf.Bucket, config.bucket, 'S3 params include proper bucket');
    assert.equal(buf.Key, item.s3url.split('s3://test/')[1], 'S3 params include proper key');

    noId.id = item.id.split('!')[1];
    assert.deepEqual(geobuf.geobufToFeature(buf.Body), noId, 'geobuf encoded as expected');

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
    var buf = encoded[1];

    assert.equal(item.id.split('!')[1], hasId.id, 'used user-assigned id');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');

    assert.ok(item.s3url.indexOf('s3://test/test/dataset/' + item.id.split('!')[1]) === 0, 's3url was assigned correctly');

    assert.deepEqual(item.val, buf.Body, 'geobuf was stored in the item');
    assert.equal(buf.Bucket, config.bucket, 'S3 params include proper bucket');
    assert.equal(buf.Key, item.s3url.split('s3://test/')[1], 'S3 params include proper key');

    assert.deepEqual(geobuf.geobufToFeature(buf.Body), hasId, 'geobuf encoded as expected');

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
    var buf = encoded[1];

    assert.equal(item.id.split('!')[1], numericId.id.toString(), 'used numeric user-assigned id as a string');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');

    assert.ok(item.s3url.indexOf('s3://test/test/dataset/' + item.id.split('!')[1]) === 0, 's3url was assigned correctly');

    assert.deepEqual(item.val, buf.Body, 'geobuf was stored in the item');
    assert.equal(buf.Bucket, config.bucket, 'S3 params include proper bucket');
    assert.equal(buf.Key, item.s3url.split('s3://test/')[1], 'S3 params include proper key');

    numericId.id = numericId.id.toString();
    assert.deepEqual(geobuf.geobufToFeature(buf.Body), numericId, 'geobuf encoded as expected');

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
    var buf = encoded[1];

    assert.equal(item.id.split('!')[1], zeroId.id.toString(), 'used zero (as a string) for id');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');

    assert.ok(item.s3url.indexOf('s3://test/test/dataset/' + item.id.split('!')[1]) === 0, 's3url was assigned correctly');

    assert.deepEqual(item.val, buf.Body, 'geobuf was stored in the item');
    assert.equal(buf.Bucket, config.bucket, 'S3 params include proper bucket');
    assert.equal(buf.Key, item.s3url.split('s3://test/')[1], 'S3 params include proper key');

    zeroId.id = item.id.split('!')[1];
    assert.deepEqual(geobuf.geobufToFeature(buf.Body), zeroId, 'geobuf encoded as expected');

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
    var buf = encoded[1];

    assert.notEqual(item.id, 'id!null', 'null id was treated as undefined');
    assert.ok(item.id, 'an id was assigned');

    assert.ok(item.west === 0 &&
        item.south === 0 &&
        item.east === 0 &&
        item.north === 0, 'correct extent');
    assert.ok(item.size, 'size was calculated');

    assert.equal(item.cell, 'cell!3000000000000000000000000000', 'expected cell');

    assert.ok(item.s3url.indexOf('s3://test/test/dataset/' + item.id.split('!')[1]) === 0, 's3url was assigned correctly');

    assert.deepEqual(item.val, buf.Body, 'geobuf was stored in the item');
    assert.equal(buf.Bucket, config.bucket, 'S3 params include proper bucket');
    assert.equal(buf.Key, item.s3url.split('s3://test/')[1], 'S3 params include proper key');

    nullId.id = item.id.split('!')[1];
    assert.deepEqual(geobuf.geobufToFeature(buf.Body), nullId, 'geobuf encoded as expected');

    assert.end();
});

test('[utils] toDatabaseRecord - no geometry', function(assert) {
    var noGeom = {
        type: 'Feature',
        properties: {
            hasNo: 'geometry'
        }
    };

    try { var encoded = utils.toDatabaseRecord(noGeom, 'dataset'); }
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
    var buf = encoded[1];

    assert.notOk(item.val, 'large geobuf not stored in database record');
    assert.end();
});

dynamodb.close();
