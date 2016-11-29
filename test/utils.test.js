var Pbf = require('pbf');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');
var url = require('url');

var states = fs.readFileSync(path.resolve(__dirname, 'data', 'states.geojson'), 'utf8');
states = JSON.parse(states);

var setup = require('./setup');
var config = setup.config;

var cardboard = require('..')(config);
var utils = require('../lib/utils')(config);

var notOk = require('./not-ok.js');

describe('utils', function() {
    before(setup.setup);
    after(setup.teardown);

    it('resolveFeatures - large feature', function(done) {
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
            var key = utils.createFeatureKey('large', putResults.id);
            config.mainTable.getItem({Key: key}, function(err, data) {
                if (err) throw err;
                utils.resolveFeatures([data.Item], function(err, resolveResults) {
                    assert.ifError(err, 'success');
                    delete resolveResults.features[0].id;
                    assert.deepEqual(resolveResults.features[0], feature, 'expected feature');
                    done();
                });
            });
        });
    });

    it('resolveFeatures - large, corrupt feature', function(done) {
        var feature = {
            id: 'corrupt',
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
            var key = utils.createFeatureKey('large', putResults.id);
            config.mainTable.getItem({Key: key}, function(err, data) {
                if (err) throw err;
                notOk(data.Item.val);
                var uri = url.parse(data.Item.s3url);
                config.s3.putObject({
                    Bucket: uri.host,
                    Key: uri.pathname.substr(1),
                    Body: new Buffer('this is not a valid protobuf')
                }, function(err) {
                    if (err) throw err;
                    utils.resolveFeatures([data.Item], function(err, features) {
                        notOk(features);
                        assert.ok(err);
                        assert.equal(err.message, 'Unimplemented type: 4');
                        done();
                    });
                });
            });
        });
    });

    it('toDatabaseRecord - no ID', function(done) {
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
        var feature = encoded.feature;
        
        notOk(encoded.s3, 'no S3 data stored for a small item');
        assert.equal(feature.index, 'dataset!feature!'+utils.idFromRecord(feature), 'an id was assigned');
    
        assert.ok(feature.west === 0 &&
            feature.south === 0 &&
            feature.east === 0 &&
            feature.north === 0, 'correct extent');
        assert.ok(feature.size, 'size was calculated');

        notOk(feature.s3url, 's3url was not assigned to a small feature');
        assert.ok(feature.val, 'geobuf was stored in the item');

        noId.id = utils.idFromRecord(feature);
        assert.deepEqual(geobuf.decode(new Pbf(feature.val)), noId, 'geobuf encoded as expected');

        done();
    });

    it('toDatabaseRecord - large feature', function(done) {
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
        var item = encoded.feature;
        var s3params = encoded.s3;

        notOk(item.val, 'geobuf was not stored in the item');
        assert.ok(s3params, 'S3 data stored for a large item');
        assert.equal(item.index, 'dataset!feature!'+utils.idFromRecord(item), 'an id was assigned');

        assert.ok(item.west === 0 &&
            item.south === 0 &&
            item.east === 0 &&
            item.north === 0, 'correct extent');
        assert.ok(item.size, 'size was calculated');

        assert.ok(item.s3url.indexOf('s3://test/test/dataset/' + utils.idFromRecord(item)) === 0, 's3url was assigned correctly');

        noId.id = utils.idFromRecord(item);
        assert.deepEqual(geobuf.decode(new Pbf(s3params.Body)), noId, 'geobuf encoded as expected');
        assert.equal(s3params.Bucket, config.bucket, 'S3 params include proper bucket');
        assert.equal(s3params.Key, item.s3url.split('s3://test/')[1], 'S3 params include proper key');

        done();
    });

    it('toDatabaseRecord - with ID', function(done) {
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
        var item = encoded.feature;

        notOk(encoded.s3, 'no S3 data stored for a small item');
        assert.equal(utils.idFromRecord(item), hasId.id, 'used user-assigned id');

        assert.ok(item.west === 0 &&
            item.south === 0 &&
            item.east === 0 &&
            item.north === 0, 'correct extent');
        assert.ok(item.size, 'size was calculated');

        notOk(item.s3url, 's3url was not assigned to a small feature');
        assert.ok(item.val, 'geobuf was stored in the item');
        assert.deepEqual(geobuf.decode(new Pbf(item.val)), hasId, 'geobuf encoded as expected');

        done();
    });

    it('toDatabaseRecord - numeric IDs become strings', function(done) {
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
        var item = encoded.feature;

        notOk(encoded.s3, 'no S3 data stored for a small item');
        assert.equal(utils.idFromRecord(item), numericId.id.toString(), 'used numeric user-assigned id as a string');

        assert.ok(item.west === 0 &&
            item.south === 0 &&
            item.east === 0 &&
            item.north === 0, 'correct extent');
        assert.ok(item.size, 'size was calculated');
        notOk(item.s3url, 's3url was not assigned to a small feature');
        assert.ok(item.val, 'geobuf was stored in the item');

        numericId.id = numericId.id.toString();
        assert.deepEqual(geobuf.decode(new Pbf(item.val)), numericId, 'geobuf encoded as expected');

        done();
    });

    it('toDatabaseRecord - zero is an acceptable ID', function(done) {
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
        var item = encoded.feature;

        notOk(encoded.s3, 'no S3 data stored for a small item');
        assert.equal(utils.idFromRecord(item), zeroId.id.toString(), 'used zero (as a string) for id');

        assert.ok(item.west === 0 &&
            item.south === 0 &&
            item.east === 0 &&
            item.north === 0, 'correct extent');
        assert.ok(item.size, 'size was calculated');

        notOk(item.s3url, 's3url was not assigned to a small feature');
        assert.ok(item.val, 'geobuf was stored in the item');

        zeroId.id = utils.idFromRecord(item);
        assert.deepEqual(geobuf.decode(new Pbf(item.val)), zeroId, 'geobuf encoded as expected');

        done();
    });

    it('toDatabaseRecord - null ID', function(done) {
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
        var item = encoded.feature;
        notOk(encoded.s3, 'no S3 data stored for a small item');
        assert.notEqual(item.index, 'dataset!feature!null', 'null id was treated as undefined');
        assert.equal(item.index, 'dataset!feature!'+utils.idFromRecord(item), 'an id was assigned');

        assert.ok(item.west === 0 &&
            item.south === 0 &&
            item.east === 0 &&
            item.north === 0, 'correct extent');
        assert.ok(item.size, 'size was calculated');

        notOk(item.s3url, 's3url was not assigned to a small feature');
        assert.ok(item.val, 'geobuf was stored in the item');

        nullId.id = utils.idFromRecord(item);
        assert.deepEqual(geobuf.decode(new Pbf(item.val)), nullId, 'geobuf encoded as expected');

        done();
    });

    it('toDatabaseRecord - no geometry', function(done) {
        var noGeom = {
            type: 'Feature',
            properties: {
                hasNo: 'geometry'
            }
        };

        try { utils.toDatabaseRecord(noGeom, 'dataset'); }
        catch (err) {
            assert.equal(err.message, 'Unlocated features can not be stored.', 'expected error message');
            return done();
        }

        assert.fail('encoded feature without geometry');
        done();
    });

    it('toDatabaseRecord - large feature', function(done) {
        var large = fixtures.random(1, 100000).features[0];
        large.id = 'biggie-fries';

        var encoded = utils.toDatabaseRecord(large, 'dataset');
        var item = encoded.feature;

        notOk(item.val, 'large geobuf not stored in database record');
        done();
    });

    it('idFromRecord - no ! in the id', function(done) {
        var record = { index: 'id!feature!123456' };
        assert.equal(utils.idFromRecord(record), '123456', 'expected value');
        done();
    });

    it('idFromRecord - has ! in the id', function(done) {
        var record = { index: 'id!feature!123456!654321' };
        assert.equal(utils.idFromRecord(record), '123456!654321', 'expected value');
        done();
    });

    it('idFromRecord - emoji', function(done) {
        var record = { index: 'id!feature!\u1F471' };
        assert.equal(utils.idFromRecord(record), '\u1F471', 'expected value');
        done();
    });
});


