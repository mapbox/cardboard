var Pbf = require('pbf');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var geobuf = require('geobuf');

var states = fs.readFileSync(path.resolve(__dirname, 'data', 'states.geojson'), 'utf8');
states = JSON.parse(states);

var setup = require('./setup');
var config = setup.config;

var utils = require('../lib/utils')(config);

describe('utils', function() {
    before(setup.setup);
    after(setup.teardown);

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
    
        var feature = utils.toDatabaseRecord(noId, 'dataset');
        
        assert.equal(feature.index, 'dataset!feature!'+utils.idFromRecord(feature), 'an id was assigned');
    
        assert.ok(feature.size, 'size was calculated');

        assert.ok(feature.val, 'geobuf was stored in the item');

        noId.id = utils.idFromRecord(feature);
        assert.deepEqual(geobuf.decode(new Pbf(feature.val)), noId, 'geobuf encoded as expected');

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
        var item = encoded;

        assert.equal(utils.idFromRecord(item), hasId.id, 'used user-assigned id');

        assert.ok(item.size, 'size was calculated');

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
        var item = encoded;

        assert.equal(utils.idFromRecord(item), numericId.id.toString(), 'used numeric user-assigned id as a string');

        assert.ok(item.size, 'size was calculated');
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
        var item = encoded;

        assert.equal(utils.idFromRecord(item), zeroId.id.toString(), 'used zero (as a string) for id');

        assert.ok(item.size, 'size was calculated');

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
        var item = encoded;
        assert.notEqual(item.index, 'dataset!feature!null', 'null id was treated as undefined');
        assert.equal(item.index, 'dataset!feature!'+utils.idFromRecord(item), 'an id was assigned');

        assert.ok(item.size, 'size was calculated');

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


