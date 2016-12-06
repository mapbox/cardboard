var Pbf = require('pbf');
var fs = require('fs');
var path = require('path');
var geobuf = require('geobuf');

var states = fs.readFileSync(path.resolve(__dirname, 'data', 'states.geojson'), 'utf8');
states = JSON.parse(states);

var mainTable = require('dynamodb-test')(require('tape'), 'cardboard', require('../lib/main_table.json'));

var config = {
    region: 'test',
    mainTable: mainTable.tableName,
    endpoint: 'http://localhost:4567'
};
var utils = require('../lib/utils')(config);

mainTable.start();

mainTable.test('[utils] toDatabaseRecord - no ID', function(assert) {
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

    assert.end(); 
});

mainTable.test('[utils] toDatabaseRecord - with ID', function(assert) {
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
    assert.end();
});

mainTable.test('[utils] toDatabaseRecord - numeric IDs become strings', function(assert) {
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
    assert.end();
});

mainTable.test('[utils] toDatabaseRecord - zero is an acceptable ID', function(assert) {
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
    assert.end();
});

mainTable.test('[utils] toDatabaseRecord - null ID', function(assert) {
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

    assert.end();
});

mainTable.test('[utils] toDatabaseRecord - no geometry', function(assert) {
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

mainTable.test('[utils] idFromRecord - no ! in the id', function(assert) {
    var record = { index: 'id!feature!123456' };
    assert.equal(utils.idFromRecord(record), '123456', 'expected value');
    assert.end();
});

mainTable.test('[utils] idFromRecord - has ! in the id', function(assert) {
    var record = { index: 'id!feature!123456!654321' };
    assert.equal(utils.idFromRecord(record), '123456!654321', 'expected value');
    assert.end();
});

mainTable.test('[utils] idFromRecord - emoji', function(assert) {
    var record = { index: 'id!feature!\u1F471' };
    assert.equal(utils.idFromRecord(record), '\u1F471', 'expected value');
    assert.end();
});

mainTable.close();
