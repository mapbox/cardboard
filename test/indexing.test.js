var _ = require('lodash');
var Cardboard = require('../');
var Pbf = require('pbf');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');

var mainTable = require('@mapbox/dynamodb-test')(require('tape'), 'cardboard', require('../lib/main-table.json'));

var config = {
    region: 'test',
    mainTable: mainTable.tableName,
    endpoint: 'http://localhost:4567'
};

var utils = require('../lib/utils');

mainTable.test('[indexing] insert', function(assert) {
    var cardboard = Cardboard(config);
    var dataset = 'default';

    cardboard.put(fixtures.nullIsland, dataset, function(err) {
        assert.equal(err, null);
        assert.end();
    });
});

mainTable.test('[indexing] insert, ! in the id is reflected properly', function(assert) {
    var cardboard = new Cardboard(config);
    var feature = {
        type: 'Feature',
        id: '1235456!654321',
        properties: {},
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    cardboard.put(feature, 'default', function(err, result) {
        assert.ifError(err, 'put feature');
        assert.equal(result.features[0].id, feature.id, 'reflects correct id');
        assert.end();
    });
});

mainTable.test('[indexing] insert, get by primary index (small feature)', function(assert) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.haitiLine, 'default', function(err, res) {
        assert.ifError(err, 'inserted');
        cardboard.get(res.features[0].id, 'default', function(err, data) {
            assert.ifError(err, 'got features');
            if (err) return assert.end(err, 'has error');
            fixtures.haitiLine.id = res.id;

            // round-trip through geobuf will always truncate coords to 6 decimal places
            var f = geobuf.decode(new Pbf(geobuf.encode(fixtures.haitiLine, new Pbf())));
            f.id = data.features[0].id;
            assert.deepEqual(data.features[0], f);
            assert.end();
        });
    });
});

mainTable.test('[indexing] insert feature with no geometry', function(assert) {
    var cardboard = Cardboard(config);
    var d = {
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err) {
        assert.ok(err, 'should return an error');
        assert.equal(err.message, 'Unlocated features can not be stored.');
        assert.end();
    });
});

mainTable.test('[indexing] insert feature with no geometry', function(assert) {
    var cardboard = Cardboard(config);
    var d = {
        properties: {},
        geometry: {
            type: 'GeometryCollection',
            geometries: []
        },
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err) {
        assert.ok(err, 'should return an error');
        assert.equal(err.message, 'The GeometryCollection geometry type is not supported.');
        assert.end();
    });
});


mainTable.test('[indexing] insert feature with no coordinates', function(assert) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {type: 'Point'},
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err) {
        assert.ok(err, 'should return an error');
        assert.equal(err.message, 'Unlocated features can not be stored.');
        assert.end();
    });
});

mainTable.test('[indexing] insert wildly precise feature', function(assert) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {
            coordinates: [
                0.987654321,
                0.123456389
            ],
            type: 'Point'
        },
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err, res) {
        assert.ifError(err, 'inserted without error');
        var key = utils.createFeatureKey('default', res.features[0].id);
        config.dyno.getItem({ Key: key}, function(err, data) {
            var item = data.Item;
            assert.ifError(err, 'got item');
            var feature = utils.decodeBuffer(item.val);

            var fLng = feature.geometry.coordinates[0].toString();
            var fLat = feature.geometry.coordinates[1].toString();

            var dLng = d.geometry.coordinates[0].toString();
            var dLat = d.geometry.coordinates[1].toString();

            assert.equal(fLng.length, 8);
            assert.equal(fLat.length, 8);

            assert.equal(fLng, dLng.slice(0, 8));
            assert.equal(fLat, dLat.slice(0, 8));

            assert.end();
        });
    });
});

mainTable.test('[indexing] insert reflects feature identical to subsequent get', function(assert) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {
            coordinates: [
                0.987654321,
                0.123456789
            ],
            type: 'Point'
        },
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err, reflected) {
        assert.ifError(err, 'put success');
        assert.ok(reflected.features.length, 'reflected a feature');
        cardboard.get(reflected.features[0].id, 'default', function(err, got) {
            assert.ifError(err, 'get success');
            assert.deepEqual(reflected, got, 'reflected feature identical');
            assert.end();
        });
    });
});

mainTable.test('[indexing] insert feature with object property', function(assert) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {
            coordinates: [
                0,
                0
            ],
            type: 'Point'
        },
        properties: {
            string: '0',
            int: 0,
            null: null,
            array: ['a', 'b', 'c'],
            object: {
                string: '0',
                int: 0,
                null: null,
                array: ['a', 'b', {foo: 'bar'}],
                object: {
                    enough: 'recursion'
                }
            }
        },
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err, res) {
        assert.ifError(err, 'inserted without error');
        cardboard.get(res.features[0].id, 'default', function(err, data) {
            assert.ifError(err, 'got item');
            d.id = res.features[0].id;
            assert.deepEqual(data.features[0], geobuf.decode(new Pbf(geobuf.encode(d, new Pbf()))));
            assert.end();
        });
    });
});

mainTable.test('[indexing] insert & update', function(assert) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.haitiLine, 'default', function(err, putResult) {
        assert.equal(err, null);

        assert.ok(putResult.features[0].id, 'got id');
        var update = _.defaults({ id: putResult.features[0].id }, fixtures.haitiLine);
        update.geometry.coordinates[0][0] = -72.588671875;

        cardboard.put(update, 'default', function(err, updateResult) {
            assert.equal(err, null);
            assert.equal(updateResult.features[0].id, putResult.features[0].id, 'same id returned');

            cardboard.get(putResult.features[0].id, 'default', function(err, getResult) {
                assert.ifError(err, 'got record');
                var f = geobuf.decode(new Pbf(geobuf.encode(update, new Pbf())));
                assert.deepEqual(getResult.features[0], f, 'expected record');
                assert.end();
            });
        });
    });
});

mainTable.test('[indexing] delete a non-extistent feature', function(assert) {
    var cardboard = Cardboard(config);
    cardboard.get('foobar', 'default', function(err, data) {
        assert.ifError(err);
        assert.equal(data.features.length, 0);
        cardboard.del('foobar', 'default', function(err, ids) {
            assert.ifError(err);
            assert.equal(ids.length, 0);
            assert.end();
        });
    });
});

mainTable.test('[indexing] insert & delete', function(assert) {
    var cardboard = Cardboard(config);
    var nullIsland = _.clone(fixtures.nullIsland);
    cardboard.put(nullIsland, 'default', function(err, putResult) {
        assert.equal(err, null);

        cardboard.get(putResult.features[0].id, 'default', function(err, fc) {
            assert.equal(err, null);
            nullIsland.id = putResult.features[0].id;
            assert.deepEqual(fc.features[0], nullIsland);
            cardboard.del(putResult.features[0].id, 'default', function(err) {
                assert.ifError(err, 'removed');
                cardboard.get(putResult.id, 'default', function(err, data) {
                    assert.ifError(err);
                    assert.equal(data.features.length, 0);
                    assert.end();
                });
            });
        });
    });
});

mainTable.test('[indexing] Insert feature with id specified by user', function(assert) {
    var cardboard = Cardboard(config);
    var haiti = _.clone(fixtures.haiti);
    haiti.id = 'doesntexist';

    cardboard.put(haiti, 'default', function(err, putResult) {
        assert.ifError(err, 'inserted');
        assert.deepEqual(putResult.features[0].id, haiti.id, 'Uses given id');
        cardboard.get(haiti.id, 'default', function(err, fc) {
            var f = geobuf.decode(new Pbf(geobuf.encode(haiti, new Pbf())));
            assert.deepEqual(fc.features[0], f, 'retrieved record');
            assert.end();
        });
    });
});


mainTable.test('[indexing] Insert with and without ids specified', function(assert) {
    var cardboard = Cardboard(config);
    var haiti = _.clone(fixtures.haiti);
    haiti.id = 'doesntexist';

    cardboard.put(utils.featureCollection([haiti, fixtures.haiti]), 'default', function(err, putResults) {
        assert.ifError(err, 'inserted features');

        cardboard.get(haiti.id, 'default', function(err, fc) {
            var f = geobuf.decode(new Pbf(geobuf.encode(haiti, new Pbf())));
            assert.deepEqual(fc.features[0], f, 'retrieved record');
            cardboard.get(putResults.features[1].id, 'default', function(err, fc) {
                var f = _.extend({ id: putResults.features[1].id }, fixtures.haiti);
                f = geobuf.decode(new Pbf(geobuf.encode(f, new Pbf())));
                assert.deepEqual(fc.features[0], f, 'retrieved record');
                assert.end();
            });
        });
    });
});

mainTable.close();
