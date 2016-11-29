var notOk = require('./not-ok');
var assert = require('assert');
var queue = require('queue-async');
var _ = require('lodash');
var Cardboard = require('../');
var geojsonNormalize = require('geojson-normalize');
var Pbf = require('pbf');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');
var crypto = require('crypto');

var s = require('./setup');
var config = s.config;

function featureCollection(features) {
    return {
        type: 'FeatureCollection',
        features: features || []
    };
}

describe('[indexing]', function() {
    beforeEach(s.setup);
    afterEach(s.teardown);

    it('insert', function(done) {
        var cardboard = Cardboard(config);
        var dataset = 'default';

        cardboard.put(fixtures.nullIsland, dataset, function(err) {
            assert.equal(err, null);
            done();
        });
    });

    it('insert, ! in the id is reflected properly', function(done) {
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
            assert.equal(result.id, feature.id, 'reflects correct id');
            done();
        });
    });

    it('insert, get by primary index (small feature)', function(done) {
        var cardboard = Cardboard(config);

        cardboard.put(fixtures.haitiLine, 'default', function(err, res) {
            assert.ifError(err, 'inserted');
            cardboard.get(res.id, 'default', function(err, data) {
                assert.equal(err, null);
                fixtures.haitiLine.id = res.id;

                // round-trip through geobuf will always truncate coords to 6 decimal places
                var f = geobuf.decode(new Pbf(geobuf.encode(fixtures.haitiLine, new Pbf())));
                delete fixtures.haitiLine.id;
                assert.deepEqual(data, f);

                // data should not be on S3
                s.config.s3.listObjects({
                    Bucket: 'test',
                    Prefix: 'test/default/' + res.id
                }, function(err, data) {
                    assert.equal(data.Contents.length, 0, 'nothing on S3');
                    done();
                });
            });
        });
    });

    it('insert feature with no geometry', function(done) {
        var cardboard = Cardboard(config);
        var d = {
            properties: {},
            type: 'Feature'
        };

        cardboard.put(d, 'default', function(err) {
            assert.ok(err, 'should return an error');
            assert.equal(err.message, 'Unlocated features can not be stored.');
            done();
        });
    });

    it('insert feature with no geometry', function(done) {
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
            done();
        });
    });


    it('insert feature with no coordinates', function(done) {
        var cardboard = Cardboard(config);
        var d = {
            geometry: {type: 'Point'},
            properties: {},
            type: 'Feature'
        };

        cardboard.put(d, 'default', function(err) {
            assert.ok(err, 'should return an error');
            assert.equal(err.message, 'Unlocated features can not be stored.');
            done();
        });
    });

    it('insert wildly precise feature', function(done) {
        var cardboard = Cardboard(config);
        var utils = require('../lib/utils')(config);
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

        cardboard.put(d, 'default', function(err, res) {
            assert.ifError(err, 'inserted without error');
            var key = utils.createFeatureKey('default', res.id);
            config.mainTable.getItem({ Key: key}, function(err, data) {
                var item = data.Item;
                assert.ifError(err, 'got item');
                if (err) return done();
                assert.equal(item.west, 0.987654, 'correct west attr');
                assert.equal(item.east, 0.987654, 'correct east attr');
                assert.equal(item.north, 0.123457, 'correct north attr');
                assert.equal(item.south, 0.123457, 'correct south attr');
                done();
            });
        });
    });

    it('insert reflects feature identical to subsequent get', function(done) {
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
            assert.ok(reflected, 'reflected a feature');
            cardboard.get(reflected.id, 'default', function(err, got) {
                assert.ifError(err, 'get success');
                assert.deepEqual(reflected, got, 'reflected feature identical');
                done();
            });
        });
    });

    it('insert feature with object property', function(done) {
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
            cardboard.get(res.id, 'default', function(err, data) {
                assert.ifError(err, 'got item');
                d.id = res.id;
                assert.deepEqual(data, geobuf.decode(new Pbf(geobuf.encode(d, new Pbf()))));
                done();
            });
        });
    });

    it('insert & update', function(done) {
        var cardboard = Cardboard(config);

        cardboard.put(fixtures.haitiLine, 'default', function(err, putResult) {
            assert.equal(err, null);

            assert.ok(putResult.id, 'got id');
            var update = _.defaults({ id: putResult.id }, fixtures.haitiLine);
            update.geometry.coordinates[0][0] = -72.588671875;

            cardboard.put(update, 'default', function(err, updateResult) {
                assert.equal(err, null);
                assert.equal(updateResult.id, putResult.id, 'same id returned');

                cardboard.get(putResult.id, 'default', function(err, getResult) {
                    assert.ifError(err, 'got record');
                    var f = geobuf.decode(new Pbf(geobuf.encode(update, new Pbf())));
                    assert.deepEqual(getResult, f, 'expected record');
                    done();
                });
            });
        });
    });

    it('delete a non-extistent feature', function(done) {
        var cardboard = Cardboard(config);
        cardboard.get('foobar', 'default', function(err, data) {
            assert.ok(err);
            assert.equal(err.message, 'Feature foobar does not exist');
            notOk(data);
            cardboard.del('foobar', 'default', function(err) {
                assert.ok(err, 'should return an error');
                assert.equal(err.message, 'Feature does not exist');
                done();
            });
        });
    });

    it('insert & delete', function(done) {
        var cardboard = Cardboard(config);
        var nullIsland = _.clone(fixtures.nullIsland);
        cardboard.put(nullIsland, 'default', function(err, putResult) {
            assert.equal(err, null);

            cardboard.get(putResult.id, 'default', function(err, data) {
                assert.equal(err, null);
                nullIsland.id = putResult.id;
                assert.deepEqual(data, nullIsland);
                cardboard.del(putResult.id, 'default', function(err) {
                    assert.ifError(err, 'removed');
                    cardboard.get(putResult.id, 'default', function(err, data) {
                        assert.ok(err);
                        assert.equal(err.message, 'Feature ' + putResult.id + ' does not exist');
                        notOk(data);
                        done();
                    });
                });
            });
        });
    });

    it('Insert feature with id specified by user', function(done) {
        var cardboard = Cardboard(config);
        var haiti = _.clone(fixtures.haiti);
        haiti.id = 'doesntexist';

        cardboard.put(haiti, 'default', function(err, putResult) {
            assert.ifError(err, 'inserted');
            assert.deepEqual(putResult.id, haiti.id, 'Uses given id');
            cardboard.get(haiti.id, 'default', function(err, feature) {
                var f = geobuf.decode(new Pbf(geobuf.encode(haiti, new Pbf())));
                assert.deepEqual(feature, f, 'retrieved record');
                done();
            });
        });
    });


    it('Insert with and without ids specified', function(done) {
        var cardboard = Cardboard(config);
        var haiti = _.clone(fixtures.haiti);
        haiti.id = 'doesntexist';

        fcPut(cardboard, featureCollection([haiti, fixtures.haiti]), 'default', function(err, putResults) {
            assert.ifError(err, 'inserted features');

            cardboard.get(haiti.id, 'default', function(err, feature) {
                var f = geobuf.decode(new Pbf(geobuf.encode(haiti, new Pbf())));
                assert.deepEqual(feature, f, 'retrieved record');
                cardboard.get(putResults.features[1].id, 'default', function(err, feature) {
                    var f = _.extend({ id: putResults.features[1].id }, fixtures.haiti);
                    f = geobuf.decode(new Pbf(geobuf.encode(f, new Pbf())));
                    assert.deepEqual(feature, f, 'retrieved record');
                    done();
                });
            });
        });
    });

    it.skip('pre-flight feature info', function(done) {
        var cardboard = Cardboard(config);
        var haiti = _.clone(fixtures.haiti);
        var info = cardboard.metadata.featureInfo('abc', haiti);
        assert.deepEqual(info, {
            size: 59,
            bounds: [-73.388671875, 18.771115062337024, -72.1142578125, 19.80805412808859],
            west: -73.388671875,
            south: 18.771115062337024,
            east: -72.1142578125,
            north: 19.80805412808859
        }, 'expected info');
        done();
    });

});

function fcPut(cardboard, fc, dataset, cb) {
    var q = queue();
    fc.features.forEach(function(f) { 
        q.defer(function(done) {
            cardboard.put(f, dataset, done);
        });
    });

    q.awaitAll(function(err, results) {
        if (err) return cb(err);
        var fc = featureCollection(results);
        cb(null, fc);
    });
}

