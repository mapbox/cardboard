var notOk = require('./not-ok');
var assert = require('assert');
var queue = require('queue-async');
var _ = require('lodash');
var Cardboard = require('../');
var Metadata = require('../lib/metadata');
var Utils = require('../lib/utils');
var geojsonExtent = require('geojson-extent');
var geojsonFixtures = require('geojson-fixtures');
var Pbf = require('pbf');
var geobuf = require('geobuf');

var s = require('./setup');
var config = s.config;

function featureCollection(features) {
    return {
        type: 'FeatureCollection',
        features: features || []
    };
}

function featureToGeobuf(feature) {
    return Buffer.from(geobuf.encode(feature, new Pbf()));
}

describe('metadata', function() {
    var dataset = 'metadatatest';
    var initial = {
        index: 'metadata!'+dataset, 
        dataset: dataset,
        count: 12,
        size: 1024,
        west: -10,
        south: -10,
        east: 10,
        north: 10
    };
    var metadata = null;

    beforeEach(function(done) {
        s.setup(function(err) {
          if (err) return done(err);
          metadata = Metadata(config.search, dataset);
          done();
        });
    });
    afterEach(function(done) {
        s.teardown(function(err) {
            metadata = null;
            done(err);
        });
    });

    it('metadata: get', function(done) {

        metadata.getInfo(noMetadataYet);

        function noMetadataYet(err, info) {
            assert.ifError(err, 'get non-extistent metadata');
            assert.deepEqual({}, info, 'returned blank obj when no info exists');
            config.search.putItem({Item: initial}, withMetadata);
        }

        function withMetadata(err) {
            assert.ifError(err, 'put test metadata');
            metadata.getInfo(function(err, info) {
                assert.ifError(err, 'get metadata');
                assert.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'valid metadata');
                done();
            });
        }
    });

    it('metadata: defaultInfo', function(done) {

        metadata.defaultInfo(function(err, res) {
            assert.ifError(err, 'no error when creating record');
            assert.ok(res, 'response indicates record was created');
            config.search.putItem({Item: initial}, overwrite);
        });

        function overwrite(err) {
            assert.ifError(err, 'overwrote default record');
            metadata.defaultInfo(applyDefaults);
        }

        function applyDefaults(err, res) {
            assert.ifError(err, 'no error when defaultInfo would overwrite');
            notOk(res, 'response indicates no adjustments were made');
            metadata.getInfo(checkRecord);
        }

        function checkRecord(err, info) {
            assert.ifError(err, 'got metadata');
            assert.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'existing metadata not adjusted by defaultInfo');
            done();
        }
    });

    it('metadata: adjust size or count', function(done) {

        metadata.adjustProperties({ count: 10 }, function(err) {
            assert.ifError(err, 'graceful if no metadata exists');
            metadata.getInfo(checkEmpty);
        });

        function checkRecord(attr, expected, callback) {
            metadata.getInfo(function(err, info) {
                assert.ifError(err, 'get metadata');
                assert.equal(info[attr], expected, 'expected '+attr);
                callback();
            });
        }

        function checkEmpty(err, info) {
            assert.ifError(err, 'gets empty record');
            assert.deepEqual(info, {}, 'no record created by adjustProperties routine');
            config.search.putItem({ Item: initial }, addCount);
        }

        function addCount(err) {
            assert.ifError(err, 'put metadata record');
            metadata.adjustProperties({ count: 1 }, function(err) {
                assert.ifError(err, 'incremented count by 1');
                checkRecord('count', initial.count + 1, subtractCount);
            });
        }

        function subtractCount() {
            metadata.adjustProperties({ count: -1 }, function(err) {
                assert.ifError(err, 'decrement count by 1');
                checkRecord('count', initial.count, addSize);
            });
        }

        function addSize() {
            metadata.adjustProperties({ size: 1024 }, function(err) {
                assert.ifError(err, 'incremented size by 1024');
                checkRecord('size', initial.size + 1024, subtractSize);
            });
        }

        function subtractSize() {
            metadata.adjustProperties({ size: -1024 }, function(err) {
                assert.ifError(err, 'decrement size by 1024');
                checkRecord('size', initial.size, addBoth);
            });
        }

        function addBoth() {
            metadata.adjustProperties({ count: 1, size: 1024 }, function(err) {
                assert.ifError(err, 'increment size and count');
                checkRecord('size', initial.size + 1024, function() {
                    checkRecord('count', initial.count + 1, function() {
                        done();
                    });
                });
            });
        }

    });


    it('metadata: adjust bounds', function(done) {
        var bbox = [-12.01, -9, 9, 12.01];

        metadata.adjustBounds(bbox, function(err) {
            assert.ifError(err, 'graceful if no metadata exists');
            metadata.getInfo(checkEmpty);
        });

        function checkEmpty(err, info) {
            assert.ifError(err, 'gets empty record');
            assert.deepEqual(info, {}, 'no record created by adjustBounds routine');
            config.search.putItem({Item: initial}, adjust);
        }

        function adjust(err) {
            assert.ifError(err, 'put metadata record');
            metadata.adjustBounds(bbox, adjusted);
        }

        function adjusted(err) {
            assert.ifError(err, 'adjusted bounds without error');
            metadata.getInfo(checkNewInfo);
        }

        function checkNewInfo(err, info) {
            assert.ifError(err, 'get new metadata');
            var expected = {
                index: 'metadata!' + dataset,
                dataset: dataset,
                west: initial.west < bbox[0] ? initial.west : bbox[0],
                south: initial.south < bbox[1] ? initial.south : bbox[1],
                east: initial.east > bbox[2] ? initial.east : bbox[2],
                north: initial.north > bbox[3] ? initial.north : bbox[3],
                count: initial.count,
                size: initial.size,
                minzoom: 0,
                maxzoom: 1
            };
            assert.deepEqual(_.omit(info, 'updated'), expected, 'updated metadata correctly');
            done();
        }
    });

    it('metadata: add a feature', function(done) {
        var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
        var expectedSize = featureToGeobuf(feature).length;
        var expectedBounds = geojsonExtent(feature);
        var cardboard = Cardboard(config);

        cardboard.metadata.addFeature(dataset, feature, brandNew);

        function brandNew(err) {
            assert.ifError(err, 'used feature to make new metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got metadata');
                assert.equal(info.count, 1, 'correct feature count');
                assert.equal(info.size, expectedSize, 'correct size');
                assert.equal(info.west, expectedBounds[0], 'correct west');
                assert.equal(info.south, expectedBounds[1], 'correct south');
                assert.equal(info.east, expectedBounds[2], 'correct east');
                assert.equal(info.north, expectedBounds[3], 'correct north');

                config.search.putItem({Item: initial}, replacedMetadata);
            });
        }

        function replacedMetadata(err) {
            assert.ifError(err, 'replaced metadata');
            cardboard.metadata.addFeature(dataset, feature, adjusted);
        }

        function adjusted(err) {
            assert.ifError(err, 'adjusted existing metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got metadata');
                assert.equal(info.count, initial.count + 1, 'correct feature count');
                assert.equal(info.size, initial.size + expectedSize, 'correct size');

                var expectedWest = expectedBounds[0] < initial.west ?
                    expectedBounds[0] : initial.west;
                var expectedSouth = expectedBounds[1] < initial.south ?
                    expectedBounds[1] : initial.south;
                var expectedEast = expectedBounds[2] > initial.east ?
                    expectedBounds[2] : initial.east;
                var expectedNorth = expectedBounds[3] > initial.north ?
                    expectedBounds[3] : initial.north;

                assert.equal(info.west, expectedWest, 'correct west');
                assert.equal(info.south, expectedSouth, 'correct south');
                assert.equal(info.east, expectedEast, 'correct east');
                assert.equal(info.north, expectedNorth, 'correct north');

                done();
            });
        }
    });

    it('metadata: add a feature via database record', function(done) {
        var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
        var expectedSize = featureToGeobuf(feature).length;
        var expectedBounds = geojsonExtent(feature);
        var cardboard = Cardboard(config);
        var utils = Utils(config);
        var encoded = utils.toDatabaseRecord(feature, dataset).feature;
        cardboard.metadata.addFeature(dataset, encoded, brandNew);

        function brandNew(err) {
            assert.ifError(err, 'used feature to make new metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got metadata');
                assert.equal(info.count, 1, 'correct feature count');
                assert.equal(info.size, expectedSize, 'correct size');
                assert.equal(info.west, expectedBounds[0], 'correct west');
                assert.equal(info.south, expectedBounds[1], 'correct south');
                assert.equal(info.east, expectedBounds[2], 'correct east');
                assert.equal(info.north, expectedBounds[3], 'correct north');

                config.search.putItem({Item: initial}, replacedMetadata);
            });
        }

        function replacedMetadata(err) {
            assert.ifError(err, 'replaced metadata');
            cardboard.metadata.addFeature(dataset, feature, adjusted);
        }

        function adjusted(err) {
            assert.ifError(err, 'adjusted existing metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got metadata');
                assert.equal(info.count, initial.count + 1, 'correct feature count');
                assert.equal(info.size, initial.size + expectedSize, 'correct size');

                var expectedWest = expectedBounds[0] < initial.west ?
                    expectedBounds[0] : initial.west;
                var expectedSouth = expectedBounds[1] < initial.south ?
                    expectedBounds[1] : initial.south;
                var expectedEast = expectedBounds[2] > initial.east ?
                    expectedBounds[2] : initial.east;
                var expectedNorth = expectedBounds[3] > initial.north ?
                    expectedBounds[3] : initial.north;

                assert.equal(info.west, expectedWest, 'correct west');
                assert.equal(info.south, expectedSouth, 'correct south');
                assert.equal(info.east, expectedEast, 'correct east');
                assert.equal(info.north, expectedNorth, 'correct north');

                done();
            });
        }
    });

    it('metadata: update a feature', function(done) {
        var original = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
        var edited = _.extend({ id: 'test-feature' }, geojsonFixtures.featurecollection.idaho.features[0]);
        var expectedSize = featureToGeobuf(edited).length - featureToGeobuf(original).length;
        var expectedBounds = geojsonExtent(edited);
        var cardboard = Cardboard(config);

        cardboard.metadata.updateFeature(dataset, original, edited, function(err) {
            assert.ifError(err, 'graceful exit if no metadata exists');
            cardboard.getDatasetInfo(dataset, checkEmpty);
        });

        function checkEmpty(err, info) {
            assert.ifError(err, 'gets empty record');
            assert.deepEqual(info, {}, 'no record created by updateFeature routine');
            metadata.defaultInfo(andThen);
        }

        function andThen(err) {
            assert.ifError(err, 'default metadata');
            cardboard.metadata.updateFeature(dataset, original, edited, checkInfo);
        }

        function checkInfo(err) {
            assert.ifError(err, 'updated metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got metadata');
                assert.equal(info.count, 0, 'correct feature count');
                assert.equal(info.size, expectedSize, 'correct size');
                assert.equal(info.west, expectedBounds[0], 'correct west');
                assert.equal(info.south, expectedBounds[1], 'correct south');
                assert.equal(info.east, expectedBounds[2], 'correct east');
                assert.equal(info.north, expectedBounds[3], 'correct north');
                done();
            });
        }

    });

    it('metadata: update a feature via database record', function(done) {
        var original = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
        var edited = _.extend({ id: 'test-feature' }, geojsonFixtures.featurecollection.idaho.features[0]);
        var expectedSize = featureToGeobuf(edited).length - featureToGeobuf(original).length;
        var expectedBounds = geojsonExtent(edited);
        var cardboard = Cardboard(config);

        var utils = Utils(config);
        var encodedOriginal = utils.toDatabaseRecord(original, dataset).feature;
        var encodedEdited = utils.toDatabaseRecord(edited, dataset).feature;

        cardboard.metadata.updateFeature(dataset, encodedOriginal, encodedEdited, function(err) {
            assert.ifError(err, 'graceful exit if no metadata exists');
            cardboard.getDatasetInfo(dataset, checkEmpty);
        });

        function checkEmpty(err, info) {
            assert.ifError(err, 'gets empty record');
            assert.deepEqual(info, {}, 'no record created by updateFeature routine');
            metadata.defaultInfo(andThen);
        }

        function andThen(err) {
            assert.ifError(err, 'default metadata');
            cardboard.metadata.updateFeature(dataset, original, edited, checkInfo);
        }

        function checkInfo(err) {
            assert.ifError(err, 'updated metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got metadata');
                assert.equal(info.count, 0, 'correct feature count');
                assert.equal(info.size, expectedSize, 'correct size');
                assert.equal(info.west, expectedBounds[0], 'correct west');
                assert.equal(info.south, expectedBounds[1], 'correct south');
                assert.equal(info.east, expectedBounds[2], 'correct east');
                assert.equal(info.north, expectedBounds[3], 'correct north');
                done();
            });
        }

    });

    it('metadata: remove a feature', function(done) {
        var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
        var expectedSize = featureToGeobuf(feature).length;
        var cardboard = Cardboard(config);

        cardboard.metadata.deleteFeature(dataset, feature, function(err) {
            assert.ifError(err, 'graceful exit if no metadata exists');
            cardboard.getDatasetInfo(dataset, checkEmpty);
        });

        function checkEmpty(err, info) {
            assert.ifError(err, 'gets empty record');
            assert.deepEqual(info, {}, 'no record created by adjustBounds routine');
            config.search.putItem({Item: initial}, del);
        }

        function del(err) {
            assert.ifError(err, 'put default metadata');
            cardboard.metadata.deleteFeature(dataset, feature, checkInfo);
        }

        function checkInfo(err) {
            assert.ifError(err, 'updated metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got info');
                assert.equal(info.count, initial.count - 1, 'correct feature count');
                assert.equal(info.size, initial.size - expectedSize, 'correct size');
                assert.equal(info.west, initial.west, 'correct west');
                assert.equal(info.south, initial.south, 'correct south');
                assert.equal(info.east, initial.east, 'correct east');
                assert.equal(info.north, initial.north, 'correct north');
                done();
            });
        }
    });

    it('metadata: remove a feature via database record', function(done) {
        var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
        var expectedSize = featureToGeobuf(feature).length;
        var cardboard = Cardboard(config);

        var utils = Utils(config);
        var encoded = utils.toDatabaseRecord(feature, dataset).feature;

        cardboard.metadata.deleteFeature(dataset, encoded, function(err) {
            assert.ifError(err, 'graceful exit if no metadata exists');
            cardboard.getDatasetInfo(dataset, checkEmpty);
        });

        function checkEmpty(err, info) {
            assert.ifError(err, 'gets empty record');
            assert.deepEqual(info, {}, 'no record created by adjustBounds routine');
            config.search.putItem({Item: initial}, del);
        }

        function del(err) {
            assert.ifError(err, 'put default metadata');
            cardboard.metadata.deleteFeature(dataset, feature, checkInfo);
        }

        function checkInfo(err) {
            assert.ifError(err, 'updated metadata');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'got info');
                assert.equal(info.count, initial.count - 1, 'correct feature count');
                assert.equal(info.size, initial.size - expectedSize, 'correct size');
                assert.equal(info.west, initial.west, 'correct west');
                assert.equal(info.south, initial.south, 'correct south');
                assert.equal(info.east, initial.east, 'correct east');
                assert.equal(info.north, initial.north, 'correct north');
                done();
            });
        }
    });

    it('metadata: calculate dataset info', function(done) {
        var cardboard = new Cardboard(config);
        var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
        var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });

        cardboard.batch.put(featureCollection(features), dataset, function(err, putFeatures) {
            assert.ifError(err, 'inserted');

            features = features.map(function(f, i) {
                var feature = _.defaults({ id: putFeatures.features[i].id }, f);
                return feature;
            });

            var expectedSize = features.reduce(function(memo, feature) {
                memo = memo + featureToGeobuf(feature).length;
                return memo;
            }, 0);

            var expected = {
                dataset: dataset,
                index: metadata.recordId,
                size: expectedSize,
                count: features.length,
                west: expectedBounds[0],
                south: expectedBounds[1],
                east: expectedBounds[2],
                north: expectedBounds[3],
                minzoom: 3,
                maxzoom: 11,
                editcount: 0
            };

            metadata.calculateInfo(function(err, info) {
                assert.ifError(err, 'calculated');
                assert.ok(info.updated, 'has updated date');
                assert.deepEqual(_.omit(info, 'updated'), expected, 'returned expected info');
                cardboard.getDatasetInfo(dataset, function(err, info) {
                    assert.ifError(err, 'got metadata');
                    assert.ok(info.updated, 'has updated date');
                    assert.deepEqual(_.omit(info, 'updated'), expected, 'got expected info from dynamo');
                    done();
                });
            });
        });
    });

    it('insert idaho & check metadata', function(done) {
        var cardboard = new Cardboard(config);

        var f = geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
            return f.properties.GEOID === '16049960100';
        })[0];

        queue()
            .defer(cardboard.put, f, dataset)
            .defer(metadata.addFeature, f)
            .awaitAll(inserted);

        function inserted(err) {
            if (err) console.error(err);
            notOk(err, 'no error returned');
            metadata.getInfo(checkInfo);
        }

        function checkInfo(err, info) {
            assert.ifError(err, 'got idaho metadata');
            var expected = {
                index: 'metadata!' + dataset,
                dataset: dataset,
                west: info.west,
                south: info.south,
                east: info.east,
                north: info.north,
                count: 1,
                size: info.size,
                minzoom: 0,
                maxzoom: 12,
                editcount: 1
            };
            assert.ok(info.updated, 'has updated date');
            assert.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
            done();
        }
    });

    it('insert many idaho features & check metadata', function(done) {
        var cardboard = new Cardboard(config);
        var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
        var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
        var expectedSize = features.reduce(function(memo, feature) {
            memo = memo + featureToGeobuf(feature).length;
            return memo;
        }, 0);

        var q = queue();

        features.forEach(function(block) {
            q.defer(cardboard.put, block, dataset);
            q.defer(metadata.addFeature, block);
        });

        q.awaitAll(inserted);

        function inserted(err) {
            if (err) console.error(err);
            notOk(err, 'no error returned');
            metadata.getInfo(checkInfo);
        }

        function checkInfo(err, info) {
            assert.ifError(err, 'got idaho metadata');
            var expected = {
                index: 'metadata!' + dataset,
                dataset: dataset,
                west: expectedBounds[0],
                south: expectedBounds[1],
                east: expectedBounds[2],
                north: expectedBounds[3],
                count: features.length,
                size: expectedSize,
                minzoom: 3,
                maxzoom: 11,
                editcount: 50
            };
            assert.ok(info.updated, 'has updated date');
            assert.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
            done();
        }
    });

    it('insert many idaho features, delete one & check metadata', function(done) {
        var cardboard = new Cardboard(config);
        var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
        var deleteThis = features[9];
        var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
        var expectedSize = features.reduce(function(memo, feature) {
            memo = memo + featureToGeobuf(feature).length;
            return memo;
        }, 0) - featureToGeobuf(deleteThis).length;

        var q = queue();

        features.forEach(function(block) {
            q.defer(cardboard.put, block, dataset);
            q.defer(metadata.addFeature, block);
        });

        q.defer(metadata.deleteFeature, deleteThis);
        q.awaitAll(inserted);

        function inserted(err) {
            if (err) console.error(err);
            notOk(err, 'no error returned');
            metadata.getInfo(checkInfo);
        }

        function checkInfo(err, info) {
            assert.ifError(err, 'got idaho metadata');
            var expected = {
                index: 'metadata!' + dataset,
                dataset: dataset,
                west: expectedBounds[0],
                south: expectedBounds[1],
                east: expectedBounds[2],
                north: expectedBounds[3],
                count: features.length - 1,
                size: expectedSize,
                minzoom: 3,
                maxzoom: 11,
                editcount: 51
            };
            assert.ok(info.updated, 'has updated date');
            assert.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
            done();
        }
    });

    it('insert idaho feature, update & check metadata', function(done) {
        var cardboard = new Cardboard(config);
        var original = geojsonFixtures.featurecollection.idaho.features[0];
        var edited = geojsonFixtures.featurecollection.idaho.features[1];

        var expectedSize;
        var expectedBounds = geojsonExtent({
            type: 'FeatureCollection',
            features: [original, edited]
        });

        queue()
            .defer(cardboard.put, original, dataset)
            .defer(metadata.addFeature, original)
            .awaitAll(inserted);

        function inserted(err, res) {
            notOk(err, 'no error returned');

            var update = _.extend({ id: res[0] }, edited);
            expectedSize = featureToGeobuf(update).length;
            queue()
                .defer(cardboard.put, update, dataset)
                .defer(metadata.updateFeature, original, update)
                .awaitAll(updated);
        }

        function updated(err) {
            assert.ifError(err, 'updated feature');
            metadata.getInfo(checkInfo);
        }

        function checkInfo(err, info) {
            assert.ifError(err, 'got idaho metadata');
            var expected = {
                index: 'metadata!' + dataset,
                dataset: dataset,
                west: expectedBounds[0],
                south: expectedBounds[1],
                east: expectedBounds[2],
                north: expectedBounds[3],
                count: 1,
                size: expectedSize,
                minzoom: 0,
                maxzoom: 12,
                editcount: 2
            };
            assert.ok(info.updated, 'has updated date');
            assert.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
            done();
        }
    });

    it('delDataset removes metadata', function(done) {
        var cardboard = new Cardboard(config);
        config.search.putItem({Item: initial}, function(err) {
            assert.ifError(err, 'put initial metadata');
            cardboard.delDataset(dataset, removed);
        });

        function removed(err) {
            assert.ifError(err, 'removed dataset');
            metadata.getInfo(function(err, info) {
                assert.ifError(err, 'looked for metadata');
                assert.deepEqual(info, {}, 'metadata was removed');
                done();
            });
        }
    });

    it('getDatasetInfo', function(done) {
        var cardboard = new Cardboard(config);
        config.search.putItem({Item: initial}, function(err) {
            assert.ifError(err, 'put initial metadata');
            cardboard.getDatasetInfo(dataset, checkInfo);
        });

        function checkInfo(err, info) {
            assert.ifError(err, 'got metadata');
            assert.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'metadata is correct');
            done();
        }
    });

    it('calculateDatasetInfo', function(done) {
        var cardboard = new Cardboard(config);
        var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
        var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });

        cardboard.batch.put(featureCollection(features), dataset, function(err, putFeatures) {
            assert.ifError(err, 'inserted');

            features = features.map(function(f, i) {
                var feature = _.defaults({ id: putFeatures.features[i].id }, f);
                return feature;
            });

            var expectedSize = features.reduce(function(memo, feature) {
                memo = memo + featureToGeobuf(feature).length;
                return memo;
            }, 0);

            var expected = {
                dataset: dataset,
                index: metadata.recordId,
                size: expectedSize,
                count: features.length,
                west: expectedBounds[0],
                south: expectedBounds[1],
                east: expectedBounds[2],
                north: expectedBounds[3],
                minzoom: 3,
                maxzoom: 11,
                editcount: 0
            };

            cardboard.calculateDatasetInfo(dataset, function(err, info) {
                assert.ifError(err, 'calculated');
                assert.ok(info.updated, 'has updated date');
                assert.deepEqual(_.omit(info, 'updated'), expected, 'returned expected info');
                done();
            });
        });
    });

    it('metadata.applyChanges', function(done) {
        var original = {
            dataset: dataset,
            index: 'metadata!' + dataset,
            editcount: 1,
            updated: 1471645277837,
            count: 2,
            size: 200,
            west: -5,
            south: -5,
            east: 5,
            north: 5
        };

        var changes = [
            {
                action: 'INSERT',
                new: {
                    dataset: dataset,
                    index: '1',
                    cell: '01',
                    size: 150,
                    west: -6,
                    south: -6,
                    east: 4,
                    north: 4
                }
            },
            {
                action: 'MODIFY',
                new: {
                    dataset: dataset,
                    index: '2',
                    cell: '01',
                    size: 10,
                    west: -4,
                    south: -4,
                    east: 6,
                    north: 6
                },
                old: {
                    dataset: dataset,
                    index: '2',
                    cell: '01',
                    size: 100,
                    west: -5,
                    south: -5,
                    east: 4,
                    north: 4
                }
            },
            {
                action: 'REMOVE',
                old: {
                    dataset: dataset,
                    index: '3',
                    cell: '01',
                    size: 100,
                    west: -4,
                    south: -4,
                    east: 5,
                    north: 5
                }
            }
        ];

        var expected = {
            dataset: dataset,
            index: 'metadata!dataset',
            editcount: 4,
            count: 2,
            size: 160,
            west: -6,
            south: -6,
            east: 6,
            north: 6
        };

        config.search.putItem({ Item: original }, function(err) {
            if (err) throw err;

            metadata.applyChanges(changes, function(err) {
                if (err) throw err;

                config.search.getItem({ Key: { dataset: dataset, index: 'metadata!' + dataset } }, function(err, data) {
                    if (err) throw err;

                    var info = data.Item;
                    assert.equal(info.editcount, expected.editcount, 'expected editcount');
                    assert.equal(info.count, expected.count, 'expected count');
                    assert.equal(info.size, expected.size, 'expected size');
                    assert.equal(info.west, expected.west, 'expected west');
                    assert.equal(info.south, expected.south, 'expected south');
                    assert.equal(info.east, expected.east, 'expected east');
                    assert.equal(info.north, expected.north, 'expected north');
                    done();
                });
            });
        });
    });
});
