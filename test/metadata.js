var test = require('tape');
var fs = require('fs');
var queue = require('queue-async');
var _ = require('lodash');
var Cardboard = require('../');
var Metadata = require('../lib/metadata');
var geojsonExtent = require('geojson-extent');
var geojsonFixtures = require('geojson-fixtures');
var geojsonNormalize = require('geojson-normalize');
var geojsonStream = require('geojson-stream');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');
var fakeAWS = require('mock-aws-s3');
var crypto = require('crypto');

var s = require('./setup');
var config = s.config;
var dyno = s.dyno;

function featureCollection(features) {
    return {
        type: 'FeatureCollection',
        features: features || []
    };
}

var dataset = 'metadatatest';
var metadata = Metadata(dyno, dataset);
var initial = {
        id: metadata.recordId,
        dataset: dataset,
        count: 12,
        size: 1024,
        west: -10,
        south: -10,
        east: 10,
        north: 10
    };

test('setup', s.setup);

test('metadata: get', function(t) {

    metadata.getInfo(noMetadataYet);

    function noMetadataYet(err, info) {
        t.ifError(err, 'get non-extistent metadata');
        t.deepEqual({}, info, 'returned blank obj when no info exists');
        dyno.putItem(initial, withMetadata);
    }

    function withMetadata(err, res) {
        t.ifError(err, 'put test metadata');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'get metadata');
            t.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'valid metadata');
            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: defaultInfo', function(t) {

    metadata.defaultInfo(function(err, res) {
        t.ifError(err, 'no error when creating record');
        t.ok(res, 'response indicates record was created');
        dyno.putItem(initial, overwrite);
    });

    function overwrite(err, res) {
        t.ifError(err, 'overwrote default record');
        metadata.defaultInfo(applyDefaults);
    }

    function applyDefaults(err, res) {
        t.ifError(err, 'no error when defaultInfo would overwrite');
        t.notOk(res, 'response indicates no adjustments were made');
        metadata.getInfo(checkRecord);
    }

    function checkRecord(err, info) {
        t.ifError(err, 'got metadata');
        t.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'existing metadata not adjusted by defaultInfo');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: adjust size or count', function(t) {

    metadata.adjustProperties({ count: 10 }, function(err, res) {
        t.ifError(err, 'graceful if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkRecord(attr, expected, callback) {
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'get metadata');
            t.equal(info[attr], expected, 'expected value');
            callback();
        });
    }

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by adjustProperties routine');
        dyno.putItem(initial, addCount);
    }

    function addCount(err, res) {
        t.ifError(err, 'put metadata record');
        metadata.adjustProperties({ count: 1 }, function(err, res) {
            t.ifError(err, 'incremented count by 1');
            checkRecord('count', initial.count + 1, subtractCount);
        });
    }

    function subtractCount() {
        metadata.adjustProperties({ count: -1 }, function(err, res) {
            t.ifError(err, 'decrement count by 1');
            checkRecord('count', initial.count, addSize);
        });
    }

    function addSize() {
        metadata.adjustProperties({ size: 1024 }, function(err, res) {
            t.ifError(err, 'incremented size by 1024');
            checkRecord('size', initial.size + 1024, subtractSize);
        });
    }

    function subtractSize() {
        metadata.adjustProperties({ size: -1024 }, function(err, res) {
            t.ifError(err, 'decrement size by 1024');
            checkRecord('size', initial.size, addBoth);
        });
    }

    function addBoth() {
        metadata.adjustProperties({ count: 1, size: 1024 }, function(err, res) {
            t.ifError(err, 'increment size and count');
            checkRecord('size', initial.size + 1024, function() {
                checkRecord('count', initial.count + 1, function() {
                    t.end();
                });
            });
        });
    }

});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: adjust bounds', function(t) {
    var bbox = [-12.01, -9, 9, 12.01];

    metadata.adjustBounds(bbox, function(err) {
        t.ifError(err, 'graceful if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by adjustBounds routine');
        dyno.putItem(initial, adjust);
    }

    function adjust(err, res) {
        t.ifError(err, 'put metadata record');
        metadata.adjustBounds(bbox, adjusted);
    }

    function adjusted(err, res) {
        t.ifError(err, 'adjusted bounds without error');
        metadata.getInfo(checkNewInfo);
    }

    function checkNewInfo(err, info) {
        t.ifError(err, 'get new metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: initial.west < bbox[0] ? initial.west : bbox[0],
            south: initial.south < bbox[1] ? initial.south : bbox[1],
            east: initial.east > bbox[2] ? initial.east : bbox[2],
            north: initial.north > bbox[3] ? initial.north : bbox[3],
            count: initial.count,
            size: initial.size,
            minzoom: 8,
            maxzoom: 9
        };
        t.deepEqual(_.omit(info, 'updated'), expected, 'updated metadata correctly');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: add a feature', function(t) {
    var feature = geojsonFixtures.feature.one;
    var expectedSize = Buffer.byteLength(JSON.stringify(feature));
    var expectedBounds = geojsonExtent(feature);
    var cardboard = Cardboard(config);

    cardboard.metadata.addFeature(dataset, feature, brandNew);

    function brandNew(err) {
        t.ifError(err, 'used feature to make new metadata');
        cardboard.getDatasetInfo(dataset, function(err, info) {
            t.ifError(err, 'got metadata');
            t.equal(info.count, 1, 'correct feature count');
            t.equal(info.size, expectedSize, 'correct size');
            t.equal(info.west, expectedBounds[0], 'correct west');
            t.equal(info.south, expectedBounds[1], 'correct south');
            t.equal(info.east, expectedBounds[2], 'correct east');
            t.equal(info.north, expectedBounds[3], 'correct north');

            dyno.putItem(initial, replacedMetadata);
        });
    }

    function replacedMetadata(err) {
        t.ifError(err, 'replaced metadata');
        cardboard.metadata.addFeature(dataset, feature, adjusted);
    }

    function adjusted(err) {
        t.ifError(err, 'adjusted existing metadata');
        cardboard.getDatasetInfo(dataset, function(err, info) {
            t.ifError(err, 'got metadata');
            t.equal(info.count, initial.count + 1, 'correct feature count');
            t.equal(info.size, initial.size + expectedSize, 'correct size');

            var expectedWest = expectedBounds[0] < initial.west ?
                expectedBounds[0] : initial.west;
            var expectedSouth = expectedBounds[1] < initial.south ?
                expectedBounds[1] : initial.south;
            var expectedEast = expectedBounds[2] > initial.east ?
                expectedBounds[2] : initial.east;
            var expectedNorth = expectedBounds[3] > initial.north ?
                expectedBounds[3] : initial.north;

            t.equal(info.west, expectedWest, 'correct west');
            t.equal(info.south, expectedSouth, 'correct south');
            t.equal(info.east, expectedEast, 'correct east');
            t.equal(info.north, expectedNorth, 'correct north');

            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: update a feature', function(t) {
    var original = geojsonFixtures.feature.one;
    var edited = geojsonFixtures.featurecollection.idaho.features[0];
    var expectedSize = JSON.stringify(edited).length - JSON.stringify(original).length;
    var expectedBounds = geojsonExtent(edited);
    var cardboard = Cardboard(config);

    cardboard.metadata.updateFeature(dataset, original, edited, function(err) {
        t.ifError(err, 'graceful exit if no metadata exists');
        cardboard.getDatasetInfo(dataset, checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by updateFeature routine');
        metadata.defaultInfo(andThen);
    }

    function andThen(err) {
        t.ifError(err, 'default metadata');
        cardboard.metadata.updateFeature(dataset, original, edited, checkInfo);
    }

    function checkInfo(err) {
        t.ifError(err, 'updated metadata');
        cardboard.getDatasetInfo(dataset, function(err, info) {
            t.ifError(err, 'got metadata');
            t.equal(info.count, 0, 'correct feature count');
            t.equal(info.size, expectedSize, 'correct size');
            t.equal(info.west, expectedBounds[0], 'correct west');
            t.equal(info.south, expectedBounds[1], 'correct south');
            t.equal(info.east, expectedBounds[2], 'correct east');
            t.equal(info.north, expectedBounds[3], 'correct north');
            t.end();
        });
    }

});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: remove a feature', function(t) {
    var feature = geojsonFixtures.feature.one;
    var expectedSize = Buffer.byteLength(JSON.stringify(feature));
    var cardboard = Cardboard(config);

    cardboard.metadata.deleteFeature(dataset, feature, function(err) {
        t.ifError(err, 'graceful exit if no metadata exists');
        cardboard.getDatasetInfo(dataset, checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by adjustBounds routine');
        dyno.putItem(initial, del);
    }

    function del(err) {
        t.ifError(err, 'put default metadata');
        cardboard.metadata.deleteFeature(dataset, feature, checkInfo);
    }

    function checkInfo(err) {
        t.ifError(err, 'updated metadata');
        cardboard.getDatasetInfo(dataset, function(err, info) {
            t.ifError(err, 'got info');
            t.equal(info.count, initial.count - 1, 'correct feature count');
            t.equal(info.size, initial.size - expectedSize, 'correct size');
            t.equal(info.west, initial.west, 'correct west');
            t.equal(info.south, initial.south, 'correct south');
            t.equal(info.east, initial.east, 'correct east');
            t.equal(info.north, initial.north, 'correct north');
            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: calculate dataset info', function(t) {
    var cardboard = new Cardboard(config);
    var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
    var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });

    cardboard.batch.put(featureCollection(features), dataset, function(err, putFeatures) {
        t.ifError(err, 'inserted');

        features = features.map(function(f, i) {
            var feature = _.defaults({ id: putFeatures.features[i].id }, f);
            return feature;
        });

        var expectedSize = features.reduce(function(memo, feature) {
            memo = memo + Buffer.byteLength(JSON.stringify(feature));
            return memo;
        }, 0);

        var expected = {
            dataset: dataset,
            id: metadata.recordId,
            size: expectedSize,
            count: features.length,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            minzoom: 2,
            maxzoom: 3
        };

        metadata.calculateInfo(function(err, info) {
            t.ifError(err, 'calculated');
            t.ok(info.updated, 'has updated date');
            t.deepEqual(_.omit(info, 'updated'), expected, 'returned expected info');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                t.ifError(err, 'got metadata');
                t.ok(info.updated, 'has updated date');
                t.deepEqual(_.omit(info, 'updated'), expected, 'got expected info from dynamo');
                t.end();
            });
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert idaho & check metadata', function(t) {
    var cardboard = new Cardboard(config);
    t.pass('inserting idaho');

    var f = geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
        return f.properties.GEOID === '16049960100';
    })[0];

    var info = metadata.getFeatureInfo(f);

    queue()
        .defer(cardboard.put, f, dataset)
        .defer(metadata.addFeature, f)
        .awaitAll(inserted);

    function inserted(err, res) {
        if (err) console.error(err);
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: info.west,
            south: info.south,
            east: info.east,
            north: info.north,
            count: 1,
            size: info.size,
            minzoom: 2,
            maxzoom: 3
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert many idaho features & check metadata', function(t) {
    var cardboard = new Cardboard(config);
    var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
    var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
    var expectedSize = features.reduce(function(memo, feature) {
        memo = memo + Buffer.byteLength(JSON.stringify(feature));
        return memo;
    }, 0);

    var q = queue();

    features.forEach(function(block) {
        q.defer(cardboard.put, block, dataset);
        q.defer(metadata.addFeature, block);
    });

    q.awaitAll(inserted);

    function inserted(err, res) {
        if (err) console.error(err);
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho features');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            count: features.length,
            size: expectedSize,
            minzoom: 2,
            maxzoom: 3
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert many idaho features, delete one & check metadata', function(t) {
    var cardboard = new Cardboard(config);
    var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
    var deleteThis = features[9];
    var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
    var expectedSize = features.reduce(function(memo, feature) {
        memo = memo + JSON.stringify(feature).length;
        return memo;
    }, 0) - JSON.stringify(deleteThis).length;

    var q = queue();

    features.forEach(function(block) {
        q.defer(cardboard.put, block, dataset);
        q.defer(metadata.addFeature, block);
    });

    q.defer(metadata.deleteFeature, deleteThis);
    q.awaitAll(inserted);

    function inserted(err, res) {
        if (err) console.error(err);
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho features and deleted one');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            count: features.length - 1,
            size: expectedSize,
            minzoom: 2,
            maxzoom: 3
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert idaho feature, update & check metadata', function(t) {
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
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho feature');

        var update = _.extend({ id: res[0] }, edited);
        expectedSize = JSON.stringify(update).length;
        queue()
            .defer(cardboard.put, update, dataset)
            .defer(metadata.updateFeature, original, update)
            .awaitAll(updated);
    }

    function updated(err, res) {
        t.ifError(err, 'updated feature');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            count: 1,
            size: expectedSize,
            minzoom: 1,
            maxzoom: 2
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('delDataset removes metadata', function(t) {
    var cardboard = new Cardboard(config);
    dyno.putItem(initial, function(err) {
        t.ifError(err, 'put initial metadata');
        cardboard.delDataset(dataset, removed);
    });

    function removed(err) {
        t.ifError(err, 'removed dataset');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'looked for metadata');
            t.deepEqual(info, {}, 'metadata was removed');
            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('getDatasetInfo', function(t) {
    var cardboard = new Cardboard(config);
    dyno.putItem(initial, function(err) {
        t.ifError(err, 'put initial metadata');
        cardboard.getDatasetInfo(dataset, checkInfo);
    });

    function checkInfo(err, info) {
        t.ifError(err, 'got metadata');
        t.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'metadata is correct');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('calculateDatasetInfo', function(t) {
    var cardboard = new Cardboard(config);
    var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
    var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });

    cardboard.batch.put(featureCollection(features), dataset, function(err, putFeatures) {
        t.ifError(err, 'inserted');

        features = features.map(function(f, i) {
            var feature = _.defaults({ id: putFeatures.features[i].id }, f);
            return feature;
        });

        var expectedSize = features.reduce(function(memo, feature) {
            memo = memo + Buffer.byteLength(JSON.stringify(feature));
            return memo;
        }, 0);

        var expected = {
            dataset: dataset,
            id: metadata.recordId,
            size: expectedSize,
            count: features.length,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            minzoom: 2,
            maxzoom: 3
        };

        cardboard.calculateDatasetInfo(dataset, function(err, info) {
            t.ifError(err, 'calculated');
            t.ok(info.updated, 'has updated date');
            t.deepEqual(_.omit(info, 'updated'), expected, 'returned expected info');
            t.end();
        });
    });
});

test('teardown', s.teardown);
