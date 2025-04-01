var test = require('tape');
// var queue = require('queue-async');
var _ = require('lodash');
var Cardboard = require('../');
var Metadata = require('../lib/metadata');
// var Utils = require('../lib/utils');
var geojsonExtent = require('@mapbox/geojson-extent');
var geojsonFixtures = require('geojson-fixtures');
var geobuf = require('geobuf');
var Pbf = require('pbf');

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
// var initial = {
//     id: metadata.recordId,
//     dataset: dataset,
//     count: 12,
//     size: 1024,
//     west: -10,
//     south: -10,
//     east: 10,
//     north: 10
// };

// test('setup', s.setup);

// test('metadata: get', function(t) {

//     metadata.getInfo(noMetadataYet);

//     function noMetadataYet(err, info) {
//         t.ifError(err, 'get non-extistent metadata');
//         t.deepEqual({}, info, 'returned blank obj when no info exists');
//         dyno.putItem({Item: initial}, withMetadata);
//     }

//     function withMetadata(err) {
//         t.ifError(err, 'put test metadata');
//         metadata.getInfo(function(err, info) {
//             t.ifError(err, 'get metadata');
//             t.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'valid metadata');
//             t.end();
//         });
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: defaultInfo', function(t) {

//     metadata.defaultInfo(function(err, res) {
//         t.ifError(err, 'no error when creating record');
//         t.ok(res, 'response indicates record was created');
//         dyno.putItem({Item: initial}, overwrite);
//     });

//     function overwrite(err) {
//         t.ifError(err, 'overwrote default record');
//         metadata.defaultInfo(applyDefaults);
//     }

//     function applyDefaults(err, res) {
//         t.ifError(err, 'no error when defaultInfo would overwrite');
//         t.notOk(res, 'response indicates no adjustments were made');
//         metadata.getInfo(checkRecord);
//     }

//     function checkRecord(err, info) {
//         t.ifError(err, 'got metadata');
//         t.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'existing metadata not adjusted by defaultInfo');
//         t.end();
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: adjust size or count', function(t) {

//     metadata.adjustProperties({ count: 10 }, function(err) {
//         t.ifError(err, 'graceful if no metadata exists');
//         metadata.getInfo(checkEmpty);
//     });

//     function checkRecord(attr, expected, callback) {
//         metadata.getInfo(function(err, info) {
//             t.ifError(err, 'get metadata');
//             t.equal(info[attr], expected, 'expected value');
//             callback();
//         });
//     }

//     function checkEmpty(err, info) {
//         t.ifError(err, 'gets empty record');
//         t.deepEqual(info, {}, 'no record created by adjustProperties routine');
//         dyno.putItem({ Item: initial }, addCount);
//     }

//     function addCount(err) {
//         t.ifError(err, 'put metadata record');
//         metadata.adjustProperties({ count: 1 }, function(err) {
//             t.ifError(err, 'incremented count by 1');
//             checkRecord('count', initial.count + 1, subtractCount);
//         });
//     }

//     function subtractCount() {
//         metadata.adjustProperties({ count: -1 }, function(err) {
//             t.ifError(err, 'decrement count by 1');
//             checkRecord('count', initial.count, addSize);
//         });
//     }

//     function addSize() {
//         metadata.adjustProperties({ size: 1024 }, function(err) {
//             t.ifError(err, 'incremented size by 1024');
//             checkRecord('size', initial.size + 1024, subtractSize);
//         });
//     }

//     function subtractSize() {
//         metadata.adjustProperties({ size: -1024 }, function(err) {
//             t.ifError(err, 'decrement size by 1024');
//             checkRecord('size', initial.size, addBoth);
//         });
//     }

//     function addBoth() {
//         metadata.adjustProperties({ count: 1, size: 1024 }, function(err) {
//             t.ifError(err, 'increment size and count');
//             checkRecord('size', initial.size + 1024, function() {
//                 checkRecord('count', initial.count + 1, function() {
//                     t.end();
//                 });
//             });
//         });
//     }

// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: adjust bounds', function(t) {
//     var bbox = [-12.01, -9, 9, 12.01];

//     metadata.adjustBounds(bbox, function(err) {
//         t.ifError(err, 'graceful if no metadata exists');
//         metadata.getInfo(checkEmpty);
//     });

//     function checkEmpty(err, info) {
//         t.ifError(err, 'gets empty record');
//         t.deepEqual(info, {}, 'no record created by adjustBounds routine');
//         dyno.putItem({Item: initial}, adjust);
//     }

//     function adjust(err) {
//         t.ifError(err, 'put metadata record');
//         metadata.adjustBounds(bbox, adjusted);
//     }

//     function adjusted(err) {
//         t.ifError(err, 'adjusted bounds without error');
//         metadata.getInfo(checkNewInfo);
//     }

//     function checkNewInfo(err, info) {
//         t.ifError(err, 'get new metadata');
//         var expected = {
//             id: 'metadata!' + dataset,
//             dataset: dataset,
//             west: initial.west < bbox[0] ? initial.west : bbox[0],
//             south: initial.south < bbox[1] ? initial.south : bbox[1],
//             east: initial.east > bbox[2] ? initial.east : bbox[2],
//             north: initial.north > bbox[3] ? initial.north : bbox[3],
//             count: initial.count,
//             size: initial.size,
//             minzoom: 0,
//             maxzoom: 1
//         };
//         t.deepEqual(_.omit(info, 'updated'), expected, 'updated metadata correctly');
//         t.end();
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: add a feature', function(t) {
//     var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
//     var expectedSize = Buffer.from(geobuf.encode(feature, new Pbf())).length;
//     var expectedBounds = geojsonExtent(feature);
//     var cardboard = Cardboard(config);

//     cardboard.metadata.addFeature(dataset, feature, brandNew);

//     function brandNew(err) {
//         t.ifError(err, 'used feature to make new metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got metadata');
//             t.equal(info.count, 1, 'correct feature count');
//             t.equal(info.size, expectedSize, 'correct size');
//             t.equal(info.west, expectedBounds[0], 'correct west');
//             t.equal(info.south, expectedBounds[1], 'correct south');
//             t.equal(info.east, expectedBounds[2], 'correct east');
//             t.equal(info.north, expectedBounds[3], 'correct north');

//             dyno.putItem({Item: initial}, replacedMetadata);
//         });
//     }

//     function replacedMetadata(err) {
//         t.ifError(err, 'replaced metadata');
//         cardboard.metadata.addFeature(dataset, feature, adjusted);
//     }

//     function adjusted(err) {
//         t.ifError(err, 'adjusted existing metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got metadata');
//             t.equal(info.count, initial.count + 1, 'correct feature count');
//             t.equal(info.size, initial.size + expectedSize, 'correct size');

//             var expectedWest = expectedBounds[0] < initial.west ?
//                 expectedBounds[0] : initial.west;
//             var expectedSouth = expectedBounds[1] < initial.south ?
//                 expectedBounds[1] : initial.south;
//             var expectedEast = expectedBounds[2] > initial.east ?
//                 expectedBounds[2] : initial.east;
//             var expectedNorth = expectedBounds[3] > initial.north ?
//                 expectedBounds[3] : initial.north;

//             t.equal(info.west, expectedWest, 'correct west');
//             t.equal(info.south, expectedSouth, 'correct south');
//             t.equal(info.east, expectedEast, 'correct east');
//             t.equal(info.north, expectedNorth, 'correct north');

//             t.end();
//         });
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: add a feature via database record', function(t) {
//     var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
//     var expectedSize = Buffer.from(geobuf.encode(feature, new Pbf())).length;
//     var expectedBounds = geojsonExtent(feature);
//     var cardboard = Cardboard(config);
//     var utils = Utils(config);
//     var encoded = utils.toDatabaseRecord(feature, dataset)[0];
//     cardboard.metadata.addFeature(dataset, encoded, brandNew);

//     function brandNew(err) {
//         t.ifError(err, 'used feature to make new metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got metadata');
//             t.equal(info.count, 1, 'correct feature count');
//             t.equal(info.size, expectedSize, 'correct size');
//             t.equal(info.west, expectedBounds[0], 'correct west');
//             t.equal(info.south, expectedBounds[1], 'correct south');
//             t.equal(info.east, expectedBounds[2], 'correct east');
//             t.equal(info.north, expectedBounds[3], 'correct north');

//             dyno.putItem({Item: initial}, replacedMetadata);
//         });
//     }

//     function replacedMetadata(err) {
//         t.ifError(err, 'replaced metadata');
//         cardboard.metadata.addFeature(dataset, feature, adjusted);
//     }

//     function adjusted(err) {
//         t.ifError(err, 'adjusted existing metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got metadata');
//             t.equal(info.count, initial.count + 1, 'correct feature count');
//             t.equal(info.size, initial.size + expectedSize, 'correct size');

//             var expectedWest = expectedBounds[0] < initial.west ?
//                 expectedBounds[0] : initial.west;
//             var expectedSouth = expectedBounds[1] < initial.south ?
//                 expectedBounds[1] : initial.south;
//             var expectedEast = expectedBounds[2] > initial.east ?
//                 expectedBounds[2] : initial.east;
//             var expectedNorth = expectedBounds[3] > initial.north ?
//                 expectedBounds[3] : initial.north;

//             t.equal(info.west, expectedWest, 'correct west');
//             t.equal(info.south, expectedSouth, 'correct south');
//             t.equal(info.east, expectedEast, 'correct east');
//             t.equal(info.north, expectedNorth, 'correct north');

//             t.end();
//         });
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: update a feature', function(t) {
//     var original = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
//     var edited = _.extend({ id: 'test-feature' }, geojsonFixtures.featurecollection.idaho.features[0]);
//     var expectedSize = Buffer.from(geobuf.encode(edited, new Pbf())).length - Buffer.from(geobuf.encode(original, new Pbf())).length;
//     var expectedBounds = geojsonExtent(edited);
//     var cardboard = Cardboard(config);

//     cardboard.metadata.updateFeature(dataset, original, edited, function(err) {
//         t.ifError(err, 'graceful exit if no metadata exists');
//         cardboard.getDatasetInfo(dataset, checkEmpty);
//     });

//     function checkEmpty(err, info) {
//         t.ifError(err, 'gets empty record');
//         t.deepEqual(info, {}, 'no record created by updateFeature routine');
//         metadata.defaultInfo(andThen);
//     }

//     function andThen(err) {
//         t.ifError(err, 'default metadata');
//         cardboard.metadata.updateFeature(dataset, original, edited, checkInfo);
//     }

//     function checkInfo(err) {
//         t.ifError(err, 'updated metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got metadata');
//             t.equal(info.count, 0, 'correct feature count');
//             t.equal(info.size, expectedSize, 'correct size');
//             t.equal(info.west, expectedBounds[0], 'correct west');
//             t.equal(info.south, expectedBounds[1], 'correct south');
//             t.equal(info.east, expectedBounds[2], 'correct east');
//             t.equal(info.north, expectedBounds[3], 'correct north');
//             t.end();
//         });
//     }

// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: update a feature via database record', function(t) {
//     var original = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
//     var edited = _.extend({ id: 'test-feature' }, geojsonFixtures.featurecollection.idaho.features[0]);
//     var expectedSize = Buffer.from(geobuf.encode(edited, new Pbf())).length - Buffer.from(geobuf.encode(original, new Pbf())).length;
//     var expectedBounds = geojsonExtent(edited);
//     var cardboard = Cardboard(config);

//     var utils = Utils(config);
//     var encodedOriginal = utils.toDatabaseRecord(original, dataset)[0];
//     var encodedEdited = utils.toDatabaseRecord(edited, dataset)[0];

//     cardboard.metadata.updateFeature(dataset, encodedOriginal, encodedEdited, function(err) {
//         t.ifError(err, 'graceful exit if no metadata exists');
//         cardboard.getDatasetInfo(dataset, checkEmpty);
//     });

//     function checkEmpty(err, info) {
//         t.ifError(err, 'gets empty record');
//         t.deepEqual(info, {}, 'no record created by updateFeature routine');
//         metadata.defaultInfo(andThen);
//     }

//     function andThen(err) {
//         t.ifError(err, 'default metadata');
//         cardboard.metadata.updateFeature(dataset, original, edited, checkInfo);
//     }

//     function checkInfo(err) {
//         t.ifError(err, 'updated metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got metadata');
//             t.equal(info.count, 0, 'correct feature count');
//             t.equal(info.size, expectedSize, 'correct size');
//             t.equal(info.west, expectedBounds[0], 'correct west');
//             t.equal(info.south, expectedBounds[1], 'correct south');
//             t.equal(info.east, expectedBounds[2], 'correct east');
//             t.equal(info.north, expectedBounds[3], 'correct north');
//             t.end();
//         });
//     }

// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: remove a feature', function(t) {
//     var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
//     var expectedSize = Buffer.from(geobuf.encode(feature, new Pbf())).length
//     var cardboard = Cardboard(config);

//     cardboard.metadata.deleteFeature(dataset, feature, function(err) {
//         t.ifError(err, 'graceful exit if no metadata exists');
//         cardboard.getDatasetInfo(dataset, checkEmpty);
//     });

//     function checkEmpty(err, info) {
//         t.ifError(err, 'gets empty record');
//         t.deepEqual(info, {}, 'no record created by adjustBounds routine');
//         dyno.putItem({Item: initial}, del);
//     }

//     function del(err) {
//         t.ifError(err, 'put default metadata');
//         cardboard.metadata.deleteFeature(dataset, feature, checkInfo);
//     }

//     function checkInfo(err) {
//         t.ifError(err, 'updated metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got info');
//             t.equal(info.count, initial.count - 1, 'correct feature count');
//             t.equal(info.size, initial.size - expectedSize, 'correct size');
//             t.equal(info.west, initial.west, 'correct west');
//             t.equal(info.south, initial.south, 'correct south');
//             t.equal(info.east, initial.east, 'correct east');
//             t.equal(info.north, initial.north, 'correct north');
//             t.end();
//         });
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata: remove a feature via database record', function(t) {
//     var feature = _.extend({ id: 'test-feature' }, geojsonFixtures.feature.one);
//     var expectedSize = Buffer.from(geobuf.encode(feature, new Pbf())).length
//     var cardboard = Cardboard(config);

//     var utils = Utils(config);
//     var encoded = utils.toDatabaseRecord(feature, dataset)[0];

//     cardboard.metadata.deleteFeature(dataset, encoded, function(err) {
//         t.ifError(err, 'graceful exit if no metadata exists');
//         cardboard.getDatasetInfo(dataset, checkEmpty);
//     });

//     function checkEmpty(err, info) {
//         t.ifError(err, 'gets empty record');
//         t.deepEqual(info, {}, 'no record created by adjustBounds routine');
//         dyno.putItem({Item: initial}, del);
//     }

//     function del(err) {
//         t.ifError(err, 'put default metadata');
//         cardboard.metadata.deleteFeature(dataset, feature, checkInfo);
//     }

//     function checkInfo(err) {
//         t.ifError(err, 'updated metadata');
//         cardboard.getDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'got info');
//             t.equal(info.count, initial.count - 1, 'correct feature count');
//             t.equal(info.size, initial.size - expectedSize, 'correct size');
//             t.equal(info.west, initial.west, 'correct west');
//             t.equal(info.south, initial.south, 'correct south');
//             t.equal(info.east, initial.east, 'correct east');
//             t.equal(info.north, initial.north, 'correct north');
//             t.end();
//         });
//     }
// });

// test('teardown', s.teardown);

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
            memo = memo + Buffer.from(geobuf.encode(feature, new Pbf())).length;
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
            minzoom: 3,
            maxzoom: 11,
            editcount: 0
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

// test('setup', s.setup);

// test('insert idaho & check metadata', function(t) {
//     var cardboard = new Cardboard(config);
//     t.pass('inserting idaho');

//     var f = geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
//         return f.properties.GEOID === '16049960100';
//     })[0];

//     queue()
//         .defer(cardboard.put, f, dataset)
//         .defer(metadata.addFeature, f)
//         .awaitAll(inserted);

//     function inserted(err) {
//         if (err) console.error(err);
//         t.notOk(err, 'no error returned');
//         t.pass('inserted idaho');
//         metadata.getInfo(checkInfo);
//     }

//     function checkInfo(err, info) {
//         t.ifError(err, 'got idaho metadata');
//         var expected = {
//             id: 'metadata!' + dataset,
//             dataset: dataset,
//             west: info.west,
//             south: info.south,
//             east: info.east,
//             north: info.north,
//             count: 1,
//             size: info.size,
//             minzoom: 0,
//             maxzoom: 12,
//             editcount: 1
//         };
//         t.ok(info.updated, 'has updated date');
//         t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
//         t.end();
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('insert many idaho features & check metadata', function(t) {
//     var cardboard = new Cardboard(config);
//     var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
//     var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
//     var expectedSize = features.reduce(function(memo, feature) {
//         memo = memo + Buffer.from(geobuf.encode(feature, new Pbf())).length;
//         return memo;
//     }, 0);

//     var q = queue();

//     features.forEach(function(block) {
//         q.defer(cardboard.put, block, dataset);
//         q.defer(metadata.addFeature, block);
//     });

//     q.awaitAll(inserted);

//     function inserted(err) {
//         if (err) console.error(err);
//         t.notOk(err, 'no error returned');
//         t.pass('inserted idaho features');
//         metadata.getInfo(checkInfo);
//     }

//     function checkInfo(err, info) {
//         t.ifError(err, 'got idaho metadata');
//         var expected = {
//             id: 'metadata!' + dataset,
//             dataset: dataset,
//             west: expectedBounds[0],
//             south: expectedBounds[1],
//             east: expectedBounds[2],
//             north: expectedBounds[3],
//             count: features.length,
//             size: expectedSize,
//             minzoom: 3,
//             maxzoom: 11,
//             editcount: 50
//         };
//         t.ok(info.updated, 'has updated date');
//         t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
//         t.end();
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('insert many idaho features, delete one & check metadata', function(t) {
//     var cardboard = new Cardboard(config);
//     var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
//     var deleteThis = features[9];
//     var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
//     var expectedSize = features.reduce(function(memo, feature) {
//         memo = memo + Buffer.from(geobuf.encode(feature, new Pbf())).length
//         return memo;
//     }, 0) - Buffer.from(geobuf.encode(deleteThis, new Pbf())).length;

//     var q = queue();

//     features.forEach(function(block) {
//         q.defer(cardboard.put, block, dataset);
//         q.defer(metadata.addFeature, block);
//     });

//     q.defer(metadata.deleteFeature, deleteThis);
//     q.awaitAll(inserted);

//     function inserted(err) {
//         if (err) console.error(err);
//         t.notOk(err, 'no error returned');
//         t.pass('inserted idaho features and deleted one');
//         metadata.getInfo(checkInfo);
//     }

//     function checkInfo(err, info) {
//         t.ifError(err, 'got idaho metadata');
//         var expected = {
//             id: 'metadata!' + dataset,
//             dataset: dataset,
//             west: expectedBounds[0],
//             south: expectedBounds[1],
//             east: expectedBounds[2],
//             north: expectedBounds[3],
//             count: features.length - 1,
//             size: expectedSize,
//             minzoom: 3,
//             maxzoom: 11,
//             editcount: 51
//         };
//         t.ok(info.updated, 'has updated date');
//         t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
//         t.end();
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('insert idaho feature, update & check metadata', function(t) {
//     var cardboard = new Cardboard(config);
//     var original = geojsonFixtures.featurecollection.idaho.features[0];
//     var edited = geojsonFixtures.featurecollection.idaho.features[1];

//     var expectedSize;
//     var expectedBounds = geojsonExtent({
//         type: 'FeatureCollection',
//         features: [original, edited]
//     });

//     queue()
//         .defer(cardboard.put, original, dataset)
//         .defer(metadata.addFeature, original)
//         .awaitAll(inserted);

//     function inserted(err, res) {
//         t.notOk(err, 'no error returned');
//         t.pass('inserted idaho feature');

//         var update = _.extend({ id: res[0] }, edited);
//         expectedSize = Buffer.from(geobuf.encode(update, new Pbf())).length;
//         queue()
//             .defer(cardboard.put, update, dataset)
//             .defer(metadata.updateFeature, original, update)
//             .awaitAll(updated);
//     }

//     function updated(err) {
//         t.ifError(err, 'updated feature');
//         metadata.getInfo(checkInfo);
//     }

//     function checkInfo(err, info) {
//         t.ifError(err, 'got idaho metadata');
//         var expected = {
//             id: 'metadata!' + dataset,
//             dataset: dataset,
//             west: expectedBounds[0],
//             south: expectedBounds[1],
//             east: expectedBounds[2],
//             north: expectedBounds[3],
//             count: 1,
//             size: expectedSize,
//             minzoom: 0,
//             maxzoom: 12,
//             editcount: 2
//         };
//         t.ok(info.updated, 'has updated date');
//         t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
//         t.end();
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('delDataset removes metadata', function(t) {
//     var cardboard = new Cardboard(config);
//     dyno.putItem({Item: initial}, function(err) {
//         t.ifError(err, 'put initial metadata');
//         cardboard.delDataset(dataset, removed);
//     });

//     function removed(err) {
//         t.ifError(err, 'removed dataset');
//         metadata.getInfo(function(err, info) {
//             t.ifError(err, 'looked for metadata');
//             t.deepEqual(info, {}, 'metadata was removed');
//             t.end();
//         });
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('getDatasetInfo', function(t) {
//     var cardboard = new Cardboard(config);
//     dyno.putItem({Item: initial}, function(err) {
//         t.ifError(err, 'put initial metadata');
//         cardboard.getDatasetInfo(dataset, checkInfo);
//     });

//     function checkInfo(err, info) {
//         t.ifError(err, 'got metadata');
//         t.deepEqual(info, _.extend({ minzoom: 0, maxzoom: 1}, initial), 'metadata is correct');
//         t.end();
//     }
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('calculateDatasetInfo', function(t) {
//     var cardboard = new Cardboard(config);
//     var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
//     var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });

//     cardboard.batch.put(featureCollection(features), dataset, function(err, putFeatures) {
//         t.ifError(err, 'inserted');

//         features = features.map(function(f, i) {
//             var feature = _.defaults({ id: putFeatures.features[i].id }, f);
//             return feature;
//         });

//         var expectedSize = features.reduce(function(memo, feature) {
//             memo = memo + Buffer.from(geobuf.encode(feature, new Pbf())).length;
//             return memo;
//         }, 0);

//         var expected = {
//             dataset: dataset,
//             id: metadata.recordId,
//             size: expectedSize,
//             count: features.length,
//             west: expectedBounds[0],
//             south: expectedBounds[1],
//             east: expectedBounds[2],
//             north: expectedBounds[3],
//             minzoom: 3,
//             maxzoom: 11,
//             editcount: 0
//         };

//         cardboard.calculateDatasetInfo(dataset, function(err, info) {
//             t.ifError(err, 'calculated');
//             t.ok(info.updated, 'has updated date');
//             t.deepEqual(_.omit(info, 'updated'), expected, 'returned expected info');
//             t.end();
//         });
//     });
// });

// test('teardown', s.teardown);

// test('setup', s.setup);

// test('metadata.applyChanges', function(t) {
//     var original = {
//         dataset: dataset,
//         id: 'metadata!' + dataset,
//         editcount: 1,
//         updated: 1471645277837,
//         count: 2,
//         size: 200,
//         west: -5,
//         south: -5,
//         east: 5,
//         north: 5
//     };

//     var changes = [
//         {
//             action: 'INSERT',
//             new: {
//                 dataset: dataset,
//                 id: '1',
//                 cell: '01',
//                 size: 150,
//                 west: -6,
//                 south: -6,
//                 east: 4,
//                 north: 4
//             }
//         },
//         {
//             action: 'MODIFY',
//             new: {
//                 dataset: dataset,
//                 id: '2',
//                 cell: '01',
//                 size: 10,
//                 west: -4,
//                 south: -4,
//                 east: 6,
//                 north: 6
//             },
//             old: {
//                 dataset: dataset,
//                 id: '2',
//                 cell: '01',
//                 size: 100,
//                 west: -5,
//                 south: -5,
//                 east: 4,
//                 north: 4
//             }
//         },
//         {
//             action: 'REMOVE',
//             old: {
//                 dataset: dataset,
//                 id: '3',
//                 cell: '01',
//                 size: 100,
//                 west: -4,
//                 south: -4,
//                 east: 5,
//                 north: 5
//             }
//         }
//     ];

//     var expected = {
//         dataset: dataset,
//         id: 'metadata!dataset',
//         editcount: 4,
//         count: 2,
//         size: 160,
//         west: -6,
//         south: -6,
//         east: 6,
//         north: 6
//     };

//     dyno.putItem({ Item: original }, function(err) {
//         if (err) throw err;

//         metadata.applyChanges(changes, function(err) {
//             if (err) throw err;

//             dyno.getItem({ Key: { dataset: dataset, id: 'metadata!' + dataset } }, function(err, data) {
//                 if (err) throw err;

//                 var info = data.Item;
//                 t.equal(info.editcount, expected.editcount, 'expected editcount');
//                 t.equal(info.count, expected.count, 'expected count');
//                 t.equal(info.size, expected.size, 'expected size');
//                 t.equal(info.west, expected.west, 'expected west');
//                 t.equal(info.south, expected.south, 'expected south');
//                 t.equal(info.east, expected.east, 'expected east');
//                 t.equal(info.north, expected.north, 'expected north');
//                 t.end();
//             });
//         });
//     });
// });

// test('teardown', s.teardown);
