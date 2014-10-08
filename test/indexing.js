var test = require('tap').test,
    fs = require('fs'),
    queue = require('queue-async'),
    concat = require('concat-stream'),
    _ = require('lodash'),
    bufferEqual = require('buffer-equal'),
    Cardboard = require('../'),
    Metadata = require('../lib/metadata'),
    geojsonExtent = require('geojson-extent'),
    geojsonFixtures = require('geojson-fixtures'),
    geojsonNormalize = require('geojson-normalize'),
    geobuf = require('geobuf'),
    fixtures = require('./fixtures'),
    fakeAWS = require('mock-aws-s3');

var s = require('./setup');
var config = s.config;
var dyno = s.dyno;

function featureCollection(features) {
    return  {
        type: 'FeatureCollection',
        features: features || []
    };
}

test('setup', s.setup);
test('tables', function(t) {
    dyno.listTables(function(err, res) {
        t.equal(err, null);
        t.deepEqual(res, { TableNames: ['geo'] });
        t.end();
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('dump', function(t) {
    var cardboard = Cardboard(config);
    cardboard.dump(function(err, items) {
        t.equal(err, null);
        t.deepEqual(items, [], 'no results with a new database');
        t.end();
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert & dump', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'default';

    cardboard.put(fixtures.nullIsland, dataset, function(err, res) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.dump(function(err, data) {
            t.equal(err, null);
            t.equal(data.length, 1, 'creates data');
            t.end();
        });
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert, get by primary index', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.haitiLine, 'default', function(err, res) {
        t.ifError(err, 'inserted');
        cardboard.get(res, 'default', function(err, data) {
            t.equal(err, null);
            fixtures.haitiLine.id = res;
            // round-trip through geobuf will always truncate coords to 6 decimal places
            var f = geobuf.geobufToFeature(geobuf.featureToGeobuf(fixtures.haitiLine).toBuffer());
            delete fixtures.haitiLine.id;
            t.deepEqual(data, geojsonNormalize(f));
            t.end();
        });
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert, get by secondary index', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(
        featureCollection([
            _.clone(fixtures.haiti),
            _.clone(fixtures.haiti),
            _.clone(fixtures.haitiLine)
        ]), 'haiti', get);

    function get(err, ids) {
       t.ifError(err, 'inserted');
       cardboard.getBySecondaryId(fixtures.haiti.properties.id, 'haiti', function(err, res){
           t.notOk(err, 'should not return an error');
           t.ok(res, 'should return a array of features');
           t.equal(res.features.length, 2);
           t.equal(res.features[0].properties.id, 'haitipolygonid');
           t.equal(res.features[0].id, ids[0]);
           t.equal(res.features[1].properties.id, 'haitipolygonid');
           t.equal(res.features[1].id, ids[1]);
           t.end();
       });
    }
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert & update', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.haitiLine, 'default', function(err, primary) {
        t.equal(err, null);

        t.ok(primary, 'got id');
        t.pass('inserted');
        update = _.defaults({ id: primary[0] }, fixtures.haitiLine);
        update.geometry.coordinates[0][0] = -72.588671875;

        cardboard.put(update, 'default', function(err, res) {
            t.equal(err, null);
            t.equal(res[0], primary[0], 'same id returned');

            cardboard.get(primary, 'default', function(err, data) {
                t.ifError(err, 'got record');
                var f = geobuf.geobufToFeature(geobuf.featureToGeobuf(update).toBuffer());
                t.deepEqual(data.features[0], f, 'expected record');
                t.end();
            });
        });
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert & delete', function(t) {
    var cardboard = Cardboard(config);
    var nullIsland = _.clone(fixtures.nullIsland);
    cardboard.put(nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');

        cardboard.get(primary, 'default', function(err, data) {
            t.equal(err, null);
            nullIsland.id = primary[0];
            t.deepEqual(data, geojsonNormalize(nullIsland));
            cardboard.del(primary, 'default', function(err, data) {
                t.ifError(err, 'removed');
                cardboard.get(primary[0], 'default', function(err, data) {
                    t.equal(err, null);
                    t.deepEqual(data, featureCollection());
                    t.end();
                });
            });
        });
    });
});
test('teardown', s.teardown);


test('setup', s.setup);
test('insert & delDataset', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.get(primary, 'default', function(err, data) {
            t.equal(err, null);
            var nullIsland = _.clone(fixtures.nullIsland)
            nullIsland.id = primary[0];
            t.deepEqual(data, geojsonNormalize(nullIsland));
            cardboard.delDataset('default', function(err, data) {
                t.equal(err, null);
                cardboard.get(primary[0], 'default', function(err, data) {
                    t.equal(err, null);
                    t.deepEqual(data.features.length, 0);
                    t.end();
                });
            });
        });
    });
});
test('teardown', s.teardown);



test('setup', s.setup);
test('listIds', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');

        cardboard.listIds('default', function(err, data) {
            var expected = [ 'id!' + primary ];
            t.deepEqual(data, expected);
            t.end();
        });
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('export', function(t) {
    var cardboard = new Cardboard(config);
    var first = geojsonFixtures.featurecollection.idaho.features.slice(0, 10);
    var second = geojsonFixtures.featurecollection.idaho.features.slice(10, 20);

    var q = queue();
    first.forEach(function(f) {
        q.defer(cardboard.put, f, 'first');
    });
    second.forEach(function(f) {
        q.defer(cardboard.put, f, 'second');
    });
    q.defer(cardboard.calculateDatasetInfo, 'first');
    q.defer(cardboard.calculateDatasetInfo, 'second');
    q.awaitAll(function(err, ids) {
        t.ifError(err, 'inserted and calc metadata');
        first = first.map(function(f, i) {
            return _.defaults({ id: ids[0] }, f);
        });
        second = second.map(function(f, i) {
            return _.defaults({ id: ids[i + 10] }, f);
        });
        var expected = { type: 'FeatureCollection', features: _.union(first, second) };
        var found = '';
        cardboard.export()
            .on('data', function(chunk) { found = found + chunk; })
            .on('error', function(err) {
                t.notOk(err, 'stream error');
                t.end();
            })
            .on('end', function() {
                found = JSON.parse(found);
                t.equal(found.features.length, expected.features.length, 'expected number of features');
                t.end();
            });
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert & query', function(t) {
    var queries = [
        {
            query: [-10, -10, 10, 10],
            length: 1
        },
        {
            query: [30, 30, 40, 40],
            length: 0
        },
        {
            query: [10, 10, 20, 20],
            length: 0
        },
        {
            query: [-79.0, 38.0, -76, 40],
            length: 1
        }
    ];
    var cardboard = Cardboard(config);
    var insertQueue = queue(1);

    [fixtures.nullIsland,
    fixtures.dc].forEach(function(fix) {
        insertQueue.defer(cardboard.put, fix, 'default');
    });

    insertQueue.awaitAll(function(err, res) {
        t.ifError(err, 'inserted');
        inserted();
    });

    function inserted() {
        var q = queue(1);
        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, resp) {
                    t.ifError(err, 'no error for ' + query.query.join(','));
                    if (err) return callback(err);

                    t.equal(resp.features.length, query.length, query.query.join(',') + ' finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function(err) {
            t.ifError(err, 'queries passed');
            t.equal(cardboard.list('default', function(err, resp) {
                t.ifError(err, 'no error for list');
                if (err) return callback(err);

                var length = queries.reduce(function(memo, query) {
                    return memo + query.length;
                }, 0);
                t.equal(resp.features.length, length, 'list finds ' + length + ' data with a query');
                t.end();
            }), undefined, '.list');

        });
    }
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert polygon', function(t) {
    var cardboard = Cardboard(config);
    cardboard.put(fixtures.haiti, 'default', inserted);

    function inserted(err, res) {
        t.notOk(err, 'no error returned');
        var queries = [
            {
                query: [-10, -10, 10, 10],
                length: 0
            },
            {
                query: [-76.0, 38.0, -79, 40],
                length: 0
            }
        ];
        var q = queue(1);
        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, resp) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(resp.features.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() { t.end(); });
    }
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert linestring', function(t) {
    var cardboard = Cardboard(config);
    cardboard.put(fixtures.haitiLine, 'default', inserted);

    function inserted(err, res) {
        t.notOk(err, 'no error returned');
        var queries = [
            {
                query: [-10, -10, 10, 10],
                length: 0
            },
            {
                query: [-76.0, 38.0, -79, 40],
                length: 0
            }
        ];
        var q = queue(1);
        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, resp) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(resp.features.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() { t.end(); });
    }
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert idaho', function(t) {
    var cardboard = Cardboard(config);
    t.pass('inserting idaho');

    var idaho = geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
        return f.properties.GEOID === '16049960100';
    })[0];
    cardboard.put(idaho, 'default', function(err) {
        t.ifError(err, 'inserted');
        if (err) return t.end();

        var bbox = [-115.09552001953124,45.719603972998634,-114.77691650390625,45.947330315089275];
        cardboard.bboxQuery(bbox, 'default', function(err, res) {
            t.ifError(err, 'no error for ' + bbox.join(','));
            t.equal(res.features.length, 1, 'finds 1 data with a query');
            t.end();
        });
    });
});
test('teardown', s.teardown);

test('setup', s.setup);
test('insert datasets and listDatasets', function(t) {
    var cardboard = Cardboard(config);
    var q = queue(1);
    q.defer(function(cb) {
        cardboard.put(fixtures.haiti, 'haiti', function(){
            cb();
        });
    });
    q.defer(function(cb) {
        cardboard.put(fixtures.dc, 'dc', function(){
            cb();
        });
    });

    q.awaitAll(getDatasets);

    function getDatasets(){
        cardboard.listDatasets(function(err, res){
            t.notOk(err, 'should not return an error');
            t.ok(res, 'should return a array of datasets');
            t.equal(res.length, 2);
            t.equal(res[0], 'dc');
            t.equal(res[1], 'haiti');
            t.end();
        });
    }
});
test('teardown', s.teardown);

test('setup', s.setup);
test('update feature that doesnt exist.', function(t) {
    var cardboard = Cardboard(config);
    var haiti = _.clone(fixtures.haiti)
    haiti.id = 'doesntexist';

    cardboard.put(haiti, 'default', failUpdate);

    function failUpdate(err, id) {
        t.ok(err, 'should return an error');
        t.equal(err.message, 'Feature does not exist');
        t.end();
    }
});
test('teardown', s.teardown);

test('setup', s.setup);
test('mix of failing update and an insert', function(t) {
    var cardboard = Cardboard(config);
    var haiti = _.clone(fixtures.haiti)
    haiti.id = 'doesntexist';

    cardboard.put(featureCollection([haiti, fixtures.haiti]), 'default', failUpdate);

    function failUpdate(err, id) {
        t.ok(err, 'should return an error');
        t.equal(err.message, 'Feature does not exist');

        cardboard.dump(function(err, items) {
            t.equal(err, null);
            t.deepEqual(items, [], 'nothing should have been put in the database');
            t.end();
        });
    }

});
test('teardown', s.teardown);

// Metadata tests
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
            t.deepEqual(info, initial, 'valid metadata');
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
        t.deepEqual(info, initial, 'existing metadata not adjusted by defaultInfo');
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
            size: initial.size
        };
        t.deepEqual(_.omit(info, 'updated'), expected, 'updated metadata correctly');
        t.end();
    }
});
test('teardown', s.teardown);

test('setup', s.setup);
test('metadata: add a feature', function(t) {
    var feature = geojsonFixtures.feature.one;
    var expectedSize = JSON.stringify(feature).length;
    var expectedBounds = geojsonExtent(feature);

    metadata.addFeature(feature, brandNew);

    function brandNew(err) {
        t.ifError(err, 'used feature to make new metadata');
        metadata.getInfo(function(err, info) {
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
        metadata.addFeature(feature, adjusted);
    }

    function adjusted(err) {
        t.ifError(err, 'adjusted existing metadata');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'got metadata');
            t.equal(info.count, initial.count + 1, 'correct feature count');
            t.equal(info.size, initial.size + expectedSize, 'correct size');

            var expectedWest = expectedBounds[0] < initial.west ?
                    expectedBounds[0] : initial.west,
                expectedSouth = expectedBounds[1] < initial.south ?
                    expectedBounds[1] : initial.south,
                expectedEast = expectedBounds[2] > initial.east ?
                    expectedBounds[2] : initial.east,
                expectedNorth = expectedBounds[3] > initial.north ?
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

    metadata.updateFeature(original, edited, function(err) {
        t.ifError(err, 'graceful exit if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by updateFeature routine');
        metadata.defaultInfo(andThen);
    }

    function andThen(err) {
        t.ifError(err, 'default metadata');
        metadata.updateFeature(original, edited, checkInfo);
    }

    function checkInfo(err) {
        t.ifError(err, 'updated metadata');
        metadata.getInfo(function(err, info) {
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
    var expectedSize = JSON.stringify(feature).length;

    metadata.deleteFeature(feature, function(err) {
        t.ifError(err, 'graceful exit if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by adjustBounds routine');
        dyno.putItem(initial, del);
    }

    function del(err) {
        t.ifError(err, 'put default metadata');
        metadata.deleteFeature(feature, checkInfo);
    }

    function checkInfo(err) {
        t.ifError(err, 'updated metadata');
        metadata.getInfo(function(err, info) {
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

    cardboard.put(featureCollection(features), dataset, function(err, ids) {
        t.ifError(err, 'inserted');

        features = features.map(function(f, i) {
            var feature = _.defaults({ id: ids[i] }, f);
            return feature;
        });

        var expectedSize = features.reduce(function(memo, feature) {
            memo = memo + JSON.stringify(feature).length;
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
            north: expectedBounds[3]
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
            id : "metadata!" + dataset,
            dataset : dataset,
            west : info.west,
            south : info.south,
            east : info.east,
            north : info.north,
            count : 1,
            size : info.size
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
        memo = memo + JSON.stringify(feature).length;
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
          id : "metadata!" + dataset,
          dataset : dataset,
          west : expectedBounds[0],
          south : expectedBounds[1],
          east : expectedBounds[2],
          north : expectedBounds[3],
          count : features.length,
          size : expectedSize
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
          id : "metadata!" + dataset,
          dataset : dataset,
          west : expectedBounds[0],
          south : expectedBounds[1],
          east : expectedBounds[2],
          north : expectedBounds[3],
          count : features.length - 1,
          size : expectedSize
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
    var edited = geojsonFixtures.feature.one;

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
          id : "metadata!" + dataset,
          dataset : dataset,
          west : expectedBounds[0],
          south : expectedBounds[1],
          east : expectedBounds[2],
          north : expectedBounds[3],
          count : 1,
          size : expectedSize
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
        t.deepEqual(info, initial, 'metadata is correct');
        t.end();
    }
});
test('teardown', s.teardown);
