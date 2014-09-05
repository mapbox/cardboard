var test = require('tap').test,
    fs = require('fs'),
    queue = require('queue-async'),
    concat = require('concat-stream'),
    Cardboard = require('../'),
    geojsonExtent = require('geojson-extent'),
    geojsonFixtures = require('geojson-fixtures'),
    geojsonNormalize = require('geojson-normalize'),
    fixtures = require('./fixtures'),
    fakeAWS = require('mock-aws-s3');

var config = {
    awsKey: 'fake',
    awsSecret: 'fake',
    table: 'geo',
    endpoint: 'http://localhost:4567',
    bucket: 'test',
    prefix: 'test',
    s3: fakeAWS.S3() // only for mocking s3
};

var emptyFeatureCollection = {
    type: 'FeatureCollection',
    features: []
};

var dynalite, client, db;

var dyno = require('dyno')(config);

function setup() {
    test('setup', function(t) {
        dynalite = require('dynalite')({
            createTableMs: 0,
            updateTableMs: 0,
            deleteTableMs: 0
        });
        dynalite.listen(4567, function() {
            t.pass('dynalite listening');
            var cardboard = Cardboard(config);
            cardboard.createTable(config.table, function(err, resp){
                t.pass('table created');
                t.end();
            });
        });
    });
}

function teardown() {
    test('teardown', function(t) {
        dynalite.close(function() {
            t.end();
        });
    });
}

setup();
test('tables', function(t) {
    dyno.listTables(function(err, res) {
        t.equal(err, null);
        t.deepEqual(res, { TableNames: ['geo'] });
        t.end();
    });
});
teardown();

setup();
test('dump', function(t) {
    var cardboard = Cardboard(config);
    cardboard.dump(function(err, data) {
        t.equal(err, null);
        t.deepEqual(data.items, [], 'no results with a new database');
        t.end();
    });
});
teardown();

setup();
test('no new', function(t) {
    var cardboard = Cardboard(config);

    cardboard.dumpGeoJSON(function(err, data) {
        t.deepEqual(data, emptyFeatureCollection, 'no results with a new database');
        t.equal(err, null);
        t.end();
    });
});
teardown();

setup();
test('dumpGeoJSON', function(t) {
    var cardboard = Cardboard(config);

    cardboard.dumpGeoJSON(function(err, data) {
        t.deepEqual(data, emptyFeatureCollection, 'no results with a new database');
        t.equal(err, null);
        t.end();
    });
});
teardown();

setup();
test('insert & dump', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.dump(function(err, data) {
            t.equal(err, null);
            t.equal(data.items.length, 2, 'creates data');
            t.end();
        });
    });
});
teardown();

setup();
test('insert & get by index', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.get(primary, 'default', function(err, data) {
            t.equal(err, null);
            fixtures.nullIsland.id = primary;
            t.deepEqual(data, geojsonNormalize(fixtures.nullIsland));
            delete fixtures.nullIsland.id;
            t.end();
        });
    });
});
teardown();

setup();
test('insert & and update', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.haitiLine, 'default', function(err, primary) {
        t.equal(err, null);
        t.ok(primary, 'got id');
        t.pass('inserted');
        fixtures.haitiLine.id = primary;
        fixtures.haitiLine.geometry.coordinates[0][0] = -72.588671875;

        dyno.query({
            id: { 'BEGINS_WITH': [ 'cell!' ] },
            dataset: { 'EQ': 'default' }
        },
        { pages: 0 },
        function(err, data){
            t.equal(data.items.length, 50, 'correct num of index entries');
            updateFeature();
        })

        function updateFeature(){
            cardboard.put(fixtures.haitiLine, 'default', function(err, id) {
                t.equal(err, null);
                t.equal(id, primary);
                delete fixtures.haitiLine.id;
                dyno.query({
                    id: { 'BEGINS_WITH': [ 'cell!' ] },
                    dataset: { 'EQ': 'default' }
                },
                { pages: 0 },
                function(err, data){

                    t.equal(data.items.length, 50, 'correct num of index entries');
                    t.end();
                });
            });
        }
    });
});
teardown();

setup();
test('insert & delete', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.get(primary, 'default', function(err, data) {
            t.equal(err, null);
            fixtures.nullIsland.id = primary;
            t.deepEqual(data, geojsonNormalize(fixtures.nullIsland));
            delete fixtures.nullIsland.id;
            cardboard.delFeature(primary, 'default', function(err, data) {
                t.equal(err, null);
                cardboard.get(primary, 'default', function(err, data) {
                    t.equal(err, null);
                    t.deepEqual(data, emptyFeatureCollection);
                    t.end();
                });
            });
        });
    });
});
teardown();


setup();
test('insert & delDataset', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.get(primary, 'default', function(err, data) {
            t.equal(err, null);
            fixtures.nullIsland.id = primary;
            t.deepEqual(data, geojsonNormalize(fixtures.nullIsland));
            delete fixtures.nullIsland.id;
            cardboard.delDataset('default', function(err, data) {
                t.equal(err, null);
                cardboard.get(primary, 'default', function(err, data) {
                    t.equal(err, null);
                    t.deepEqual(data.features.length, 0);
                    t.end();
                });
            });
        });
    });
});
teardown();



setup();
test('listIds', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.get(primary, 'default', function(err, data) {
            t.equal(err, null);
            fixtures.nullIsland.id = primary;
            t.deepEqual(data, geojsonNormalize(fixtures.nullIsland));
            delete fixtures.nullIsland.id;
            cardboard.listIds('default', function(err, data) {
                t.deepEqual(data, ['cell!1!10000000001!'+primary, 'id!'+primary]);
                t.end();
            });
        });
    });
});
teardown();

setup();
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
            query: [-76.0, 38.0, -79, 40],
            length: 1
        }
    ];
    var cardboard = Cardboard(config);
    var insertQueue = queue(1);

    [fixtures.nullIsland,
    fixtures.dc].forEach(function(fix) {
        insertQueue.defer(cardboard.put.bind(cardboard), fix, 'default');
    });

    insertQueue.awaitAll(inserted);

    function inserted() {
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
        q.awaitAll(function() {
            t.end();
        });
    }
});
teardown();

setup();
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
teardown();

setup();
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
teardown();

setup();
test('insert idaho', function(t) {
    var cardboard = Cardboard(config);
    var q = queue(1);
    t.pass('inserting idaho');
    geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
        return f.properties.GEOID === '16049960100';
    }).forEach(function(block) {
        q.defer(cardboard.put.bind(cardboard), block, 'default');
    });
    q.awaitAll(inserted);

    function inserted(err, res) {
        if (err) console.error(err);
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho');
        var queries = [
            {
                query: [-115.09552001953124,45.719603972998634,-114.77691650390625,45.947330315089275],
                length: 1
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
teardown();


setup();
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
            cb()
        });
    });

    q.awaitAll(getDatasets)

    function getDatasets(){
        cardboard.listDatasets(function(err, res){
            t.notOk(err, 'should not return an error')
            t.ok(res, 'should return a array of datasets');
            t.equal(res.length, 2)
            t.equal(res[0], 'dc')
            t.equal(res[1], 'haiti')
            t.end();
        })
    }
});
teardown();

setup();
test('insert feature with user specified id.', function(t) {
    var cardboard = Cardboard(config);
    var q = queue(1);

    q.defer(cardboard.put, fixtures.haiti, 'haiti');
    q.defer(cardboard.put, fixtures.haiti, 'haiti');
    q.defer(cardboard.put, fixtures.haitiLine, 'haiti');

    q.awaitAll(getByUserSpecifiedId)

    function getByUserSpecifiedId(err, ids){
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
teardown();

setup();
test('update feature that doesnt exist.', function(t) {
    var cardboard = Cardboard(config);
    var q = queue(1);

    fixtures.haiti.id = 'doesntexist';

    cardboard.put(fixtures.haiti, 'default', failUpdate);

    function failUpdate(err, ids) {
        t.ok(err, 'should return an error');
        t.notOk(ids, 'should return an empty of ids');
        t.equal(err.message, 'Update failed. Feature does not exist');
        t.end();
    }
});
teardown();

// Metadata tests
var dataset = 'metadatatest';
var metadata = require('../lib/metadata')(dyno, dataset);
var initial = {
        id: 'metadata!' + dataset,
        dataset: dataset,
        count: 12,
        size: 1024,
        west: -10,
        south: -10,
        east: 10,
        north: 10
    };

setup();
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
        })
    }
});
teardown();

setup();
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
teardown();

setup();
test('metadata: adjust size or count', function(t) {

    metadata.adjustProperty('count', 10, function(err, res) {
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
        t.deepEqual(info, {}, 'no record created by adjustProperty routine');
        dyno.putItem(initial, addCount);
    }

    function addCount(err, res) {
        t.ifError(err, 'put metadata record');
        metadata.adjustProperty('count', 1, function(err, res) {
            t.ifError(err, 'incremented count by 1');
            checkRecord('count', initial.count + 1, subtractCount);
        });
    }

    function subtractCount() {
        metadata.adjustProperty('count', -1, function(err, res) {
            t.ifError(err, 'decrement count by 1');
            checkRecord('count', initial.count, addSize);
        });
    }

    function addSize() {
        metadata.adjustProperty('size', 1024, function(err, res) {
            t.ifError(err, 'incremented size by 1024');
            checkRecord('size', initial.size + 1024, subtractSize);
        });
    }

    function subtractSize() {
        metadata.adjustProperty('size', -1024, function(err, res) {
            t.ifError(err, 'decrement size by 1024');
            checkRecord('size', initial.size, function() {
                t.end();
            });
        });
    }

});
teardown();

setup();
test('metadata: adjust bounds', function(t) {
    var bbox = [-12, -9, 9, 12];

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
        t.deepEqual(info, expected, 'updated metadata correctly');
        t.end();
    }
});
teardown();

setup();
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
teardown();
