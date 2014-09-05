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

var dyno = require('dyno')(config);

var emptyFeatureCollection = {
    type: 'FeatureCollection',
    features: []
};

var dynalite, client, db;

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
        fixtures.haitiLine.geometry.coordinates[0][0] = -73.588671875;
        cardboard.put(fixtures.haitiLine, 'default', function(err, id) {
            t.equal(err, null);
            t.equal(id, primary);
            delete fixtures.haitiLine.id;
            t.end();
        });
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
                    t.deepEqual(data, []);
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
                    t.deepEqual(data, []);
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
                    t.equal(resp.length, query.length, 'finds ' + query.length + ' data with a query');
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
                    t.equal(resp.length, query.length, 'finds ' + query.length + ' data with a query');
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
                    t.equal(resp.length, query.length, 'finds ' + query.length + ' data with a query');
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
                    t.equal(resp.length, query.length, 'finds ' + query.length + ' data with a query');
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

    q.defer(cardboard.put.bind(cardboard), fixtures.haiti, 'haiti');
    q.defer(cardboard.put.bind(cardboard), fixtures.haiti, 'haiti');
    q.defer(cardboard.put.bind(cardboard), fixtures.haitiLine, 'haiti');

    q.awaitAll(getByUserSpecifiedId)

    function getByUserSpecifiedId(err, ids){
        cardboard.getBySecondaryId(fixtures.haiti.properties.id, 'haiti', function(err, res){
            t.notOk(err, 'should not return an error');
            t.ok(res, 'should return a array of features');
            t.equal(res.length, 2);
            t.equal(res[0].val.properties.id, 'haitipolygonid');
            t.equal(res[0].val.id, ids[0]);
            t.equal(res[1].val.properties.id, 'haitipolygonid');
            t.equal(res[1].val.id, ids[1]);
            t.end();
        });
    }
});
teardown();
