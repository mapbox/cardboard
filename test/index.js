var test = require('tap').test,
    fs = require('fs'),
    queue = require('queue-async'),
    concat = require('concat-stream'),
    Cardboard = require('../'),
    geojsonExtent = require('geojson-extent'),
    geojsonFixtures = require('geojson-fixtures'),
    fixtures = require('./fixtures');

var config = {
    awsKey: 'fake',
    awsSecret: 'fake',
    table: 'geo',
    endpoint: 'http://localhost:4567'
};

var dyno = require('dyno')(config);

var emptyFeatureCollection = {
    type: 'FeatureCollection',
    features: []
};

var dynalite, client, db;

function setup(t) {
    test('setup', function(t) {
        dynalite = require('dynalite')({
            createTableMs: 0,
            updateTableMs: 0,
            deleteTableMs: 0
        });
        dynalite.listen(4567, function() {
            t.pass('dynalite listening');
            var cardboard = new Cardboard(config);
            cardboard.createTable(config.table, function(err, resp){
                t.pass('table created');
                t.end();
            });
        });
    });
}

function teardown(cb) {
    test('teardown', function(t) {
        dynalite.close(function() {
            t.end();
        });
    });
}

setup(test);
test('tables', function(t) {
    dyno.listTables(function(err, res) {
        t.equal(err, null);
        t.deepEqual(res, { TableNames: ['geo'] });
        t.end();
    });
});
teardown(test);

setup(test);
test('dump', function(t) {
    var cardboard = new Cardboard(config);
    cardboard.dump(function(err, data) {
        t.equal(err, null);
        t.deepEqual(data.items, [], 'no results with a new database');
        t.end();
    });
});
teardown(test);

setup(test);
test('dumpGeoJSON', function(t) {
    var cardboard = new Cardboard(config);

    cardboard.dumpGeoJSON(function(err, data) {
        t.deepEqual(data, emptyFeatureCollection, 'no results with a new database');
        t.equal(err, null);
        t.end();
    });
});
teardown(test);

setup(test);
test('insert & dump', function(t) {
    var cardboard = new Cardboard(config);

    cardboard.insert('hello', fixtures.nullIsland, 'default', function(err) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.dump(function(err, data) {
            t.equal(err, null);
            t.equal(data.items.length, 2, 'creates data');
            t.end();
        });
    });
});
teardown(test);

setup(test);
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
    var cardboard = new Cardboard(config);
    var insertQueue = queue(1);

    [['nullisland', fixtures.nullIsland],
    ['dc', fixtures.dc]].forEach(function(fix) {
        insertQueue.defer(cardboard.insert.bind(cardboard), fix[0], fix[1], 'default');
    });

    insertQueue.awaitAll(inserted);

    function inserted() {
        var q = queue(1);
        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, data) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() {
            t.end();
        });
    }
});
teardown(test);

setup(test);
test('insert polygon', function(t) {
    var cardboard = new Cardboard(config);
    cardboard.insert('us', fixtures.haiti, 'default', inserted);

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
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, data) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() { t.end(); });
    }
});
teardown(test);

setup(test);
test('insert linestring', function(t) {
    var cardboard = new Cardboard(config);
    cardboard.insert('us', fixtures.haitiLine, 'default', inserted);

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
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, data) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() { t.end(); });
    }
});
teardown(test);

setup(test);
test('insert idaho', function(t) {
    var cardboard = new Cardboard(config);
    var q = queue(1);
    t.pass('inserting idaho');
    var i = 0;
    geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
        return f.properties.GEOID === '16049960100';
    }).forEach(function(block) {
        q.defer(cardboard.insert.bind(cardboard), (i++).toString(), block, 'default');
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
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, data) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() { t.end(); });
    }
});
teardown(test);
