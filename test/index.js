var test = require('tap').test,
    levelup = require('levelup'),
    memdown = require('memdown'),
    fs = require('fs'),
    queue = require('queue-async'),
    concat = require('concat-stream'),
    Cardboard = require('../'),
    geojsonExtent = require('geojson-extent'),
    fixtures = require('./fixtures');

var emptyFeatureCollection = {
    type: 'FeatureCollection',
    features: []
};

test('dump', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    cardboard.dump().pipe(concat(function(data) {
        t.deepEqual(data, [], 'no results with a new database');
        t.end();
    }));
});

test('dumpGeoJSON', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    cardboard.dumpGeoJSON().pipe(concat(function(data) {
        t.deepEqual(data, JSON.stringify(emptyFeatureCollection), 'no results with a new database');
        t.end();
    }));
});

test('insert & dump', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    t.equal(cardboard.insert('hello', fixtures.nullIsland), cardboard, '.insert');

    cardboard.dump().pipe(concat(function(data) {
        t.equal(data.length, 1, 'creates data');
        t.end();
    }));
});

test('insert & query', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    t.equal(cardboard.insert('nullisland', fixtures.nullIsland), cardboard, '.insert');
    t.equal(cardboard.insert('dc', fixtures.dc), cardboard, '.insert');

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

    var q = queue(1);

    queries.forEach(function(query) {
        q.defer(function(query, callback) {
            t.equal(cardboard.bboxQuery(query.query, function(err, data) {
                t.equal(err, null, 'no error for ' + query.query.join(','));
                t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                callback();
            }), undefined, '.bboxQuery');
        }, query);
    });

    q.awaitAll(function() { t.end(); });
});

test('insert polygon', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    t.equal(cardboard.insert('us', fixtures.USA), cardboard, '.insert USA');

    var queries = [
        {
            query: [-10, -10, 10, 10],
            length: 0
        },
        {
            query: [-76.0, 38.0, -79, 40],
            length: 1
        },
        {
            query: geojsonExtent(fixtures.USA),
            length: 1
        }
    ];

    var q = queue(1);

    queries.forEach(function(query) {
        q.defer(function(query, callback) {
            t.equal(cardboard.bboxQuery(query.query, function(err, data) {
                t.equal(err, null, 'no error for ' + query.query.join(','));
                t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                callback();
            }), undefined, '.bboxQuery');
        }, query);
    });

    q.awaitAll(function() { t.end(); });
});
