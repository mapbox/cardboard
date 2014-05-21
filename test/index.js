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
    levelup('', { db: memdown }, function(err, db) {
        t.notOk(err);
        var cardboard = new Cardboard(db);

        cardboard.dump().pipe(concat(function(data) {
            t.deepEqual(data, [], 'no results with a new database');
            t.end();
        }));
    });
});

test('dumpGeoJSON', function(t) {
    levelup('', { db: memdown }, function(err, db) {
        t.notOk(err);
        var cardboard = new Cardboard(db);

        cardboard.dumpGeoJSON().pipe(concat(function(data) {
            t.deepEqual(data, JSON.stringify(emptyFeatureCollection), 'no results with a new database');
            t.end();
        }));
    });
});

test('insert & dump', function(t) {
    levelup('', { db: memdown }, function(err, db) {
        t.notOk(err);
        var cardboard = new Cardboard(db);

        cardboard.insert('hello', fixtures.nullIsland, function() {
            t.pass('inserted');
            cardboard.dump().pipe(concat(function(data) {
                t.equal(data.length, 1, 'creates data');
                t.end();
            }));
        });
    });
});

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

    levelup('', { db: memdown }, function(err, db) {
        t.notOk(err);
        var cardboard = new Cardboard(db);
        var insertQueue = queue(1);

        [['nullisland', fixtures.nullIsland],
        ['dc', fixtures.dc]].forEach(function(fix) {
            insertQueue.defer(cardboard.insert.bind(cardboard), fix[0], fix[1]);
        });

        insertQueue.awaitAll(inserted);

        function inserted() {
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
        }
    });
});

test('insert polygon', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    cardboard.insert('us', fixtures.USA, inserted);

    function inserted() {
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
    }
});
