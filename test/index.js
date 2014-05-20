var test = require('tap').test,
    levelup = require('levelup'),
    memdown = require('memdown'),
    fs = require('fs'),
    queue = require('queue-async'),
    concat = require('concat-stream'),
    Cardboard = require('../');

var nullIsland = {
    type: 'Feature',
    geometry: {
        type: 'Point',
        coordinates: [0, 0]
    }
};

var dc = {
    type: 'Feature',
    geometry: {
        type: 'Point',
        coordinates: [
          -77.02875137329102,
          38.93337493490118
        ]
    }
};

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

    t.equal(cardboard.insert('hello', nullIsland), cardboard, '.insert');

    cardboard.dump().pipe(concat(function(data) {
        t.equal(data.length, 1, 'creates data');
        t.end();
    }));
});

test('insert & query', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    t.equal(cardboard.insert('nullisland', nullIsland), cardboard, '.insert');
    t.equal(cardboard.insert('dc', dc), cardboard, '.insert');

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

function ids(res) {
    return res.map(function(r) {
        return r.key.split('!')[2];
    });
}
