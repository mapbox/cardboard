var test = require('tap').test,
    levelup = require('levelup'),
    memdown = require('memdown'),
    concat = require('concat-stream'),
    Cardboard = require('../');

test('Cardboard#dump', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    cardboard.dump().pipe(concat(function(data) {
        t.deepEqual(data, [], 'no results with a new database');
        t.end();
    }));
});

test('Cardboard#dumpGeoJSON', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    cardboard.dumpGeoJSON().pipe(concat(function(data) {
        t.deepEqual(data, JSON.stringify({
            type: 'FeatureCollection',
            features: []
        }), 'no results with a new database');
        t.end();
    }));
});

test('Cardboard#insert', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    t.equal(cardboard.insert('hello', {
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    }), cardboard, '.insert');

    cardboard.dumpGeoJSON().pipe(concat(function(data) {
        t.ok(data, 'creates data');
        t.end();
    }));
});

test('Cardboard#intersects', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    t.equal(cardboard.insert('hello', {
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    }), cardboard, '.insert');

    cardboard.intersects([0, 0], function(err, res) {
        t.deepEqual(res, [{
            key : "cell!10000001!hello", // != undefined
            value : "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[0,0]}}" // != undefined
        }]);
        t.end();
    });
});
