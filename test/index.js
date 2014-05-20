var test = require('tap').test,
    levelup = require('levelup'),
    memdown = require('memdown'),
    fs = require('fs'),
    concat = require('concat-stream'),
    Cardboard = require('../');

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
        t.deepEqual(data, JSON.stringify({
            type: 'FeatureCollection',
            features: []
        }), 'no results with a new database');
        t.end();
    }));
});

test('insert & dump', function(t) {
    var cardboard = new Cardboard(levelup('', { db: memdown }));

    t.equal(cardboard.insert('hello', {
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    }), cardboard, '.insert');

    cardboard.dump().pipe(concat(function(data) {
        t.equal(data.length, 1, 'creates data');
        t.end();
    }));
});

function ids(res) {
    return res.map(function(r) {
        return r.key.split('!')[2];
    });
}
