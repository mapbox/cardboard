var test = require('tap').test,
    levelup = require('levelup'),
    memdown = require('memdown'),
    fs = require('fs'),
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
        t.deepEqual(ids(res), ['hello']);
        t.end();
    });
});

test('countries.geojson', function(t) {
    var countries = JSON.parse(fs.readFileSync(__dirname + '/data/countries.geojson'));
    var cardboard = new Cardboard(levelup('', { db: memdown }));
    countries.features.forEach(function(feature) {
        var id = ((feature.id !== undefined) ?
            feature.id : feature.properties.id);
        cardboard.insert(id, feature);
    });
    cardboard.intersects([-96.6796875, 37.996162679728116], function(err, res) {
        t.deepEqual(ids(res), ["USA"]);
        t.end();
    });
});

function ids(res) {
    return res.map(function(r) {
        return r.key.split('!')[2];
    });
}
