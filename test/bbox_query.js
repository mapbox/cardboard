var test = require('tap').test,
    queue = require('queue-async'),
    Cardboard = require('../');

var s = require('./setup');
var config = s.config;
var dyno = s.dyno;

test('setup', s.setup);
test('queries along 0 lat/lon', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'default';

    // Insert one feature per quadrant
    var features = [[-1, 1], [1, 1], [1, -1], [-1, -1]].map(function(coords) {
        return {
            type: 'Feature',
            properties: {},
            geometry: {
                type: 'Point',
                coordinates: coords
            }
        };
    });

    // Query in each quadrant + queries that just cross quadrant bounds
    // all queries should return a single result
    var queries = [
        [-10, 0, 0, 10],
        [-10, -0.5, 0.5, 10],
        [0, 0, 10, 10],
        [-0.5, -0.5, 10, 10],
        [0, -10, 10, 0],
        [-0.5, -10, 10, 0.5],
        [-10, -10, 0, 0],
        [-10, -10, 0.5, 0.5]
    ];

    var q = queue();
    features.forEach(function(f) {
        q.defer(cardboard.put, f, dataset);
    });
    q.awaitAll(function(err, res) {
        t.ifError(err, 'inserted');
        runQueries();
    });

    function runQueries() {
        var q = queue();
        queries.forEach(function(query) {
            function deal(callback) {
                cardboard.bboxQuery(query, dataset, function(err, res) {
                    if (err) return callback(err);
                    t.equal(res.features.length, 1, query.join(',') + ' returned one feature');
                    callback();
                });
            }
            q.defer(deal);
        });
        q.await(function(err) {
            t.ifError(err, 'passed queries');
            t.end();
        })
    }
});
test('teardown', s.teardown);

// Test findability of a linestring crossing the prime meridian.
//
// There's a query on either side of lon 0 and two meridian-spanning
// queries.
test('setup', s.setup);
test('query for line crossing 0 lon', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'line-query';

    var feature = {
            type: 'Feature',
            properties: {},
            geometry: {
                type: 'LineString',
                coordinates: [[-1, 1], [1, 1]]
            }};

    // all queries should return a single result
    var queries = [
        [-10, 0, 0, 10],
        [-10, -0.5, 0.5, 10],
        [0, 0, 10, 10],
        [-0.5, -0.5, 10, 10],
    ];

    var q = queue();
    q.defer(cardboard.put, feature, dataset);
    q.awaitAll(function(err, res) {
        t.ifError(err, 'inserted');
        runQueries();
    });

    function runQueries() {
        var q = queue();
        queries.forEach(function(query) {
            function deal(callback) {
                cardboard.bboxQuery(query, dataset, function(err, res) {
                    if (err) return callback(err);
                    t.equal(res.features.length, 1, query.join(',') + ' returned one feature');
                    callback();
                });
            }
            q.defer(deal);
        });
        q.await(function(err) {
            t.ifError(err, 'passed queries');
            t.end();
        })
    }
});
test('teardown', s.teardown);

// Test findability of a linestring along the prime meridian.
//
// These queries are intended to be hard to get right.
test('setup', s.setup);
test('query for line from 0,0 to 0,1', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'line-query';

    var feature = {
            type: 'Feature',
            properties: {},
            geometry: {
                type: 'LineString',
                coordinates: [[0, 0], [0, 1]]
            }};

    // all queries should return a single result.
    var queries = [
        [-1, -1, 0, 0],     // touches sw corner of line's bbox
        [0, -1, 1, 0],      // touches se corner of line's bbox
        [-1, 0.2, 0, 0.4],  // touches w edge of line's bbox
        [0, 0.2, 1, 0.4],   // touches e edge of line's bbox
    ];

    var q = queue();
    q.defer(cardboard.put, feature, dataset);
    q.awaitAll(function(err, res) {
        t.ifError(err, 'inserted');
        runQueries();
    });

    function runQueries() {
        var q = queue();
        queries.forEach(function(query) {
            function deal(callback) {
                cardboard.bboxQuery(query, dataset, function(err, res) {
                    if (err) return callback(err);
                    t.equal(res.features.length, 1, query.join(',') + ' returned one feature');
                    callback();
                });
            }
            q.defer(deal);
        });
        q.await(function(err) {
            t.ifError(err, 'passed queries');
            t.end();
        })
    }
});
test('teardown', s.teardown);

