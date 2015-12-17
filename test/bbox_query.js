var test = require('tape');
var queue = require('queue-async');
var Cardboard = require('../');

var s = require('./setup');
var config = s.config;

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

    q.awaitAll(function(err) {
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
        });
    }
});

test('teardown', s.teardown);

// Test findability of a linestring crossing the prime meridian west to east
// N of the equator.
//
// There's a query on either side of lon 0 and two meridian-spanning
// queries.
test('setup', s.setup);

test('query for line crossing 0 lon n of eq', function(t) {
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
        [-0.75, 0.75, -0.25, 1.25],
        [-10, -0.5, 0.5, 10],
        [0, 0, 10, 10],
        [0.25, 0.75, 0.75, 1.25],
        [-0.5, -0.5, 10, 10],
        [-1E-6, 1 - 1E-6, 1E+6, 1 + 1E+6],
        [-180, -85.05112877980659, 0, 85.0511287798066],
        [-180, -85.05112877980659, 1, 85.0511287798066]
    ];

    var q = queue();
    q.defer(cardboard.put, feature, dataset);
    q.awaitAll(function(err) {
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
        });
    }
});

test('teardown', s.teardown);

// Test findability of a linestring crossing the prime meridian west to east
// S of the equator.
//
// There's a query on either side of lon 0 and two meridian-spanning
// queries.
test('setup', s.setup);

test('query for line crossing 0 lon s of eq', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'line-query';

    var feature = {
        type: 'Feature',
        properties: {},
        geometry: {
            type: 'LineString',
            coordinates: [[-1, -1], [1, -1]]
        }};

    // all queries should return a single result
    var queries = [
        [-10, -10, 0, 0],
        [-0.75, -1.25, -0.25, -0.75],
        [-10, -9.5, 0.5, 0.5],
        [0, -10, 10, 0],
        [0.25, -1.25, 0.75, -0.75],
        [-0.5, -9.5, 10, 10],
        [-1E-6, -1 - 1E-6, 1E+6, -1 + 1E+6],
        [-180, -85.05112877980659, 0, 85.0511287798066],
        [-180, -85.05112877980659, 1, 85.0511287798066]
    ];

    var q = queue();
    q.defer(cardboard.put, feature, dataset);
    q.awaitAll(function(err) {
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
        });
    }
});

test('teardown', s.teardown);

// Test findability of a linestring near 90dW N of the equator.
test('setup', s.setup);

test('query for line near -90 lon n of eq', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'line-query';

    // tile for this feature: [ 31, 63, 7 ]
    var wanted = {
        type: 'Feature',
        properties: {},
        geometry: {
            type: 'LineString',
            coordinates: [[-92, 1], [-91, 1]]
        }};

    // tile for this feature: [0, 0, 0]
    var unwanted = {
        type: 'Feature',
        properties: {},
        geometry: {
            type: 'LineString',
            coordinates: [[-1, 1], [1, 1]]
        }};

    // all queries should return a single result
    var queries = [
        [-93, 0, -90, 2],           // [0, 0, 0] -- right answer only because
                                    // of query filters.
        [-92.5, 0.5, -90.5, 1.5]    // [ 31, 63, 7 ]
    ];

    var q = queue();
    q.defer(cardboard.put, wanted, dataset);
    q.defer(cardboard.put, unwanted, dataset);
    q.awaitAll(function(err) {
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
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('queries along antimeridian (W)', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'default';

    // Insert one feature per quadrant
    var features = [[-181, 1], [-179, 1], [-179, -1], [-181, -1]].map(function(coords) {
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
        [-190, 0, -180, 10],
        [-190, -0.5, -179.5, 10],
        [-180, 0, -170, 10],
        [-180.5, -0.5, -170, 10],
        [-180, -10, -170, 0],
        [-180.5, -10, -170, 0.5],
        [-190, -10, -180, 0],
        [-190, -10, -179.5, 0.5]
    ];

    var q = queue();

    features.forEach(function(f) {
        q.defer(cardboard.put, f, dataset);
    });

    q.awaitAll(function(err) {
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
        });
    }
});

test('paging', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'default';

    // Insert 8 features
    var features = [[1, 1], [1, 2], [2, 1], [2, 2], [-1, -1], [-1, -2], [-2, -1], [-2, -2]].map(function(coords) {
        return {
            type: 'Feature',
            properties: {},
            geometry: {
                type: 'Point',
                coordinates: coords
            }
        };
    });

    var q = queue();

    features.forEach(function(f) {
        q.defer(cardboard.put, f, dataset);
    });

    q.awaitAll(function(err) {
        t.ifError(err, 'inserted');
        runQuery();
    });

    function runQuery() {
        cardboard.bboxQuery([0,0,2,2], dataset, {maxFeatures:10}, function(err, res) {
            t.ifError(err, 'bbox paged query');
            t.equal(res.features.length, 4, ' returned 4 features');
            cardboard.bboxQuery([0,0,2,2], dataset, {maxFeatures:10, start: res.features[1].id}, function(err, res) {
                t.ifError(err, 'bbox paged query');
                t.equal(res.features.length, 2, ' returned 2 features');
                t.end();
            });
        });
    }
});

test('teardown', s.teardown);
