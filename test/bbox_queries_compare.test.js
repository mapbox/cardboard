var test = require('tape');
var queue = require('queue-async');
var Cardboard = require('../');
var Utils = require('../lib/utils');

var s = require('./setup');
var config = s.config;
var utils = new Utils(config);
var Dyno = require('dyno');

var points = [
    [0,0], [-120, 30], [-20, 30], [20, 30], [120, 30], [-120, -30],
    [-20, -30], [20, -30], [120, -30], [-120, 60], [-20, 60],
    [20, 60], [120, 60], [-120, -60], [-20, -60], [20, -60],[120, -60]
];

// bboxQuadkeyQuery
test('setup', s.setup);

test('bboxQuadkeyQuery scan', function(t) {
    var cardboard = Cardboard(config);
    var dyno = new Dyno(config);
    var dataset = 'default';

    var features = points.map(function(coords) {
        return {
            type: 'Feature',
            properties: {},
            geometry: {
                type: 'Point',
                coordinates: coords
            }
        };
    });

    var queries = [
        [-1, -1, 1, 1]
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
                bboxQuadkeyQuery(query, dataset, function(err, res) {
                    if (err) return callback(err);
                    t.equal(res.Items.length, 1, 'expected items');
                    t.equal(res.ScannedCount, 9, 'expected scan count');
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

    function bboxQuadkeyQuery(bbox, dataset, callback) {
        var quadkeyRange = utils.calcQuadkeyRange(bbox);

        var params = {
            IndexName: 'quadkey',
            ExpressionAttributeNames: { '#quadkey': 'quadkey', '#dataset': 'dataset' },
            ExpressionAttributeValues: {
                ':nw': quadkeyRange.nw,
                ':se': quadkeyRange.se,
                ':dataset': dataset,
                ':west': bbox[2],
                ':east': bbox[0],
                ':north': bbox[1],
                ':south': bbox[3]
            },
            KeyConditionExpression: '#dataset = :dataset and #quadkey BETWEEN :nw and :se',
            Limit: 100,
            FilterExpression: 'west <= :west and east >= :east and north >= :north and south <= :south'
        };

        dyno.query(params, function(err, res) {
            if (err) return callback(err);
            return callback(null, res);
        });
    }
});

test('teardown', s.teardown);

// bboxQuery
test('setup', s.setup);

test('bboxQuery scan', function(t) {
    var cardboard = Cardboard(config);
    var dyno = new Dyno(config);
    var dataset = 'default';

    var features = points.map(function(coords) {
        return {
            type: 'Feature',
            properties: {},
            geometry: {
                type: 'Point',
                coordinates: coords
            }
        };
    });

    var queries = [
        [-1, -1, 1, 1]
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
                bboxQuery(query, dataset, function(err, res) {
                    if (err) return callback(err);
                    t.equal(res.Items.length, 1, 'expected items');
                    t.equal(res.ScannedCount, 17, 'expected scan count');
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

    function bboxQuery(bbox, dataset, callback) {
        var params = {
            ExpressionAttributeNames: { '#id': 'id', '#dataset': 'dataset' },
            ExpressionAttributeValues: {
                ':id': 'id!',
                ':dataset': dataset,
                ':west': bbox[2],
                ':east': bbox[0],
                ':north': bbox[1],
                ':south': bbox[3]
            },
            KeyConditionExpression: '#dataset = :dataset and begins_with(#id, :id)',
            Limit: 100,
            FilterExpression: 'west <= :west and east >= :east and north >= :north and south <= :south'
        };

        dyno.query(params, function(err, res) {
            if (err) return callback(err);
            return callback(null, res);
        });
    }
});

test('teardown', s.teardown);
