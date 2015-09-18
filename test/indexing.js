var test = require('tape');
var queue = require('queue-async');
var _ = require('lodash');
var Cardboard = require('../');
var geojsonFixtures = require('geojson-fixtures');
var geojsonNormalize = require('geojson-normalize');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');
var crypto = require('crypto');

var s = require('./setup');
var config = s.config;
var dyno = s.dyno;

function featureCollection(features) {
    return {
        type: 'FeatureCollection',
        features: features || []
    };
}

test('setup', s.setup);

test('tables', function(t) {
    dyno.listTables(function(err, res) {
        t.equal(err, null);
        t.deepEqual(res, { TableNames: ['geo'] });
        t.end();
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert', function(t) {
    var cardboard = Cardboard(config);
    var dataset = 'default';

    cardboard.put(fixtures.nullIsland, dataset, function(err) {
        t.equal(err, null);
        t.pass('inserted');
        t.end();
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert, ! in the id is reflected properly', function(t) {
    var cardboard = new Cardboard(config);
    var feature = {
        type: 'Feature',
        id: '1235456!654321',
        properties: {},
        geometry: {
            type: 'Point',
            coordinates: [0, 0]
        }
    };

    cardboard.put(feature, 'default', function(err, result) {
        t.ifError(err, 'put feature');
        t.equal(result.id, feature.id, 'reflects correct id');
        t.end();
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert, get by primary index', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.haitiLine, 'default', function(err, res) {
        t.ifError(err, 'inserted');
        cardboard.get(res.id, 'default', function(err, data) {
            t.equal(err, null);
            fixtures.haitiLine.id = res.id;

            // round-trip through geobuf will always truncate coords to 6 decimal places
            var f = geobuf.geobufToFeature(geobuf.featureToGeobuf(fixtures.haitiLine).toBuffer());
            delete fixtures.haitiLine.id;
            t.deepEqual(data, f);
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert feature with no geometry', function(t) {
    var cardboard = Cardboard(config);
    var d = {
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err) {
        t.ok(err, 'should return an error');
        t.equal(err.message, 'Unlocated features can not be stored.');
        t.end();
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert feature with no coordinates', function(t) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {type: 'Point'},
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err) {
        t.ok(err, 'should return an error');
        t.equal(err.message, 'Unlocated features can not be stored.');
        t.end();
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert wildly precise feature', function(t) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {
            coordinates: [
                0.987654321,
                0.123456789
            ],
            type: 'Point'
        },
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err, res) {
        t.ifError(err, 'inserted without error');
        dyno.getItem({ dataset: 'default', id: 'id!' + res.id }, function(err, item) {
            t.ifError(err, 'got item');
            if (err) return t.end();
            t.equal(item.west, 0.987654, 'correct west attr');
            t.equal(item.east, 0.987654, 'correct east attr');
            t.equal(item.north, 0.123457, 'correct north attr');
            t.equal(item.south, 0.123457, 'correct south attr');
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert reflects feature identical to subsequent get', function(t) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {
            coordinates: [
                0.987654321,
                0.123456789
            ],
            type: 'Point'
        },
        properties: {},
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err, reflected) {
        t.ifError(err, 'put success');
        t.ok(reflected, 'reflected a feature');
        cardboard.get(reflected.id, 'default', function(err, got) {
            t.ifError(err, 'get success');
            t.deepEqual(reflected, got, 'reflected feature identical');
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert feature with object property', function(t) {
    var cardboard = Cardboard(config);
    var d = {
        geometry: {
            coordinates: [
                0,
                0
            ],
            type: 'Point'
        },
        properties: {
            string: '0',
            int: 0,
            null: null,
            array: ['a', 'b', 'c'],
            object: {
                string: '0',
                int: 0,
                null: null,
                array: ['a', 'b', {foo: 'bar'}],
                object: {
                    enough: 'recursion'
                }
            }
        },
        type: 'Feature'
    };

    cardboard.put(d, 'default', function(err, res) {
        t.ifError(err, 'inserted without error');
        cardboard.get(res.id, 'default', function(err, data) {
            t.ifError(err, 'got item');
            d.id = res.id;
            t.deepEqual(data, geobuf.geobufToFeature(geobuf.featureToGeobuf(d).toBuffer()));
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert & update', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.haitiLine, 'default', function(err, putResult) {
        t.equal(err, null);

        t.ok(putResult.id, 'got id');
        t.pass('inserted');
        var update = _.defaults({ id: putResult.id }, fixtures.haitiLine);
        update.geometry.coordinates[0][0] = -72.588671875;

        cardboard.put(update, 'default', function(err, updateResult) {
            t.equal(err, null);
            t.equal(updateResult.id, putResult.id, 'same id returned');

            cardboard.get(putResult.id, 'default', function(err, getResult) {
                t.ifError(err, 'got record');
                var f = geobuf.geobufToFeature(geobuf.featureToGeobuf(update).toBuffer());
                t.deepEqual(getResult, f, 'expected record');
                t.end();
            });
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('delete a non-extistent feature', function(t) {
    var cardboard = Cardboard(config);
    cardboard.get('foobar', 'default', function(err, data) {
        t.ok(err);
        t.equal(err.message, 'Feature foobar does not exist');
        t.notOk(data);
        cardboard.del('foobar', 'default', function(err) {
            t.ok(err, 'should return an error');
            t.equal(err.message, 'Feature does not exist');
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert & delete', function(t) {
    var cardboard = Cardboard(config);
    var nullIsland = _.clone(fixtures.nullIsland);
    cardboard.put(nullIsland, 'default', function(err, putResult) {
        t.equal(err, null);
        t.pass('inserted');

        cardboard.get(putResult.id, 'default', function(err, data) {
            t.equal(err, null);
            nullIsland.id = putResult.id;
            t.deepEqual(data, nullIsland);
            cardboard.del(putResult.id, 'default', function(err) {
                t.ifError(err, 'removed');
                cardboard.get(putResult.id, 'default', function(err, data) {
                    t.ok(err);
                    t.equal(err.message, 'Feature ' + putResult.id + ' does not exist');
                    t.notOk(data);
                    t.end();
                });
            });
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert & delDataset', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, putResult) {
        t.equal(err, null);
        t.pass('inserted');
        cardboard.get(putResult.id, 'default', function(err, data) {
            t.equal(err, null);
            var nullIsland = _.clone(fixtures.nullIsland);
            nullIsland.id = putResult.id;
            t.deepEqual(data, nullIsland);
            cardboard.delDataset('default', function(err) {
                t.equal(err, null);
                cardboard.get(putResult.id, 'default', function(err, data) {
                    t.ok(err);
                    t.equal(err.message, 'Feature ' + putResult.id + ' does not exist');
                    t.notOk(data);
                    t.end();
                });
            });
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('delDataset - user-provide ids with !', function(t) {
    var cardboard = new Cardboard(config);
    var collection = fixtures.random(10);

    collection.features = collection.features.map(function(f) {
        f.id = crypto.randomBytes(4).toString('hex') + '!' + crypto.randomBytes(4).toString('hex');
        return f;
    });

    cardboard.batch.put(collection, 'default', function(err) {
        t.ifError(err, 'put success');

        cardboard.delDataset('default', function(err) {
            t.ifError(err, 'delDataset success');

            cardboard.list('default', function(err, collection) {
                t.ifError(err, 'list success');
                t.equal(collection.features.length, 0, 'everything was deleted');
                t.end();
            });
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('list', function(t) {
    var cardboard = Cardboard(config);

    cardboard.put(fixtures.nullIsland, 'default', function(err, primary) {
        t.equal(err, null);
        t.pass('inserted');

        var nullIsland = _.clone(fixtures.nullIsland);
        nullIsland.id = primary.id;

        cardboard.list('default', function(err, data) {
            t.deepEqual(data.features.length, 1);
            t.deepEqual(data, geojsonNormalize(nullIsland));
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('list stream', function(t) {
    var cardboard = Cardboard(config);
    var collection = fixtures.random(2223);

    cardboard.batch.put(collection, 'default', function(err, putResults) {
        t.ifError(err, 'put success');

        var streamed = [];

        cardboard.list('default')
            .on('data', function(feature) {
                streamed.push(feature);
            })
            .on('error', function(err) {
                t.ifError(err, 'stream error encountered');
            })
            .on('end', function() {
                t.equal(streamed.length, putResults.features.length, 'got all the features');
                t.end();
            });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('list stream - query error', function(t) {
    var cardboard = Cardboard(config);
    var collection = fixtures.random(20);
    t.plan(3);

    cardboard.batch.put(collection, 'default', function(err) {
        t.ifError(err, 'put success');

        // Should fail because empty string passed for dataset
        cardboard.list('')
            .on('data', function() {
                t.fail('Should not find any data');
            })
            .on('error', function(err) {
                t.pass('expected error caught');
                t.equal(err.code, 'ValidationException', 'expected error type');
                t.end();
            });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('list first page with maxFeatures', function(t) {
    var cardboard = Cardboard(config);
    var features = featureCollection([_.clone(fixtures.haiti), _.clone(fixtures.haiti), _.clone(fixtures.haiti)]);

    cardboard.batch.put(features, 'default', function page(err, putResult) {
        t.equal(err, null);
        t.pass('collection inserted');
        cardboard.list('default', {maxFeatures: 1}, function(err, data) {
            t.equal(err, null, 'no error');
            t.deepEqual(data.features.length, 1, 'first page has one feature');
            t.deepEqual(data.features[0].id, putResult.features[0].id, 'id as expected');
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('list all pages', function(t) {
    var cardboard = Cardboard(config);

    cardboard.batch.put(
        featureCollection([
            _.clone(fixtures.haiti),
            _.clone(fixtures.haiti),
            _.clone(fixtures.haiti)
        ]), 'default', page);

    function page(err, putResults) {
        t.equal(err, null);
        t.pass('collection inserted');
        var i = 0;

        function testPage(next) {
            t.notEqual(next, null, 'next key is not null');
            var opts = {maxFeatures:1};
            if (next) opts.start = next;
            cardboard.list('default', opts, function(err, data, last) {
                t.equal(err, null, 'no error');

                if (!last) {
                    t.end();
                    return;
                }

                t.deepEqual(data.features.length, 1, 'page has one feature');
                t.deepEqual(data.features[0].id, putResults.features[i].id, 'id as expected');
                i++;
                testPage(data.features.slice(-1)[0].id);
            });
        }

        testPage();
    }

});

test('teardown', s.teardown);

test('setup', s.setup);

test('page -- without maxFeatures', function(t) {
    var cardboard = Cardboard(config);

    cardboard.batch.put(
        featureCollection([
            _.clone(fixtures.haiti),
            _.clone(fixtures.haiti),
            _.clone(fixtures.haiti)
        ]), 'default', page);

    function page(err, putResults) {
        t.equal(err, null);
        t.pass('collection inserted');

        function testPage(next) {
            t.notEqual(next, null, 'next key is not null');
            var opts = {};
            if (next) opts.start = next;
            cardboard.list('default', opts, function(err, data, last) {
                t.equal(err, null, 'no error');

                if (!last) {
                    t.end();
                    return;
                }

                t.deepEqual(data.features.length, 3, 'page has three features');
                t.deepEqual(data.features[0].id, putResults.features[0].id, 'id as expected');
                t.deepEqual(data.features[1].id, putResults.features[1].id, 'id as expected');
                t.deepEqual(data.features[2].id, putResults.features[2].id, 'id as expected');
                testPage(data.features.slice(-1)[0].id);
            });
        }

        testPage();
    }

});

test('teardown', s.teardown);

test('setup', s.setup);

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
            query: [-79.0, 38.0, -76, 40],
            length: 1
        }
    ];
    var cardboard = Cardboard(config);
    var insertQueue = queue(1);

    [fixtures.nullIsland,
    fixtures.dc].forEach(function(fix) {
        insertQueue.defer(cardboard.put, fix, 'default');
    });

    insertQueue.awaitAll(function(err) {
        t.ifError(err, 'inserted');
        inserted();
    });

    function inserted() {
        var q = queue(1);

        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, resp) {
                    t.ifError(err, 'no error for ' + query.query.join(','));
                    if (err) return callback(err);

                    t.equal(resp.features.length, query.length, query.query.join(',') + ' finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });

        q.awaitAll(function(err) {
            t.ifError(err, 'queries passed');
            t.equal(cardboard.list('default', function(err, resp) {
                t.ifError(err, 'no error for list');
                if (err) throw err;

                var length = queries.reduce(function(memo, query) {
                    return memo + query.length;
                }, 0);

                t.equal(resp.features.length, length, 'list finds ' + length + ' data with a query');

                t.end();
            }), undefined, '.list');

        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert polygon', function(t) {
    var cardboard = Cardboard(config);
    cardboard.put(fixtures.haiti, 'default', inserted);

    function inserted(err) {
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
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, resp) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(resp.features.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });

        q.awaitAll(function() { t.end(); });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert linestring', function(t) {
    var cardboard = Cardboard(config);
    cardboard.put(fixtures.haitiLine, 'default', inserted);

    function inserted(err) {
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
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, resp) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(resp.features.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });

        q.awaitAll(function() { t.end(); });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert idaho', function(t) {
    var cardboard = Cardboard(config);
    t.pass('inserting idaho');

    var idaho = geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
        return f.properties.GEOID === '16049960100';
    })[0];

    cardboard.put(idaho, 'default', function(err) {
        t.ifError(err, 'inserted');
        if (err) return t.end();

        var bbox = [-115.09552001953124, 45.719603972998634, -114.77691650390625, 45.947330315089275];
        cardboard.bboxQuery(bbox, 'default', function(err, res) {
            t.ifError(err, 'no error for ' + bbox.join(','));
            t.equal(res.features.length, 1, 'finds 1 data with a query');
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert datasets and listDatasets', function(t) {
    var cardboard = Cardboard(config);
    var q = queue(1);

    q.defer(function(cb) {
        cardboard.put(fixtures.haiti, 'haiti', function() {
            cb();
        });
    });

    q.defer(function(cb) {
        cardboard.put(fixtures.dc, 'dc', function() {
            cb();
        });
    });

    q.awaitAll(getDatasets);

    function getDatasets() {
        cardboard.listDatasets(function(err, res) {
            t.notOk(err, 'should not return an error');
            t.ok(res, 'should return a array of datasets');
            t.equal(res.length, 2);
            t.equal(res[0], 'dc');
            t.equal(res[1], 'haiti');
            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('Insert feature with id specified by user', function(t) {
    var cardboard = Cardboard(config);
    var haiti = _.clone(fixtures.haiti);
    haiti.id = 'doesntexist';

    cardboard.put(haiti, 'default', function(err, putResult) {
        t.ifError(err, 'inserted');
        t.deepEqual(putResult.id, haiti.id, 'Uses given id');
        cardboard.get(haiti.id, 'default', function(err, feature) {
            var f = geobuf.geobufToFeature(geobuf.featureToGeobuf(haiti).toBuffer());
            t.deepEqual(feature, f, 'retrieved record');
            t.end();
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('Insert with and without ids specified', function(t) {
    var cardboard = Cardboard(config);
    var haiti = _.clone(fixtures.haiti);
    haiti.id = 'doesntexist';

    cardboard.batch.put(featureCollection([haiti, fixtures.haiti]), 'default', function(err, putResults) {
        t.ifError(err, 'inserted features');

        cardboard.get(haiti.id, 'default', function(err, feature) {
            var f = geobuf.geobufToFeature(geobuf.featureToGeobuf(haiti).toBuffer());
            t.deepEqual(feature, f, 'retrieved record');
            cardboard.get(putResults.features[1].id, 'default', function(err, feature) {
                var f = _.extend({ id: putResults.features[1].id }, fixtures.haiti);
                console.log(f);
                f = geobuf.geobufToFeature(geobuf.featureToGeobuf(f).toBuffer());
                t.deepEqual(feature, f, 'retrieved record');
                t.end();
            });
        });
    });
});

test('teardown', s.teardown);
