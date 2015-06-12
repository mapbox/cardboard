var test = require('tape');
var fs = require('fs');
var queue = require('queue-async');
var concat = require('concat-stream');
var _ = require('lodash');
var bufferEqual = require('buffer-equal');
var Cardboard = require('../');
var Metadata = require('../lib/metadata');
var geojsonExtent = require('geojson-extent');
var geojsonFixtures = require('geojson-fixtures');
var geojsonNormalize = require('geojson-normalize');
var geobuf = require('geobuf');
var fixtures = require('./fixtures');
var fakeAWS = require('mock-aws-s3');

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

    cardboard.put(fixtures.nullIsland, dataset, function(err, res) {
        t.equal(err, null);
        t.pass('inserted');
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

    cardboard.put(d, 'default', function(err, res) {
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

    cardboard.put(d, 'default', function(err, res) {
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
        update = _.defaults({ id: putResult.id }, fixtures.haitiLine);
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
        cardboard.del('foobar', 'default', function(err, data) {
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
            cardboard.del(putResult.id, 'default', function(err, data) {
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
            cardboard.delDataset('default', function(err, data) {
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

    insertQueue.awaitAll(function(err, res) {
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
                if (err) return callback(err);

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

    function inserted(err, res) {
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

    function inserted(err, res) {
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

// Metadata tests
var dataset = 'metadatatest';
var metadata = Metadata(dyno, dataset);
var initial = {
        id: metadata.recordId,
        dataset: dataset,
        count: 12,
        size: 1024,
        west: -10,
        south: -10,
        east: 10,
        north: 10
    };

test('setup', s.setup);

test('metadata: get', function(t) {

    metadata.getInfo(noMetadataYet);

    function noMetadataYet(err, info) {
        t.ifError(err, 'get non-extistent metadata');
        t.deepEqual({}, info, 'returned blank obj when no info exists');
        dyno.putItem(initial, withMetadata);
    }

    function withMetadata(err, res) {
        t.ifError(err, 'put test metadata');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'get metadata');
            t.deepEqual(info, initial, 'valid metadata');
            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: defaultInfo', function(t) {

    metadata.defaultInfo(function(err, res) {
        t.ifError(err, 'no error when creating record');
        t.ok(res, 'response indicates record was created');
        dyno.putItem(initial, overwrite);
    });

    function overwrite(err, res) {
        t.ifError(err, 'overwrote default record');
        metadata.defaultInfo(applyDefaults);
    }

    function applyDefaults(err, res) {
        t.ifError(err, 'no error when defaultInfo would overwrite');
        t.notOk(res, 'response indicates no adjustments were made');
        metadata.getInfo(checkRecord);
    }

    function checkRecord(err, info) {
        t.ifError(err, 'got metadata');
        t.deepEqual(info, initial, 'existing metadata not adjusted by defaultInfo');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: adjust size or count', function(t) {

    metadata.adjustProperties({ count: 10 }, function(err, res) {
        t.ifError(err, 'graceful if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkRecord(attr, expected, callback) {
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'get metadata');
            t.equal(info[attr], expected, 'expected value');
            callback();
        });
    }

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by adjustProperties routine');
        dyno.putItem(initial, addCount);
    }

    function addCount(err, res) {
        t.ifError(err, 'put metadata record');
        metadata.adjustProperties({ count: 1 }, function(err, res) {
            t.ifError(err, 'incremented count by 1');
            checkRecord('count', initial.count + 1, subtractCount);
        });
    }

    function subtractCount() {
        metadata.adjustProperties({ count: -1 }, function(err, res) {
            t.ifError(err, 'decrement count by 1');
            checkRecord('count', initial.count, addSize);
        });
    }

    function addSize() {
        metadata.adjustProperties({ size: 1024 }, function(err, res) {
            t.ifError(err, 'incremented size by 1024');
            checkRecord('size', initial.size + 1024, subtractSize);
        });
    }

    function subtractSize() {
        metadata.adjustProperties({ size: -1024 }, function(err, res) {
            t.ifError(err, 'decrement size by 1024');
            checkRecord('size', initial.size, addBoth);
        });
    }

    function addBoth() {
        metadata.adjustProperties({ count: 1, size: 1024 }, function(err, res) {
            t.ifError(err, 'increment size and count');
            checkRecord('size', initial.size + 1024, function() {
                checkRecord('count', initial.count + 1, function() {
                    t.end();
                });
            });
        });
    }

});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: adjust bounds', function(t) {
    var bbox = [-12.01, -9, 9, 12.01];

    metadata.adjustBounds(bbox, function(err) {
        t.ifError(err, 'graceful if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by adjustBounds routine');
        dyno.putItem(initial, adjust);
    }

    function adjust(err, res) {
        t.ifError(err, 'put metadata record');
        metadata.adjustBounds(bbox, adjusted);
    }

    function adjusted(err, res) {
        t.ifError(err, 'adjusted bounds without error');
        metadata.getInfo(checkNewInfo);
    }

    function checkNewInfo(err, info) {
        t.ifError(err, 'get new metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: initial.west < bbox[0] ? initial.west : bbox[0],
            south: initial.south < bbox[1] ? initial.south : bbox[1],
            east: initial.east > bbox[2] ? initial.east : bbox[2],
            north: initial.north > bbox[3] ? initial.north : bbox[3],
            count: initial.count,
            size: initial.size
        };
        t.deepEqual(_.omit(info, 'updated'), expected, 'updated metadata correctly');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: add a feature', function(t) {
    var feature = geojsonFixtures.feature.one;
    var expectedSize = Buffer.byteLength(JSON.stringify(feature));
    var expectedBounds = geojsonExtent(feature);

    metadata.addFeature(feature, brandNew);

    function brandNew(err) {
        t.ifError(err, 'used feature to make new metadata');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'got metadata');
            t.equal(info.count, 1, 'correct feature count');
            t.equal(info.size, expectedSize, 'correct size');
            t.equal(info.west, expectedBounds[0], 'correct west');
            t.equal(info.south, expectedBounds[1], 'correct south');
            t.equal(info.east, expectedBounds[2], 'correct east');
            t.equal(info.north, expectedBounds[3], 'correct north');

            dyno.putItem(initial, replacedMetadata);
        });
    }

    function replacedMetadata(err) {
        t.ifError(err, 'replaced metadata');
        metadata.addFeature(feature, adjusted);
    }

    function adjusted(err) {
        t.ifError(err, 'adjusted existing metadata');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'got metadata');
            t.equal(info.count, initial.count + 1, 'correct feature count');
            t.equal(info.size, initial.size + expectedSize, 'correct size');

            var expectedWest = expectedBounds[0] < initial.west ?
                expectedBounds[0] : initial.west;
            var expectedSouth = expectedBounds[1] < initial.south ?
                expectedBounds[1] : initial.south;
            var expectedEast = expectedBounds[2] > initial.east ?
                expectedBounds[2] : initial.east;
            var expectedNorth = expectedBounds[3] > initial.north ?
                expectedBounds[3] : initial.north;

            t.equal(info.west, expectedWest, 'correct west');
            t.equal(info.south, expectedSouth, 'correct south');
            t.equal(info.east, expectedEast, 'correct east');
            t.equal(info.north, expectedNorth, 'correct north');

            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: update a feature', function(t) {
    var original = geojsonFixtures.feature.one;
    var edited = geojsonFixtures.featurecollection.idaho.features[0];
    var expectedSize = JSON.stringify(edited).length - JSON.stringify(original).length;
    var expectedBounds = geojsonExtent(edited);

    metadata.updateFeature(original, edited, function(err) {
        t.ifError(err, 'graceful exit if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by updateFeature routine');
        metadata.defaultInfo(andThen);
    }

    function andThen(err) {
        t.ifError(err, 'default metadata');
        metadata.updateFeature(original, edited, checkInfo);
    }

    function checkInfo(err) {
        t.ifError(err, 'updated metadata');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'got metadata');
            t.equal(info.count, 0, 'correct feature count');
            t.equal(info.size, expectedSize, 'correct size');
            t.equal(info.west, expectedBounds[0], 'correct west');
            t.equal(info.south, expectedBounds[1], 'correct south');
            t.equal(info.east, expectedBounds[2], 'correct east');
            t.equal(info.north, expectedBounds[3], 'correct north');
            t.end();
        });
    }

});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: remove a feature', function(t) {
    var feature = geojsonFixtures.feature.one;
    var expectedSize = Buffer.byteLength(JSON.stringify(feature));

    metadata.deleteFeature(feature, function(err) {
        t.ifError(err, 'graceful exit if no metadata exists');
        metadata.getInfo(checkEmpty);
    });

    function checkEmpty(err, info) {
        t.ifError(err, 'gets empty record');
        t.deepEqual(info, {}, 'no record created by adjustBounds routine');
        dyno.putItem(initial, del);
    }

    function del(err) {
        t.ifError(err, 'put default metadata');
        metadata.deleteFeature(feature, checkInfo);
    }

    function checkInfo(err) {
        t.ifError(err, 'updated metadata');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'got info');
            t.equal(info.count, initial.count - 1, 'correct feature count');
            t.equal(info.size, initial.size - expectedSize, 'correct size');
            t.equal(info.west, initial.west, 'correct west');
            t.equal(info.south, initial.south, 'correct south');
            t.equal(info.east, initial.east, 'correct east');
            t.equal(info.north, initial.north, 'correct north');
            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('metadata: calculate dataset info', function(t) {
    var cardboard = new Cardboard(config);
    var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
    var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });

    cardboard.batch.put(featureCollection(features), dataset, function(err, putFeatures) {
        t.ifError(err, 'inserted');

        features = features.map(function(f, i) {
            var feature = _.defaults({ id: putFeatures.features[i].id }, f);
            return feature;
        });

        var expectedSize = features.reduce(function(memo, feature) {
            memo = memo + Buffer.byteLength(JSON.stringify(feature));
            return memo;
        }, 0);

        var expected = {
            dataset: dataset,
            id: metadata.recordId,
            size: expectedSize,
            count: features.length,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3]
        };

        metadata.calculateInfo(function(err, info) {
            t.ifError(err, 'calculated');
            t.ok(info.updated, 'has updated date');
            t.deepEqual(_.omit(info, 'updated'), expected, 'returned expected info');
            cardboard.getDatasetInfo(dataset, function(err, info) {
                t.ifError(err, 'got metadata');
                t.ok(info.updated, 'has updated date');
                t.deepEqual(_.omit(info, 'updated'), expected, 'got expected info from dynamo');
                t.end();
            });
        });
    });
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert idaho & check metadata', function(t) {
    var cardboard = new Cardboard(config);
    t.pass('inserting idaho');

    var f = geojsonFixtures.featurecollection.idaho.features.filter(function(f) {
        return f.properties.GEOID === '16049960100';
    })[0];

    var info = metadata.getFeatureInfo(f);

    queue()
        .defer(cardboard.put, f, dataset)
        .defer(metadata.addFeature, f)
        .awaitAll(inserted);

    function inserted(err, res) {
        if (err) console.error(err);
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: info.west,
            south: info.south,
            east: info.east,
            north: info.north,
            count: 1,
            size: info.size
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert many idaho features & check metadata', function(t) {
    var cardboard = new Cardboard(config);
    var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
    var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
    var expectedSize = features.reduce(function(memo, feature) {
        memo = memo + Buffer.byteLength(JSON.stringify(feature));
        return memo;
    }, 0);

    var q = queue();

    features.forEach(function(block) {
        q.defer(cardboard.put, block, dataset);
        q.defer(metadata.addFeature, block);
    });

    q.awaitAll(inserted);

    function inserted(err, res) {
        if (err) console.error(err);
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho features');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            count: features.length,
            size: expectedSize
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert many idaho features, delete one & check metadata', function(t) {
    var cardboard = new Cardboard(config);
    var features = geojsonFixtures.featurecollection.idaho.features.slice(0, 50);
    var deleteThis = features[9];
    var expectedBounds = geojsonExtent({ type: 'FeatureCollection', features: features });
    var expectedSize = features.reduce(function(memo, feature) {
        memo = memo + JSON.stringify(feature).length;
        return memo;
    }, 0) - JSON.stringify(deleteThis).length;

    var q = queue();

    features.forEach(function(block) {
        q.defer(cardboard.put, block, dataset);
        q.defer(metadata.addFeature, block);
    });

    q.defer(metadata.deleteFeature, deleteThis);
    q.awaitAll(inserted);

    function inserted(err, res) {
        if (err) console.error(err);
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho features and deleted one');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            count: features.length - 1,
            size: expectedSize
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('insert idaho feature, update & check metadata', function(t) {
    var cardboard = new Cardboard(config);
    var original = geojsonFixtures.featurecollection.idaho.features[0];
    var edited = geojsonFixtures.featurecollection.idaho.features[1];

    var expectedSize;
    var expectedBounds = geojsonExtent({
        type: 'FeatureCollection',
        features: [original, edited]
    });

    queue()
        .defer(cardboard.put, original, dataset)
        .defer(metadata.addFeature, original)
        .awaitAll(inserted);

    function inserted(err, res) {
        t.notOk(err, 'no error returned');
        t.pass('inserted idaho feature');

        var update = _.extend({ id: res[0] }, edited);
        expectedSize = JSON.stringify(update).length;
        queue()
            .defer(cardboard.put, update, dataset)
            .defer(metadata.updateFeature, original, update)
            .awaitAll(updated);
    }

    function updated(err, res) {
        t.ifError(err, 'updated feature');
        metadata.getInfo(checkInfo);
    }

    function checkInfo(err, info) {
        t.ifError(err, 'got idaho metadata');
        var expected = {
            id: 'metadata!' + dataset,
            dataset: dataset,
            west: expectedBounds[0],
            south: expectedBounds[1],
            east: expectedBounds[2],
            north: expectedBounds[3],
            count: 1,
            size: expectedSize
        };
        t.ok(info.updated, 'has updated date');
        t.deepEqual(_.omit(info, 'updated'), expected, 'expected metadata');
        t.end();
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('delDataset removes metadata', function(t) {
    var cardboard = new Cardboard(config);
    dyno.putItem(initial, function(err) {
        t.ifError(err, 'put initial metadata');
        cardboard.delDataset(dataset, removed);
    });

    function removed(err) {
        t.ifError(err, 'removed dataset');
        metadata.getInfo(function(err, info) {
            t.ifError(err, 'looked for metadata');
            t.deepEqual(info, {}, 'metadata was removed');
            t.end();
        });
    }
});

test('teardown', s.teardown);

test('setup', s.setup);

test('getDatasetInfo', function(t) {
    var cardboard = new Cardboard(config);
    dyno.putItem(initial, function(err) {
        t.ifError(err, 'put initial metadata');
        cardboard.getDatasetInfo(dataset, checkInfo);
    });

    function checkInfo(err, info) {
        t.ifError(err, 'got metadata');
        t.deepEqual(info, initial, 'metadata is correct');
        t.end();
    }
});

test('teardown', s.teardown);
