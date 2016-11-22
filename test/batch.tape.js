var test = require('tape');
var search = require('dynamodb-test')(test, 'cardboard', require('../lib/search_table.json'));
var features = require('dynamodb-test')(test, 'cardboard', require('../lib/features_table.json'));
var fs = require('fs');
var path = require('path');
var fixtures = require('./fixtures');

var states = fs.readFileSync(path.resolve(__dirname, 'data', 'states.geojson'), 'utf8');
states = JSON.parse(states);

var cardboard = require('..')({
    bucket: 'test',
    prefix: 'test',
    features: features.dyno,
    search: search.dyno,
    s3: require('mock-aws-s3').S3()
});

var unprocessableCardboard = require('..')({
    bucket: 'test',
    prefix: 'test',
    s3: require('mock-aws-s3').S3(),
    search: {
        config: { params: { TableName: search.tableName } },
        batchWriteAll: function(params) {
            return {
                sendAll: function(concurrency, callback) {
                    setTimeout(function() {
                        callback(null, {
                            UnprocessedItems: params.RequestItems
                        });
                    }, 0);
                }
            };
        }
    },
    features: {
        config: { params: { TableName: features.tableName } },
        batchWriteAll: function(params) {
            return {
                sendAll: function(concurrency, callback) {
                    setTimeout(function() {
                        callback(null, {
                            UnprocessedItems: params.RequestItems
                        });
                    }, 0);
                }
            };
        }
    }
});

features.start();
search.start();

test('[batch] put', function(assert) {
    cardboard.batch.put(states, 'states', function(err, collection) {
        assert.ifError(err, 'success');
        if(err) return assert.end(err);
        assert.equal(collection.features.length, states.features.length, 'reflects the inserted features');

        assert.ok(collection.features.reduce(function(hasId, feature) {
            if (!feature.id) hasId = false;
            return hasId;
        }, true), 'all returned features have ids');

        var records = [];
        features.dyno.scanStream()
            .on('data', function(d) { records.push(d); })
            .on('error', function(err) { throw err; })
            .on('end', function() {
                assert.equal(records.length, states.features.length, 'inserted all the features');

                assert.ok(records.reduce(function(inDataset, record) {
                    if (record.dataset !== 'states') inDataset = false;
                    return inDataset;
                }, true), 'all records in the right dataset');

                assert.end();
            });
    });
});

features.empty();
search.empty();

test('[batch] put does not duplicate auto-generated ids', function(assert) {
    var ids = [];
    (function push(attempts) {
        attempts++;
        cardboard.batch.put(fixtures.random(100), 'default', function(err, collection) {
            if (err) return assert.end(err, 'found error');
            collection.features.forEach(function(f) {
                if (ids.indexOf(f.id) > -1) assert.fail('id was duplicated');
                else ids.push(f.id);
            });

            if (attempts < 50) return push(attempts);
            assert.end();
        });
    })(0);
});

features.empty();
search.empty();

test('[batch] unprocessed put returns feature collection', function(assert) {

    var data = fixtures.random(1);
    data.features[0].id = 'abc';

    unprocessableCardboard.batch.put(data, 'default', function(err, collection) {
        if (collection) throw new Error('mock dyno failed to error');
        assert.ok(err.unprocessed, 'got unprocessed items');
        if (err.unprocessed === undefined) return assert.end(err);
        assert.equal(err.unprocessed.type, 'FeatureCollection', 'got a feature collection');
        assert.equal(err.unprocessed.features.length, data.features.length, 'expected number unprocessed items');
        assert.end();
    });
});

features.empty();
search.empty();

test('[batch] remove', function(assert) {
    cardboard.batch.put(states, 'states', function(err, collection) {
        if (err) return assert.end(err);
        var ids = collection.features.map(function(feature) {
            return feature.id;
        });

        cardboard.batch.remove(ids, 'states', function(err) {
            assert.ifError(err, 'success');

            var records = [];
            features.dyno.scanStream().on('data', function(d) { records.push(d); }).on('end', function() {
                if (err) throw err;
                assert.equal(records.length, 0, 'removed all the records');
                assert.end();
            });
        });
    });
});

test('[batch] unprocessed delete returns array of ids', function(assert) {

    var data = fixtures.random(1);
    data.features[0].id = 'abc';

    cardboard.batch.put(data, 'default', function(err) {
        if (err) return assert.end(err);

        unprocessableCardboard.batch.remove(['abc'], 'default', function(err) {
            assert.ok(err.unprocessed, 'got unprocessed items');
            assert.ok(Array.isArray(err.unprocessed), 'got an array');

            var expected = data.features.map(function(f) { return f.id; });

            assert.deepEqual(err.unprocessed, expected, 'expected unprocessed ids');
            assert.end();
        });
    });
});

features.empty();
search.empty();
features.close();
search.close();
