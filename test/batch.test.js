var test = require('tape');
var dynamodb = require('dynamodb-test')(test, 'cardboard', require('../lib/table.json'));
var fs = require('fs');
var path = require('path');
var fixtures = require('./fixtures');
var Dyno = require('dyno');

var states = fs.readFileSync(path.resolve(__dirname, 'data', 'states.geojson'), 'utf8');
states = JSON.parse(states);

var cardboard = require('..')({
    bucket: 'test',
    prefix: 'test',
    dyno: dynamodb.dyno,
    s3: require('mock-aws-s3').S3()
});

dynamodb.start();

test('[batch] put', function(assert) {
    cardboard.batch.put(states, 'states', function(err, collection) {
        assert.ifError(err, 'success');
        assert.equal(collection.features.length, states.features.length, 'reflects the inserted features');

        assert.ok(collection.features.reduce(function(hasId, feature) {
            if (!feature.id) hasId = false;
            return hasId;
        }, true), 'all returned features have ids');

        dynamodb.dyno.scan(function(err, records) {
            if (err) throw err;

            assert.equal(records.length, states.features.length, 'inserted all the features');

            assert.ok(records.reduce(function(inDataset, record) {
                if (record.dataset !== 'states') inDataset = false;
                return inDataset;
            }, true), 'all records in the right dataset');

            assert.end();
        });
    });
});

dynamodb.empty();

test('[batch] put does not duplicate auto-generated ids', function(assert) {
    var ids = [];
    (function push(attempts) {
        attempts++;
        cardboard.batch.put(fixtures.random(100), 'default', function(err, collection) {
            collection.features.forEach(function(f) {
                if (ids.indexOf(f.id) > -1) assert.fail('id was duplicated');
                else ids.push(f.id);
            });

            if (attempts < 50) return push(attempts);
            assert.end();
        });
    })(0);
});

dynamodb.empty();

test('[batch] unprocessed put returns feature collection', function(assert) {
    var cardboard = require('..')({
        bucket: 'test',
        prefix: 'test',
        s3: require('mock-aws-s3').S3(),
        dyno: {
            putItems: function(items, callback) {
                callback({ unprocessed: {
                    tableName: items.map(function(item) {
                        return { PutRequest: { Item: JSON.parse(Dyno.serialize(item)) } };
                    })
                }});
            }
        }
    });

    var data = fixtures.random(1);
    data.features[0].id = 'abc';

    cardboard.batch.put(data, 'default', function(err, collection) {
        if (collection) throw new Error('mock dyno failed to error');
        assert.ok(err.unprocessed, 'got unprocessed items');
        assert.equal(err.unprocessed.type, 'FeatureCollection', 'got a feature collection');
        assert.equal(err.unprocessed.features.length, data.features.length, 'expected number unprocessed items');
        assert.end();
    });
});

dynamodb.empty();

test('[batch] remove', function(assert) {
    cardboard.batch.put(states, 'states', function(err, collection) {
        if (err) throw err;
        var ids = collection.features.map(function(feature) {
            return feature.id;
        });

        cardboard.batch.remove(ids, 'states', function(err) {
            assert.ifError(err, 'success');

            dynamodb.dyno.scan(function(err, records) {
                if (err) throw err;
                assert.equal(records.length, 0, 'removed all the records');
                assert.end();
            });
        });
    });
});

test('[batch] unprocessed delete returns array of ids', function(assert) {
    var mockcardboard = require('..')({
        bucket: 'test',
        prefix: 'test',
        s3: require('mock-aws-s3').S3(),
        dyno: {
            deleteItems: function(keys, callback) {
                callback({ unprocessed: {
                    tableName: keys.map(function(key) {
                        return { DeleteRequest: { Key: JSON.parse(Dyno.serialize(key)) } };
                    })
                }});
            }
        }
    });

    var data = fixtures.random(1);
    data.features[0].id = 'abc';

    cardboard.batch.put(data, 'default', function(err) {
        if (err) throw err;

        mockcardboard.batch.remove(['abc'], 'default', function(err) {
            assert.ok(err.unprocessed, 'got unprocessed items');
            assert.ok(Array.isArray(err.unprocessed), 'got an array');

            var expected = data.features.map(function(f) { return f.id; });

            assert.deepEqual(err.unprocessed, expected, 'expected unprocessed ids');
            assert.end();
        });
    });
});

dynamodb.empty();

dynamodb.close();
