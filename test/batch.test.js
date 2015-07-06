var test = require('tape');
var dynamodb = require('dynamodb-test')(test, 'cardboard', require('../lib/table.json'));
var fs = require('fs');
var path = require('path');
var fixtures = require('./fixtures');

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

test('[batch] does not duplicate auto-generated ids', function(assert) {
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

dynamodb.close();
