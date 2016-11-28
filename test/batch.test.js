var assert = require('assert');
var fs = require('fs');
var path = require('path');
var fixtures = require('./fixtures');
var states = JSON.parse(fs.readFileSync(path.resolve(__dirname, 'data', 'states.geojson'), 'utf8'));

var s = require('./setup');
var config = s.config;

function unprocessableDyno(table) {
    return {
        config: { params: { TableName: table} },
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
};

describe('[batch]', function() {
    var cardboard = require('../')(config);     
    var unprocessableCardboard = require('..')({
        bucket: 'test',
        prefix: 'test',
        s3: require('mock-aws-s3').S3(),
        features: unprocessableDyno(config.featuresTable),
        search: unprocessableDyno(config.searchTable)
    });

    beforeEach(s.setup);
    afterEach(s.teardown);
        
    it('put', function(done) {
        cardboard.batch.put(states, 'states', function(err, collection) {
            assert.ifError(err, 'success');
            assert.equal(collection.features.length, states.features.length, 'reflects the inserted features');

            assert.ok(collection.features.reduce(function(hasId, feature) {
                if (!feature.id) hasId = false;
                return hasId;
            }, true), 'all returned features have ids');

            var records = [];
            config.features.scanStream()
                .on('data', function(d) { records.push(d); })
                .on('error', function(err) { throw err; })
                .on('end', function() {
                    assert.equal(records.length, states.features.length, 'inserted all the features');

                    assert.ok(records.reduce(function(inDataset, record) {
                        if (record.index.indexOf('states!') !== 0) inDataset = false;
                        return inDataset;
                    }, true), 'all records in the right dataset');

                    done();
                });
        });
    });

    it('put does not duplicate auto-generated ids', function(done) {
        this.timeout(20000);
        var ids = [];
        (function push(attempts) {
            attempts++;
            cardboard.batch.put(fixtures.random(100), 'default', function(err, collection) {
                collection.features.forEach(function(f) {
                    if (ids.indexOf(f.id) > -1) assert.fail('id was duplicated');
                    else ids.push(f.id);
                });

                if (attempts < 50) return push(attempts);
                done();
            });
        })(0);
    });

    it.skip('unprocessed put returns feature collection', function(done) {

        var data = fixtures.random(1);
        data.features[0].id = 'abc';
        unprocessableCardboard.batch.put(data, 'default', function(err, collection) {
            if (collection) throw new Error('mock dyno failed to error');
            assert.ok(err.unprocessed, 'got unprocessed items');
            assert.equal(err.unprocessed.type, 'FeatureCollection', 'got a feature collection');
            assert.equal(err.unprocessed.features.length, data.features.length, 'expected number unprocessed items');
            done();
        });
    });

    it('del', function(done) {
        cardboard.batch.put(states, 'states', function(err, collection) {
            if (err) throw err;
            var ids = collection.features.map(function(feature) {
                return feature.id;
            });

            cardboard.batch.del(ids, 'states', function(err) {
                assert.ifError(err, 'success');

                var records = [];
                config.features.scanStream().on('data', function(d) { records.push(d); }).on('end', function() {
                    if (err) throw err;
                    assert.equal(records.length, 0, 'deleted all the records');
                    done();
                });
            });
        });
    });

    it.skip('unprocessed delete returns array of ids', function(done) {

        var data = fixtures.random(1);
        data.features[0].id = 'abc';

        cardboard.batch.put(data, 'default', function(err) {
            if (err) throw err;

            unprocessableCardboard.batch.del(['abc'], 'default', function(err) {
                assert.ok(err.unprocessed, 'got unprocessed items');
                assert.ok(Array.isArray(err.unprocessed), 'got an array');

                var expected = data.features.map(function(f) { return f.id; });

                assert.deepEqual(err.unprocessed, expected, 'expected unprocessed ids');
                done();
            });
        });
    });
});

