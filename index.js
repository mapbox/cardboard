var _ = require('lodash');
var Metadata = require('./lib/metadata');
var queue = require('queue-async');
var Dyno = require('dyno');
var AWS = require('aws-sdk');
var Pbf = require('pbf');
var geobuf = require('geobuf');
var stream = require('stream');

var MAX_GEOMETRY_SIZE = 1024 * 10;  // 10KB

module.exports = Cardboard;

/**
 * Cardboard client generator
 * @param {object} config - a configuration object
 * @param {string} config.mainTable - the name of a DynamoDB table to connect to
 * @param {string} config.region - the AWS region containing the DynamoDB table
 * @param {string} config.bucket - the name of an S3 bucket to use
 * @param {string} config.prefix - the name of a folder within the indicated S3 bucket
 * @param {dyno} [config.dyno] - a pre-configured [dyno client](https://github.com/mapbox/dyno) for connecting to DynamoDB
 * @param {s3} [config.s3] - a pre-configured [S3 client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)
 * @returns {cardboard} a cardboard client
 */
function Cardboard(config) {
    config = config || {};
    config.MAX_GEOMETRY_SIZE = config.MAX_GEOMETRY_SIZE || MAX_GEOMETRY_SIZE;

    // Allow caller to pass in aws-sdk clients
    if (!config.s3) config.s3 = new AWS.S3(config);
    if (!config.mainTable) throw new Error('No main table has been set');
    if (typeof config.mainTable === 'string' && !config.region) throw new Error('No region set');
    if (typeof config.mainTable === 'string') config.mainTable = Dyno({table: config.mainTable, region: config.region, endpoint: config.endpoint});
    if (!config.bucket) throw new Error('No bucket set');
    if (!config.prefix) throw new Error('No s3 prefix set');

    var utils = require('./lib/utils')(config);

    /**
     * A client configured to interact with a backend cardboard database
     */
    var cardboard = {};
    var mainTable = config.mainTable.config.params.TableName;

    /**
     * Insert or update a single GeoJSON feature
     * @param {(Feature|FeatureCollection)} input - a GeoJSON feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.put = function(input, dataset, callback) {
        if (input.type === 'Feature') input = utils.featureCollection([input]);
        if (input.type !== 'FeatureCollection') throw new Error('Must be a Feature or FeatureCollection');

        var records = [];
        var geobufs = {};

        var encoded;
        var q = queue(150);

        for (var i = 0; i < input.features.length; i++) {
            try { encoded = utils.toDatabaseRecord(input.features[i], dataset); }
            catch (err) { return callback(err); }

            records.push(encoded.feature);
            geobufs[encoded.feature.index] = encoded.feature.val || encoded.s3.Body;
            if (encoded.s3) q.defer(config.s3.putObject.bind(config.s3), encoded.s3);
        }

        q.awaitAll(function(err) {
            if (err) return callback(err);

            var params = { RequestItems: {} };
            params.RequestItems[mainTable] = records.map(function(record) {
                return { PutRequest: { Item: record } };
            });

            config.mainTable.batchWriteAll(params).sendAll(10, function(err, res) {
                if (err) return callback(err);

                var unprocessed = res.UnprocessedItems[mainTable] ? res.UnprocessedItems[mainTable] : [];

                var pending = unprocessed.map(function(req) {
                    var item = req.PutRequest.Item;
                    var buffer = geobufs[item.index];
                    delete geobufs[item.index];
                    return utils.decodeBuffer(buffer);
                });

                var features = Object.keys(geobufs).map(function(index) {
                    return utils.decodeBuffer(geobufs[index]);
                });

                var fc = utils.featureCollection(features);
                if (pending.length > 0) fc.pending = pending;
                callback(null, fc);
            });
        });
    };

    /**
     * Remove a single GeoJSON feature
     * @param {string | [string]} input - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.del = function(input, dataset, callback) {
        if (!Array.isArray(input)) input = [input];
        var params = { RequestItems: {} };
        params.RequestItems[mainTable] = input.map(function(id) {
            if (typeof id !== 'string') throw new Error('All ids must be strings');
            return { DeleteRequest: { Key: utils.createFeatureKey(dataset, id) } };
        });

        config.mainTable.batchWriteAll(params).sendAll(10, function(err, res) {
            if (err) return callback(err);

            var unprocessed = res.UnprocessedItems ? res.UnprocessedItems[mainTable] || [] : [];

            var ids = unprocessed.map(function(item) {
                return utils.idFromRecord(item.DeleteRequest.Key);
            });

            callback(null, ids);
        });
    };

    /**
     * Retrieve a single GeoJSON feature
     * @param {string|[string]} input - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.get = function(input, dataset, callback) {
        if (!Array.isArray(input)) input = [input]; 

        var keys = input.map(function(id) { return utils.createFeatureKey(dataset, id); });

        var params = { RequestItems: {}};
        params.RequestItems[mainTable] = { Keys: keys };

        config.mainTable.batchGetAll(params).sendAll(10, function(err, res) {
            if (err) return callback(err);
            var features = res.Responses ? res.Responses[mainTable] : [];
            var pending = res.UnprocessedKeys && res.UnprocessedKeys[mainTable] ? res.UnprocessedKeys[mainTable].Keys : [];

            utils.resolveFeatures(features, function(err, data) {
                if (err) return callback(err);
                if (pending.length > 0) {
                    data.pending = pending.map(function(key) { return utils.idFromRecord(key); });
                }
                callback(null, data);
            });
        });
    };

    /**
     * Create DynamoDB tables with Cardboard's schema
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.createTable = function(callback) {
        var featuresTable = require('./lib/features_table.json');
        featuresTable.TableName = config.mainTable.TableName;
        config.mainTable.createTable(featuresTable, callback);
    };

    return cardboard;
}
