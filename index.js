var queue = require('queue-async');
var Dyno = require('dyno');

module.exports = Cardboard;

/**
 * Cardboard client generator
 * @param {object} config - a configuration object
 * @param {string} config.dyno - the name of a DynamoDB table to connect to
 * @param {string} config.region - the AWS region containing the DynamoDB table
 * @param {string} config.bucket - the name of an S3 bucket to use
 * @param {string} config.prefix - the name of a folder within the indicated S3 bucket
 * @param {dyno} [config.dyno] - a pre-configured [dyno client](https://github.com/mapbox/dyno) for connecting to DynamoDB
 * @param {s3} [config.s3] - a pre-configured [S3 client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)
 * @returns {cardboard} a cardboard client
 */
function Cardboard(config) {
    config = config || {};

    // Allow caller to pass in aws-sdk clients
    if (typeof config.mainTable !== 'string' || config.mainTable.length === 0) throw new Error('"mainTable" must be a string');
    if (!config.dyno && !config.region) throw new Error('No region set');
    if (!config.dyno) config.dyno = Dyno({table: config.mainTable, region: config.region, endpoint: config.endpoint});

    var utils = require('./lib/utils');

    /**
     * A client configured to interact with a backend cardboard database
     */
    var cardboard = {};

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

            records.push(encoded);
            geobufs[encoded.key] = encoded.val;
        }

        q.awaitAll(function(err) {
            if (err) return callback(err);

            var params = { RequestItems: {} };
            params.RequestItems[config.mainTable] = records.map(function(record) {
                return { PutRequest: { Item: record } };
            });

            config.dyno.batchWriteAll(params).sendAll(10, function(err, res) {
                if (err) return callback(err);

                var unprocessed = res.UnprocessedItems[config.mainTable] ? res.UnprocessedItems[config.mainTable] : [];

                var pending = unprocessed.map(function(req) {
                    var item = req.PutRequest.Item;
                    var buffer = geobufs[item.key];
                    delete geobufs[item.key];
                    return utils.decodeBuffer(buffer);
                });

                var features = Object.keys(geobufs).map(function(key) {
                    return utils.decodeBuffer(geobufs[key]);
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
        params.RequestItems[config.mainTable] = input.map(function(id) {
            if (typeof id !== 'string') throw new Error('All ids must be strings');
            return { DeleteRequest: { Key: utils.createFeatureKey(dataset, id) } };
        });

        config.dyno.batchWriteAll(params).sendAll(10, function(err, res) {
            if (err) return callback(err);

            var unprocessed = res.UnprocessedItems ? res.UnprocessedItems[config.mainTable] || [] : [];

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
        params.RequestItems[config.mainTable] = { Keys: keys };

        config.dyno.batchGetAll(params).sendAll(10, function(err, res) {
            if (err) return callback(err);
            var features = res.Responses ? res.Responses[config.mainTable] : [];
            var pending = res.UnprocessedKeys && res.UnprocessedKeys[config.mainTable] ? res.UnprocessedKeys[config.mainTable].Keys : [];

            try {
                var fc = utils.resolveFeatures(features);
                if (pending.length > 0) {
                    fc.pending = pending.map(function(key) { return utils.idFromRecord(key); });
                }
                return callback(null, fc);
            }
            catch(err) {
                return callback(err);
            }
        });
    };

    /**
     * Create DynamoDB tables with Cardboard's schema
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.createTable = function(callback) {
        var tableSchema = require('./lib/main-table.json');
        tableSchema.TableName = config.mainTable;
        config.dyno.createTable(tableSchema, callback);
    };

    return cardboard;
}

Cardboard.streamHelper = require('./lib/stream-helper');
