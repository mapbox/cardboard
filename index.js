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

    /**
     * Insert or update a single GeoJSON feature
     * @param {object} feature - a GeoJSON feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.put = function(feature, dataset, callback) {
        var encoded;
        try { encoded = utils.toDatabaseRecord(feature, dataset); }
        catch (err) { return callback(err); }

        var q = queue(1);
        if (encoded.s3) q.defer(config.s3.putObject.bind(config.s3), encoded.s3);
        q.defer(function(done) {
            var params = {
                Item: encoded.feature,
                ReturnValues: 'ALL_OLD'
            };
            config.mainTable.putItem(params, done);
        });
        q.await(function(err) {
            var result = geobuf.decode(new Pbf(encoded.feature.val || encoded.s3.Body));
            result.id = utils.idFromRecord(encoded.feature);
            callback(err, result);
        });
    };

    /**
     * Remove a single GeoJSON feature
     * @param {string} primary - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.del = function(primary, dataset, callback) {
        var key = utils.createFeatureKey(dataset, primary);

        config.mainTable.deleteItem({
            Key: key,
            ConditionExpression: 'attribute_exists(#index)',
            ExpressionAttributeNames: { '#index': 'index' }
        }, function(err) {
            if (err && err.code === 'ConditionalCheckFailedException') return callback(new Error('Feature does not exist'));
            if (err) return callback(err, true);
            else callback();
        });
    };

    /**
     * Retrieve a single GeoJSON feature
     * @param {string} primary - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.get = function(primary, dataset, callback) {
        var key = utils.createFeatureKey(dataset, primary);

        config.mainTable.getItem({Key: key}, function(err, data) {
            if (err) return callback(err);
            if (!data.Item) return callback(new Error('Feature ' + primary + ' does not exist'));
            utils.resolveFeatures([data.Item], function(err, data) {
                if (err) return callback(err);
                callback(null, data.features[0]);
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

    cardboard.batch = require('./lib/batch')(cardboard);

    return cardboard;
}
