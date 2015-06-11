var queue = require('queue-async');
var geobuf = require('geobuf');
var url = require('url');
var geojsonNormalize = require('geojson-normalize');
var _ = require('lodash');
var cuid = require('cuid');
var Metadata = require('./metadata');
var tilebelt = require('tilebelt');

/**
 * Gain access to utils
 * @name UtilsFactory
 * @param {CardboardClientConfiguration} config - cardboard configuration object
 * @returns {utils} module containing utility functions
 */
module.exports = function(config) {
    /**
     * A module containing internal utility functions
     */
    var utils = {};

    /**
     * Convert a set of backend records into a GeoJSON features
     * @param {object[]} dynamoRecords - an array of items returned from DynamoDB in object format
     * @param {function} callback - a callback function to handle the response
     */
    utils.resolveFeatures = function(dynamoRecords, callback) {
        var q = queue(100); // Concurrency of S3 requests

        dynamoRecords.forEach(function(dynamoRecord) {
            var val = dynamoRecord.val;
            var uri = url.parse(dynamoRecord.s3url);

            q.defer(function(next) {
                if (val) return next(null, geobuf.geobufToFeature(val));

                config.s3.getObject({
                    Bucket: uri.host,
                    Key: uri.pathname.substr(1)
                }, function(err, data) {
                    if (err) return next(err);
                    next(null, geobuf.geobufToFeature(data.Body));
                });
            });
        });

        q.awaitAll(function(err, results) {
            if (err) return callback(err);
            callback(null, utils.featureCollection(results));
        });
    };

    /**
     * Wraps an array of GeoJSON features in a FeatureCollection
     * @private
     * @param {object[]} records - an array of GeoJSON features
     * @param {function} callback - a callback function to handle the response
     */
    utils.featureCollection = function(records, callback) {
        return geojsonNormalize({ type: 'FeatureCollection', features: records });
    };

    /**
     * Converts a single GeoJSON feature into backend format
     * @param {object} feature - a GeoJSON feature
     * @param {string} dataset - the name of the dataset the feature belongs to
     * @returns {object[]} an array: The first element is a DynamoDB record suitable for inserting via dyno.putItem, the second are parameters suitable for uploading via s3.putObject.
     */
    utils.toDatabaseRecord = function(feature, dataset) {
        var f = feature.hasOwnProperty('id') ? _.clone(feature) : _.extend({ id: cuid() }, feature);
        var primary = f.id;

        if (!f.geometry || !f.geometry.coordinates)
            throw new Error('Unlocated features can not be stored.');

        var info = Metadata(config.dyno, dataset).getFeatureInfo(f);
        var buf = geobuf.featureToGeobuf(f).toBuffer();
        var tile = tilebelt.bboxToTile([info.west, info.south, info.east, info.north]);
        var cell = tilebelt.tileToQuadkey(tile);
        var useS3 = buf.length >= config.MAX_GEOMETRY_SIZE;
        var s3Key = [config.prefix, dataset, primary, +new Date()].join('/');
        var s3Params = { Bucket: config.bucket, Key: s3Key, Body: buf };

        var item = {
            dataset: dataset,
            id: 'id!' + primary,
            cell: 'cell!' + cell,
            size: info.size,
            west: truncateNum(info.west),
            south: truncateNum(info.south),
            east: truncateNum(info.east),
            north: truncateNum(info.north),
            s3url: ['s3:/', config.bucket, s3Key].join('/')
        };

        if (!useS3) item.val = buf;
        return [item, s3Params];
    };

    return utils;
};

function truncateNum(num) {
    return Math.round(Math.pow(10, 6) * num) / Math.pow(10, 6);
}
