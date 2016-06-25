var queue = require('queue-async');
var geobuf = require('geobuf');
var url = require('url');
var geojsonNormalize = require('geojson-normalize');
var _ = require('lodash');
var cuid = require('cuid');
var Metadata = require('./metadata');
var tilebelt = require('tilebelt');

module.exports = Utils;

function decode(buf, callback) {
    var feature;
    try {
        feature = geobuf.geobufToFeature(buf);
    } catch(err) {
        return callback(err);
    }

    for (var key in feature.properties) feature.properties[key] = JSON.parse(feature.properties[key]);
    callback(null, feature);
}

function Utils(config) {
    /**
     * A module containing internal utility functions
     */
    var utils = {};

    /**
     * Convert a set of backend records into a GeoJSON features
     * @param {object[]} dynamoRecords - an array of items returned from DynamoDB in simplified JSON format
     * @param {function} callback - a callback function to handle the response
     */
    utils.resolveFeatures = function(dynamoRecords, callback) {
        var q = queue(100); // Concurrency of S3 requests

        dynamoRecords.forEach(function(dynamoRecord) {
            q.defer(function(next) {
                var val = dynamoRecord.val;
                var uri = dynamoRecord.s3url ? url.parse(dynamoRecord.s3url) : undefined;

                if (val) return decode(val, next);
                if (!uri) return next(new Error('No feature data was found for ' + utils.idFromRecord(dynamoRecord)));

                config.s3.getObject({
                    Bucket: uri.host,
                    Key: uri.pathname.substr(1)
                }, function(err, data) {
                    if (err) return next(err);
                    decode(data.Body, next);
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
     */
    utils.featureCollection = function(records) {
        return geojsonNormalize({ type: 'FeatureCollection', features: records });
    };

    /**
     * Converts a single GeoJSON feature into backend format
     * @param {object} feature - a GeoJSON feature
     * @param {string} dataset - the name of the dataset the feature belongs to
     * @returns {object[]} the first element is a DynamoDB record suitable for inserting via `dyno.putItem`, the second are parameters suitable for uploading via `s3.putObject`.
     */
    utils.toDatabaseRecord = function(feature, dataset) {
        if (feature.id === 0) feature.id = '0';
        var f = feature.id ? _.clone(feature) : _.extend({}, feature, { id: cuid() });
        var primary = f.id;

        if (!f.geometry || !f.geometry.coordinates)
            throw new Error('Unlocated features can not be stored.');

        var info = Metadata(config.dyno, dataset).getFeatureInfo(f);

        var encodedProperties = {};
        for (var key in f.properties) encodedProperties[key] = JSON.stringify(f.properties[key]);
        f.properties = encodedProperties;

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
            north: truncateNum(info.north)
        };

        if (useS3) {
            item.s3url = ['s3:/', config.bucket, s3Key].join('/');
            return [item, s3Params];
        } else {
            item.val = buf;
            return [item];
        }
    };

    /**
     * Strips database-information from a DynamoDB record's id
     * @param {object} record - a DynamoDB record
     * @returns {string} id - the feature's identifier
     */
    utils.idFromRecord = function(record) {
        var id = record.id.S || record.id;
        var bits = id.split('!');
        bits.shift();
        return bits.join('!');
    };

    return utils;
}

function truncateNum(num) {
    return Math.round(Math.pow(10, 6) * num) / Math.pow(10, 6);
}
