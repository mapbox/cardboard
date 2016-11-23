var queue = require('queue-async');
var geobuf = require('geobuf');
var Pbf = require('pbf');
var url = require('url');
var geojsonNormalize = require('geojson-normalize');
var _ = require('lodash');
var cuid = require('cuid');
var Metadata = require('./metadata');

module.exports = Utils;

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
                var feature;
                if (val) {
                    try {
                        feature = geobuf.decode(new Pbf(val));
                    } catch(e) {
                        return next(e);
                    }
                    return next(null, feature);
                }
                if (!uri) return next(new Error('No feature data was found for ' + utils.idFromRecord(dynamoRecord)));

                config.s3.getObject({
                    Bucket: uri.host,
                    Key: uri.pathname.substr(1)
                }, function(err, data) {
                    if (err) return next(err);
                    try {
                        feature = geobuf.decode(new Pbf(data.Body));
                    } catch(e) {
                        return next(e);
                    }
                    next(null, feature);
                });
            });
        });

        q.awaitAll(function(err, results) {
            if (err) return callback(err);
            callback(null, utils.featureCollection(results));
        });
    };

    /**
     * Convert a bunch of feature ids into GeoJSON features.
     * Useful for displaying the full feature value from a
     * set of search table results
     * @param {string} dataset - the name of the dataset
     * @param {string[]} featureIds - a list of feature ids
     * @param {function} callback - the callback function
     */

    utils.resolveFeaturesByIds = function(dataset, featureIds, callback) {
        var q = queue(100);

        featureIds.forEach(function (id) {
            // TODO: have this use bulk gets
            q.defer(function(done) {
                var params = {Key: { index: dataset + '!' + id}};
                config.features.getItem(params, function(err, item) {
                    if (err) return done(err);
                    else done(null, item.Item);
                });
            });
        });

        q.awaitAll(function(err, results) {
            if (err) return callback(err);
            utils.resolveFeatures(results, callback);
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

        if (!f.geometry) {
            throw new Error('Unlocated features can not be stored.');
        }

        if (f.geometry.type === 'GeometryCollection') {
            throw new Error('The GeometryCollection geometry type is not supported.');
        }

        if (!f.geometry.coordinates) {
            throw new Error('Unlocated features can not be stored.');
        }

        var info = Metadata(config.dyno, dataset).getFeatureInfo(f);
        var buf = Buffer.from(geobuf.encode(f, new Pbf()));
        var useS3 = buf.length >= config.MAX_GEOMETRY_SIZE;
        var s3Key = [config.prefix, dataset, primary, +new Date()].join('/');
        var s3Params = { Bucket: config.bucket, Key: s3Key, Body: buf };

        var databaseFeature = {
            index: dataset + '!' + primary,
            val: useS3 ? null : buf,
            size: info.size,
            west: truncateNum(info.west),
            south: truncateNum(info.south),
            east: truncateNum(info.east),
            north: truncateNum(info.north),
            s3url: useS3 ? ['s3:/', config.bucket, s3Key].join('/') : null
        };

        var search = {
            dataset: dataset,
            index: 'feature_id!' + primary 
        };

        return {
            feature: databaseFeature,
            search: search,
            s3: useS3 ? s3Params : null
        }
    };

    /**
     * Strips database-information from a DynamoDB record's id
     * @param {object} record - a DynamoDB record
     * @returns {string} id - the feature's identifier
     */
    utils.idFromRecord = function(record) {
        var index = record.index.S || record.index;
        var bits = index.split('!');
        bits.shift();
        return bits.join('!');
    };

    return utils;
}

function truncateNum(num) {
    return Math.round(Math.pow(10, 6) * num) / Math.pow(10, 6);
}
