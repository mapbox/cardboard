var queue = require('queue-async');
var geobuf = require('geobuf');
var url = require('url');
var geojsonNormalize = require('geojson-normalize');
var _ = require('lodash');
var cuid = require('cuid');
var Metadata = require('./metadata');
var tilebelt = require('tilebelt');

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
                        feature = geobuf.geobufToFeature(val);
                        feature.quadkey = dynamoRecord.quadkey;
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
                        feature = geobuf.geobufToFeature(data.Body);
                        feature.quadkey = dynamoRecord.quadkey;
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
        var bbox = [info.west, info.south, info.east, info.north];
        var buf = geobuf.featureToGeobuf(f).toBuffer();
        var tile = tilebelt.bboxToTile(bbox);
        var cell = tilebelt.tileToQuadkey(tile);
        var centerpoint = utils.getCenterpoint(bbox);
        var quadkey = utils.getQuadkey(centerpoint[0], centerpoint[1]);
        var useS3 = buf.length >= config.MAX_GEOMETRY_SIZE;
        var s3Key = [config.prefix, dataset, primary, +new Date()].join('/');
        var s3Params = { Bucket: config.bucket, Key: s3Key, Body: buf };

        var item = {
            dataset: dataset,
            id: 'id!' + primary,
            cell: 'cell!' + cell,
            quadkey: quadkey,
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


    /**
    * Gets bbox centerpoint
    * @param {number[]} bbox - feature bounding box
    * @returns {number[]} centerpoint - [lon, lat]
    */
    utils.getCenterpoint = function(bbox) {
        var b = bbox.map(truncateNum);
        var lon = (b[0] + b[2])/2;
        var lat = (b[1] + b[3])/2;
        return [lon, lat];
    }

    /**
    * Gets quadkey from point
    * @param {number} lon - longitude
    * @param {number} lat - latitude
    * @param {number} [zoom=25] - default zoom 25 equals ~1m square tile
    * @returns {number} quadkey - tile quadkey
    */
    utils.getQuadkey = function(lon, lat) {
        var z = 25;
        var tile = tilebelt.pointToTile(truncateNum(lon), truncateNum(lat), z);
        return tilebelt.tileToQuadkey(tile);
    }

    /**
    * Calculate quadkey range based on the bbox. To mitigate possible missing features along bbox edges,
    * the quadkey range will be computed at 200% the bbox.
    * @private
    * @params {number[]} bbox - the bounding box as [west, south, east, north]
    * @returns {QuadkeyRange} - the calculated quadkey range
    *
    * @typedef QuadkeyRange
    * @type Object
    * @property {string} nw - the northwest corner quadkey
    * @property {string} se - the southeast corner quadkey
    */
    utils.calcQuadkeyRange = function(bbox) {
        function normalizeMeridian(val, bounding) {
            var deg = truncateNum(val);
            if (deg < -bounding) return (deg + bounding) + bounding;
            if (deg > bounding) return (deg - bounding) - bounding;
            return deg;
        }

        function doubleAxis(a, b, bounding) {
            var high = Math.max(a, b);
            var low = Math.min(a, b);
            var center = (high + low)/2;
            var multiplied = (high - low);
            var max = center + multiplied;
            var min = center - multiplied;

            return {
                max: max >= bounding ? bounding - 0.0001 : max,
                min: min <= -bounding ? -bounding + 0.0001 : min
            };
        }

        var west = normalizeMeridian(bbox[0], 180);
        var east = normalizeMeridian(bbox[2], 180);
        var south = normalizeMeridian(bbox[1], 90);
        var north = normalizeMeridian(bbox[3], 90);
        var horizontal = doubleAxis(east, west, 180);
        var vertical = doubleAxis(north, south, 90);

        var nw = utils.getQuadkey(horizontal.min, vertical.max);
        var se = utils.getQuadkey(horizontal.max, vertical.min);

        return {nw: nw, se: se};
    }

    return utils;
}

function truncateNum(num) {
    return Math.round(Math.pow(10, 6) * num) / Math.pow(10, 6);
}
