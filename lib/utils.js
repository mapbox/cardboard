var geobuf = require('geobuf');
var Pbf = require('pbf');
var geojsonNormalize = require('geojson-normalize');
var _ = require('lodash');
var cuid = require('cuid');

module.exports = Utils;

function Utils() {
    /**
     * A module containing internal utility functions
     */
    var utils = {};

    /**
     * Convert a set of backend records into a GeoJSON featureCollection
     * @param {object[]} dynamoRecords - an array of items returned from DynamoDB in simplified JSON format
     */
    utils.resolveFeatures = function(dynamoRecords) {
        return utils.featureCollection(dynamoRecords.map(function(record) {
            return utils.decodeBuffer(record.val);
        }));
    };

    /**
     * makes sure the key is create the same way everytime
     */
    utils.createFeatureKey = function(dataset, featureId) {
        return {
            key: dataset + '!feature!' + featureId
        }
    };

    /**
     * Takes a buffer and turns it into a feature
     * This should be the only place in the code we do this
     * so that changing versions of geobuf happens in one place
     */
    utils.decodeBuffer = function(buf) {
        return geobuf.decode(new Pbf(buf));
    };

    /**
     * Takes a feature and converts it into a buffer
     * This should be the only palce in the code we do this
     * so that changing versionf of geobuf happens in one place
     */
    utils.encodeFeature = function(feature) {
        return Buffer.from(geobuf.encode(feature, new Pbf()));  
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
        f.id = ''+f.id;

        if (!f.geometry) {
            throw new Error('Unlocated features can not be stored.');
        }

        if (f.geometry.type === 'GeometryCollection') {
            throw new Error('The GeometryCollection geometry type is not supported.');
        }

        if (!f.geometry.coordinates) {
            throw new Error('Unlocated features can not be stored.');
        }

        var buf = utils.encodeFeature(f);
        var databaseFeature = utils.createFeatureKey(dataset, f.id);
        databaseFeature.val = buf;
        databaseFeature.size = buf.length;

        return databaseFeature;
    };

    /**
     * Strips database-information from a DynamoDB record's id
     * @param {object} record - a DynamoDB record
     * @returns {string} id - the feature's identifier
     */
    utils.idFromRecord = function(record) {
        var key = record.key.S || record.key;
        var bits = key.split('!');
        bits.shift();
        bits.shift();
        return bits.join('!');
    };

    return utils;
}

