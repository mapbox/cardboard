var queue = require('queue-async');
var extent = require('geojson-extent');
var through = require('through2');

var Metadata = module.exports = function(dyno, dataset) {

    /**
     * A client for interacting with the metadata for a dataset
     */
    var metadata = {};
    var recordId = metadata.recordId = 'metadata!' + dataset;
    var key = metadata.key = { id: recordId, dataset: dataset };

    /**
     * Helper routine for performing conditional updates. Ignores ConditionalCheckFailedExceptions, but returns true/false to indicate whether an update was performed
     * @private
     * @param {object} item - a dyno object
     * @param {object} opts - an object defining the conditional expression
     * @param {function} cb - a callback function to handle the response
     */
    function conditionalUpdate(item, opts, cb) {
        dyno.updateItem(key, item, opts, function(err) {
            if (err && err.code === 'ConditionalCheckFailedException')
                return cb(null, false);
            else if (err)
                return cb(err, false);
            cb(null, true);
        });
    }

    /**
     * Return dataset metadata or an empty object
     * @param {function} callback - a callback function to handle the response
     */
    metadata.getInfo = function(callback) {
        dyno.getItem(key, function(err, item) {
            if (err) return callback(err);
            info = item || {};
            callback(null, info);
        });
    };

    /**
     * Return the details for a given GeoJSON feature
     * @param {object} feature - a GeoJSON feature
     * @returns {object} an object describing the feature's size and extent
     */
    metadata.getFeatureInfo = function(feature) {
        var bounds = extent(feature);
        return {
            size: Buffer.byteLength(JSON.stringify(feature)),
            bounds: bounds,
            west: bounds[0],
            south: bounds[1],
            east: bounds[2],
            north: bounds[3]
        };
    };

    /**
     * Create and stores a metadata record with default values if no record exists. Returns true/false to indicate whether a record was created.
     * @private
     * @param {function} callback - a function to handle the response
     */
    metadata.defaultInfo = function(callback) {
        var info = {};
        info.put = {
            west: 180,
            south: 90,
            east: -180,
            north: -90,
            count: 0,
            size: 0,
            updated: +new Date()
        };

        var opts = { expected: {} };
        opts.expected.id = { NULL: [] };

        conditionalUpdate(info, opts, callback);
    };

    /**
     * Find all features in a dataset and bring metadata record up-to-date
     * @param {function} callback - a function to handle the response
     */
    metadata.calculateInfo = function(callback) {
        var info = {
            dataset: dataset,
            id: recordId,
            west: 180,
            south: 90,
            east: -180,
            north: -90,
            count: 0,
            size: 0,
            updated: +new Date()
        };

        var query = { dataset: { EQ: dataset }, id: { BEGINS_WITH: 'id!' } };
        var opts = { pages: 0 };

        dyno.query(query, opts)
            .on('error', callback)
            .pipe(through({ objectMode: true }, function(data, enc, cb) {
                info.count++;
                info.size = info.size + data.size;
                info.west = info.west > data.west ? data.west : info.west;
                info.south = info.south > data.south ? data.south : info.south;
                info.east = info.east < data.east ? data.east : info.east;
                info.north = info.north < data.north ? data.north : info.north;
                cb();
            }))
            .on('error', callback)
            .on('finish', function() {
                dyno.putItem(info, function(err) {
                    callback(err, info);
                });
            });
    };

    /**
     * Adjust the bounds in an existing metadata record. This operation **will not** create a metadata record if one does not exist.
     * @private
     * @param {number[]} bounds - bounds to add to the existing bounds for the dataset
     * @param {function} callback - a function to handle the response
     */
    metadata.adjustBounds = function(bounds, callback) {
        var q = queue();
        var labels = ['west', 'south', 'east', 'north'];

        bounds.forEach(function(bound, i) {
            var item = { put: {} };
            item.put[labels[i]] = bound;
            item.put.updated = +new Date();

            var opts = { expected: {} };
            opts.expected[labels[i]] =  i < 2 ? {GT: bound } : {LT: bound };
            q.defer(conditionalUpdate, item, opts);
        });

        q.awaitAll(callback);
    };

    /**
     * Increment/decrement the specified properties. This operation **will not** create a metadata record if one does not exist.
     * @private
     * @param {object} properties - an object describing the properties in increase or decrease
     * @param {function} callback - a function to handle the response
     */
    metadata.adjustProperties = function(properties, callback) {
        var item = { add: properties, put: {} };
        item.put.updated = +new Date();

        var opts = { expected: {} };
        opts.expected.id = { NOT_NULL: [] };

        conditionalUpdate(item, opts, callback);
    };

    /**
     * Given a GeoJSON feature, perform all required metadata updates. This operation **will** create a metadata record if one does not exist.
     * @param {object} feature - a GeoJSON feature being added to the dataset
     * @param {function} callback - a function to handle the response
     */
    metadata.addFeature = function(feature, callback) {
        var info = metadata.getFeatureInfo(feature);

        metadata.defaultInfo(function(err) {
            if (err) return callback(err);

            queue()
                .defer(metadata.adjustProperties, { count: 1, size: info.size })
                .defer(metadata.adjustBounds, info.bounds)
                .awaitAll(callback);
        });
    };

    /**
     * Given before and after states of a GeoJSON feature, perform all required metadata adjustments. This operation **will not** create a metadata record if one does not exist.
     * @param {object} from - a GeoJSON feature representing the state of the feature *before* the update
     * @param {object} to - a GeoJSON feature representing the state of the feature *after* the update
     * @param {function} callback - a function to handle the response
     */
    metadata.updateFeature = function(from, to, callback) {
        var fromInfo = metadata.getFeatureInfo(from);
        var toInfo = metadata.getFeatureInfo(to);
        var bounds = toInfo.bounds;
        var size = toInfo.size - fromInfo.size;

        queue()
            .defer(metadata.adjustProperties, { size: size })
            .defer(metadata.adjustBounds, bounds)
            .awaitAll(callback);
    };

    /**
     * Given a GeoJSON feature to remove, perform all required metadata updates. This operation **will not** create a metadata record if one does not exist. This operation **will not** shrink metadata bounds.
     * @param {object} feature - a GeoJSON feature to remove from the dataset
     * @param {function} callback - a function to handle the response
     */
    metadata.deleteFeature = function(feature, callback) {
        var info = metadata.getFeatureInfo(feature);

        queue()
            .defer(metadata.adjustProperties, { count: -1, size: -info.size })
            .awaitAll(callback);
    };

    return metadata;
};
