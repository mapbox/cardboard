var queue = require('queue-async');
var extent = require('geojson-extent');
var through = require('through2');

module.exports = function(dyno, dataset) {    

    var metadata = {};
    var recordId = metadata.recordId = 'metadata!' + dataset;
    var key = metadata.key = { id: recordId, dataset: dataset };

    // Helper routine for performing conditional updates.
    // Ignores ConditionalCheckFailedExceptions, but returns true/false to 
    // indicate whether an update was performed
    function conditionalUpdate(item, opts, cb) {
        dyno.updateItem(key, item, opts, function(err) {
            if (err && err.code === 'ConditionalCheckFailedException')
                return cb(null, false);
            else if (err)
                return cb(err, false);
            cb(null, true);
        });
    }

    // Return dataset metadata or an empty object
    metadata.getInfo = function(callback) {
        var query = {
            id: { 'EQ': recordId },
            dataset: { 'EQ': dataset }
        };
        dyno.query(query, function(err, res) {
            if (err) return callback(err);
            var info = res.count < 1 ? {} : res.items[0];
            callback(null, info);
        });
    };

    // Return the details for a given GeoJSON feature
    metadata.getFeatureInfo = function(feature) {
        var bounds = extent(feature);
        return {
            size: JSON.stringify(feature).length,
            bounds: bounds,
            west: bounds[0],
            south: bounds[1],
            east: bounds[2],
            north: bounds[3]
        };
    };

    // Create a metadata record with default values if no record exists
    // Returns true/false to indicate whether a record was created
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

        var opts = { expected: {} }
        opts.expected.id = { ComparisonOperator: 'NULL' };

        conditionalUpdate(info, opts, callback);
    };

    // Find all features in a dataset and bring metadata record up-to-date
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

        var query = { dataset: { EQ: dataset }, id: { BEGINS_WITH: 'id!' } },
            opts = { pages: 0 };

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

    // Adjust the bounds in an existing metadata record
    // This operation **will not** create a metadata record if one does not exist
    metadata.adjustBounds = function(bounds, callback) {
        var q = queue();
        var labels = [ 'west', 'south', 'east', 'north' ];

        bounds.forEach(function(bound, i) {
            var item = { put: {} };
            item.put[labels[i]] = bound;
            item.put.updated = +new Date();

            var opts = { expected: {} };
            opts.expected[labels[i]] = {
                AttributeValueList: [ { N: bound.toString() } ],
                ComparisonOperator: i < 2 ? 'GT' : 'LT'
            };

            q.defer(conditionalUpdate, item, opts);
        });

        q.awaitAll(callback);
    };

    // Increment/decrement the specified properties
    // This operation **will not** create a metadata record if one does not exist
    metadata.adjustProperties = function(properties, callback) {
        var item = { add: properties, put: {} };
        item.put.updated = +new Date();

        var opts = { expected: {} }
        opts.expected.id = { ComparisonOperator: 'NOT_NULL' };

        conditionalUpdate(item, opts, callback);
    };

    // Given a GeoJSON feature, perform all required metadata updates
    // This operation **will** create a metadata record if one does not exist
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

    // Given before and after states of a GeoJSON feature, perform all required metadata adjustments
    // This operation **will not** create a metadata record if one does not exist
    metadata.updateFeature = function(from, to, callback) {
        var fromInfo = metadata.getFeatureInfo(from),
            toInfo = metadata.getFeatureInfo(to),
            bounds = toInfo.bounds,
            size = toInfo.size - fromInfo.size;

        queue()
            .defer(metadata.adjustProperties, { size: size })
            .defer(metadata.adjustBounds, bounds)
            .awaitAll(callback);
    };

    // Given a GeoJSON feature to remove, perform all required metadata updates
    // This operation **will not** create a metadata record if one does not exist
    // This operation **will not** shrink metadata bounds
    metadata.deleteFeature = function(feature, callback) {
        var info = metadata.getFeatureInfo(feature);

        queue()
            .defer(metadata.adjustProperties, { count: -1, size: -info.size })
            .awaitAll(callback);
    };

    return metadata;
}
