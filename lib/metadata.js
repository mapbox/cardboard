var queue = require('queue-async');
var extent = require('geojson-extent');

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
            size: 0
        };

        var opts = { expected: {} }
        opts.expected.id = { ComparisonOperator: 'NULL' };

        conditionalUpdate(info, opts, callback);
    }

    // Adjust the bounds in an existing metadata record
    // This operation **will not** create a metadata record if one does not exist
    metadata.adjustBounds = function(bounds, callback) {
        var q = queue();
        var labels = [ 'west', 'south', 'east', 'north' ];

        bounds.forEach(function(bound, i) {
            var item = { put: {} };
            item.put[labels[i]] = bound;

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
        var item = { add: properties };

        var opts = { expected: {} }
        opts.expected.id = { ComparisonOperator: 'NOT_NULL' };

        conditionalUpdate(item, opts, callback);
    }

    // Given a GeoJSON feature, perform all required metadata updates
    // This operation **will** create a metadata record if one does not exist
    metadata.addFeature = function(feature, callback) {
        var bounds = extent(feature);
        var size = JSON.stringify(feature).length;

        metadata.defaultInfo(function(err) {
            if (err) return callback(err);

            queue()
                .defer(metadata.adjustProperties, { count: 1, size: size })
                .defer(metadata.adjustBounds, bounds)
                .awaitAll(callback);
        });
    };

    // Given a GeoJSON feature to remove, perform all required metadata updates
    // This operation **will not** create a metadata record if one does not exist
    // This operation **will not** shrink metadata bounds
    metadata.deleteFeature = function(feature, callback) {
        var size = JSON.stringify(feature).length;
        queue()
            .defer(metadata.adjustProperties, { count: -1, size: -size })
            .awaitAll(callback);
    }

    return metadata;
}
