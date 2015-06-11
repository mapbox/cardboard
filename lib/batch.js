var url = require('url');
var queue = require('queue-async');
var geobuf = require('geobuf');

module.exports = function(config) {
    if (!config.bucket) throw new Error('No bucket set');
    if (!config.prefix) throw new Error('No s3 prefix set');
    if (!config.s3) config.s3 = new AWS.S3(config);
    if (!config.dyno) config.dyno = Dyno(config);

    var utils = require('./utils')(config);

    var batch = {};

    /**
     * Insert or update a set of GeoJSON features
     * @param {object} collection - a GeoJSON FeatureCollection containing features to insert and/or update
     * @param {string} dataset - the name of the dataset that these features belongs to
     * @param {function} callback - the callback function to handle the response
     */
    batch.put = function(collection, dataset, callback) {
        var records = [];
        var geobufs = [];
        var s3objects = [];

        var encoded;
        var q = queue(150);

        for (var i = 0; i < collection.features.length; i++) {
            try { encoded = utils.toDatabaseRecord(collection.features[i], dataset); }
            catch (err) { return callback(err); }

            records.push(encoded[0]);
            geobufs.push(encoded[1].Body);
            q.defer(config.s3.putObject.bind(config.s3), encoded[1]);
        }

        q.awaitAll(function(err) {
            if (err) return callback(err);
            config.dyno.putItems(records, function(err, items) {
                if (err) return callback(err);

                var features = geobufs.map(geobuf.geobufToFeature.bind(geobuf));

                callback(null, { type: 'FeatureCollection', features: features });
            });
        });
    };

    /**
     * Remove a set of features
     * @param {string[]} ids - an array of feature ids to remove
     * @param {string} dataset - the name of the dataset that these features belong to
     * @param {function} callback - the callback function to handle the response
     */
    batch.remove = function(ids, dataset, callback) {
        var keys = ids.map(function(id) {
            return { dataset: dataset, id: 'id!' + id };
        });

        config.dyno.deleteItems(keys, function(err) {
            callback(err);
        });
    };

    return batch;
};
