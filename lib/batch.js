var url = require('url');
var queue = require('queue-async');
var geobuf = require('geobuf');
var _ = require('lodash');
var Dyno = require('dyno');

module.exports = function(config) {
    if (!config.bucket) throw new Error('No bucket set');
    if (!config.prefix) throw new Error('No s3 prefix set');
    if (!config.s3) config.s3 = new AWS.S3(config);
    if (!config.dyno) config.dyno = Dyno(config);

    var utils = require('./utils')(config);

    /**
     * A module for batch requests
     * @name cardboard.batch
     */
    var batch = {};

    /**
     * Insert or update a set of GeoJSON features
     * @static
     * @memberof cardboard.batch
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
                if (err && err.unprocessed) {
                    var table = Object.keys(err.unprocessed)[0];
                    var unprocessed = err.unprocessed[table].map(function(item) {
                        var id = utils.idFromRecord(item.PutRequest.Item);
                        var i = _.findIndex(records, function(record) {
                            return utils.idFromRecord(record) === id;
                        });

                        return geobuf.geobufToFeature(geobufs[i]);
                    });

                    err.unprocessed = { type: 'FeatureCollection', features: unprocessed };
                }

                if (err) return callback(err);

                var features = geobufs.map(geobuf.geobufToFeature.bind(geobuf));
                callback(null, { type: 'FeatureCollection', features: features });
            });
        });
    };

    batch.get = function(ids, dataset, callback) {
        var results= []
        ids.forEach(function(id){
            var key = { dataset: dataset, id: 'id!' + primary };
            config.dyno.getItem(key, function(err, item) {
                //if (err) return callback(err);
                if (!item) results.push({'id':id, 'error': 'No feature of this ID'});
                utils.resolveFeatures([item], function(err, features) {
                    //if (err) return callback(err);
                    results.push(features.features[0]);
                });
            });
        })
        callback(null, results);
    }


    /**
     * Remove a set of features
     * @static
     * @memberof cardboard.batch
     * @param {string[]} ids - an array of feature ids to remove
     * @param {string} dataset - the name of the dataset that these features belong to
     * @param {function} callback - the callback function to handle the response
     */
    batch.remove = function(ids, dataset, callback) {
        var keys = ids.map(function(id) {
            return { dataset: dataset, id: 'id!' + id };
        });

        config.dyno.deleteItems(keys, function(err) {
            if (err && err.unprocessed) {
                var table = Object.keys(err.unprocessed)[0];
                err.unprocessed = err.unprocessed[table].map(function(item) {
                    return utils.idFromRecord(item.DeleteRequest.Key);
                });
            }

            callback(err);
        });
    };

    return batch;
};
