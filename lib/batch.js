var url = require('url');
var queue = require('queue-async');

module.exports = function(config) {
    if (!config.bucket) throw new Error('No bucket set');
    if (!config.prefix) throw new Error('No s3 prefix set');
    if (!config.s3) config.s3 = new AWS.S3(config);
    if (!config.dyno) config.dyno = Dyno(config);

    var utils = require('./utils')(config);

    var batch = {};

    batch.put = function(collection, dataset, callback) {
        var records = [];
        var s3objects = [];

        var encoded;
        var q = queue(150);

        for (var i = 0; i < collection.features.length; i++) {
            try { encoded = utils.toDatabaseRecord(collection.features[i], dataset); }
            catch (err) { return callback(err); }

            records.push(encoded[0]);
            q.defer(config.s3.putObject.bind(config.s3), encoded[1]);
        }

        q.awaitAll(function(err) {
            if (err) return callback(err);
            config.dyno.putItems(records, function(err, items) {
                if (err) return callback(err);

                var response = JSON.parse(JSON.stringify(collection));
                response.features = response.features.map(function(feature, j) {
                    feature.id = records[j].id.split('!')[1];
                    return feature;
                });

                callback(null, response);
            });
        });
    };

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