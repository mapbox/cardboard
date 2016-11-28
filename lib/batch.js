var queue = require('queue-async');

module.exports = function(cardboard) {
    var batch = {};

    batch.put = function(fc, dataset, cb) {
        var q = queue();
        fc.features.forEach(function(f) {
            q.defer(function(done) {
                cardboard.put(f, dataset, done);     
            });
        });

        q.awaitAll(function(err, items) {
            if (err) return cb(err);
            cb(null, {features:items, type: 'FeatureCollection'});
        });
    };

    batch.del = function(ids, dataset, cb) {
        var q = queue();
        ids.forEach(function(id) {
            q.defer(function(done) {
                cardboard.del(id, dataset, done);     
            });
        });

        q.awaitAll(cb);
    };

    return batch;
}
