var dynamodb = require('./dynamodb');

module.exports = function(cb) {
    dynamodb.createTable(dynamodb.client, function(err) {
        if (err) throw new Error(err);
        return cb({
            rangeQuery: rangeQuery,
            createWriteStream: createWriteStream,
            createReadStream: createReadStream
        });
    });

    function rangeQuery(idx, cb) {
        var readStream = db.createReadStream({
            start: 'cell!' + idx[0],
            end: 'cell!' + idx[1]
        });
        readStream.pipe(concat(function(data) {
            cb(null, data);
        }));
    }

    function createWriteStream() {
        return db.createWriteStream();
    }

    function createReadStream() {
        return db.createReadStream();
    }
};
