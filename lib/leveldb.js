var concat = require('concat-stream');

module.exports = function(db) {
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

    return {
        rangeQuery: rangeQuery,
        createWriteStream: createWriteStream,
        createReadStream: createReadStream
    };
};
