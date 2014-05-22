var s2 = require('s2'),
    through = require('through2'),
    _ = require('lodash'),
    geojsonStream = require('geojson-stream'),
    concat = require('concat-stream'),
    geojsonCover = require('./lib/geojsoncover'),
    uniq = require('uniq'),
    queue = require('queue-async');

var DEBUG = true;

function log(s) { if (DEBUG) console.log(s); }

module.exports = Cardboard;

function Cardboard(db) {
    if (!(this instanceof Cardboard)) return new Cardboard(db);
    this.db = db;
}

Cardboard.prototype.insert = function(primary, feature, cb) {
    var ws = this.db.createWriteStream(),
        indexes = geojsonCover.geometry(feature.geometry),
        featureStr = JSON.stringify(feature);

    log('indexing ' + primary + ' with ' + indexes.length + ' indexes');
    indexes.forEach(writeFeature);
    ws.end();
    ws.on('close', cb);

    function writeFeature(index) {
        ws.write({ key: 'cell!' + index + '!' + primary, value: featureStr });
    }
};

Cardboard.prototype.bboxQuery = function(input, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input);
    var q = queue(1);
    var db = this.db;
    log('querying with ' + indexes.length + ' indexes');
    indexes.forEach(function(idx) {
        q.defer(db.rangeQuery, idx);
    });
    q.awaitAll(function(err, res) {
        var flat = _.flatten(res);
        uniq(flat, function(a, b) {
            return a.key.split('!')[2] !== b.key.split('!')[2];
        });
        callback(err, flat);
    });
};

Cardboard.prototype.dump = function(_) {
    return this.db.createReadStream();
};

Cardboard.prototype.dumpGeoJSON = function(_) {
    return this.db.createReadStream()
        .pipe(through({ objectMode: true }, function(data, enc, cb) {
            this.push({
                type: 'Feature',
                properties: {
                    key: data.key
                },
                geometry: new s2.S2Cell(new s2.S2CellId()
                    .fromToken(
                        data.key.split('!')[1])).toGeoJSON()
            });
            cb();
        }))
        .pipe(geojsonStream.stringify());
};

Cardboard.prototype.export = function(_) {
    return this.db.createReadStream()
        .pipe(through({ objectMode: true }, function(data, enc, cb) {
            this.push(JSON.parse(data.value));
            cb();
        }))
        .pipe(geojsonStream.stringify());
};
