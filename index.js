var s2 = require('s2'),
    through = require('through2'),
    _ = require('lodash'),
    geojsonStream = require('geojson-stream'),
    concat = require('concat-stream'),
    normalize = require('geojson-normalize'),
    geojsonCover = require('./lib/geojsoncover'),
    uniq = require('uniq'),
    queue = require('queue-async');

module.exports = Cardboard;

function Cardboard(db) {
    if (!(this instanceof Cardboard)) return new Cardboard(db);
    this.db = db;
}

Cardboard.prototype.insert = function(primary, feature) {
    var ws = this.db.createWriteStream(),
        indexes = geojsonCover(feature.geometry),
        featureStr = JSON.stringify(feature);

    indexes.forEach(writeFeature);
    ws.end();
    return this;

    function writeFeature(index) {
        ws.write({ key: 'cell!' + index + '!' + primary, value: featureStr });
    }
};

Cardboard.prototype.intersects = function(input, callback) {
    if (typeof input == 'object' && input.length == 2) {
        input = { type: 'Point', coordinates: input };
    }
    var indexes = geojsonCover(normalize(input).features[0].geometry);
    var q = queue(1);
    var db = this.db;
    indexes.forEach(function(idx) {
        q.defer(function(idx, cb) {
            var readStream = db.createReadStream({
                start: 'cell!' + idx
            });
            readStream.pipe(concat(function(data) {
                cb(null, data);
            }));
            readStream.on('data', function(data) {
                if (data.key.indexOf('cell!' + idx) !== 0) {
                    readStream.emit('end');
                    readStream.destroy();
                }
            });
        }, idx);
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

function cellString(cell, primary) {
    if (primary !== undefined) {
        return 'cell!' + cell.toToken() + '!' + primary;
    } else {
        return 'cell!' + cell.toToken();
    }
}
