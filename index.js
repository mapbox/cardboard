var levelup = require('levelup'),
    s2index = require('s2-index'),
    through = require('through2'),
    geojsonStream = require('geojson-stream'),
    normalize = require('geojson-normalize'),
    queue = require('queue-async');

module.exports = Cardboard;

function Cardboard(name) {
    if (!(this instanceof Cardboard)) {
        return new Cardboard(name);
    }

    this.db = levelup(name);
}

Cardboard.prototype.importGeoJSON = function(_) {
    var features = normalize(_).features,
        ws = this.db.createWriteStream();

    features.forEach(getIndexes);

    function getIndexes(feature) {
        var indexes = indexGeoJSON(feature.geometry),
            featureStr = JSON.stringify(feature);

        indexes.forEach(writeFeature);
        ws.end();

        function writeFeature(index) {
            ws.write({ key: index, value: featureStr });
        }
    }
};

Cardboard.prototype.query = function(_, callback) {
    if (typeof _ == 'object' && _.length == 2) {
        _ = { type: 'Point', coordinates: _ };
    }
    var indexes = indexGeoJSON(normalize(_).features[0].geometry);
    var q = queue(1);
    var db = this.db;
    indexes.forEach(function(idx) {
        q.defer(function(idx, cb) {
            db.get(idx, function(err, val) {
                if (err) {
                    return cb();
                } else {
                    return cb(null, val);
                }
            });
        }, idx);
    }.bind(this));
    q.awaitAll(callback);
};

Cardboard.prototype.dump = function(_) {
    return this.db.createReadStream();
};

Cardboard.prototype.export = function(_) {
    return this.db.createReadStream()
        .pipe(through({ objectMode: true }, function(data, enc, cb) {
            this.push(JSON.parse(data.value));
            cb();
        }))
        .pipe(geojsonStream.stringify());
};

function indexGeoJSON(geom) {
    if (geom.type === 'Point') {
        return s2index.point(geom.coordinates, 6, 12);
    }
    if (geom.type === 'Polygon') {
        return geom.coordinates.reduce(function(mem, ring) {
            return mem.concat(s2index.polygon(ring, {
                min: 6,
                max: 12,
                max_cells: 100
            }));
        }, []);
    }
    return [];
}
