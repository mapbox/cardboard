var levelup = require('levelup'),
    s2 = require('s2'),
    through = require('through2'),
    _ = require('lodash'),
    geojsonStream = require('geojson-stream'),
    concat = require('concat-stream'),
    normalize = require('geojson-normalize'),
    uniq = require('uniq'),
    queue = require('queue-async');

module.exports = Cardboard;

function Cardboard(name) {
    if (!(this instanceof Cardboard)) return new Cardboard(name);
    this.db = levelup(name);
}

Cardboard.prototype.insert = function(primary, feature) {
    var ws = this.db.createWriteStream(),
        indexes = indexGeoJSON(feature.geometry, primary),
        featureStr = JSON.stringify(feature);

    indexes.forEach(writeFeature);
    ws.end();

    function writeFeature(index) {
        ws.write({ key: index, value: featureStr });
    }
};

Cardboard.prototype.intersects = function(input, callback) {
    if (typeof input == 'object' && input.length == 2) {
        input = { type: 'Point', coordinates: input };
    }
    var indexes = indexGeoJSON(normalize(input).features[0].geometry);
    var q = queue(1);
    var db = this.db;
    indexes.forEach(function(idx) {
        q.defer(function(idx, cb) {
            db.createReadStream({
                start: idx
            }).pipe(concat(function(data) {
                cb(null, data);
            }));
        }, idx);
    });
    q.awaitAll(function(err, res) {
        var flat = _.flatten(res);
        uniq(flat, function(a, b) {
            return a.key.split('!')[2] !== b.key.split('!')[2];
        });
        console.log(flat);
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

function indexGeoJSON(geom, primary) {
    var i, j;
    if (geom.type === 'Point') {
        return pointIndexes(geom.coordinates, 0, 30, primary);
    }
    if (geom.type === 'Polygon') {
        var indexes = [];
        for (i = 0; i < geom.coordinates.length; i++) {
            var toAdd = polygonIndexes(geom.coordinates[i], {
                min: 1,
                max: 30,
                max_cells: 200
            }, primary);
            for (var k = 0; k < toAdd.length; k++) {
                indexes.push(toAdd[k]);
            }
        }
        return indexes;
    }
    if (geom.type === 'MultiPolygon') {
        var indexes = [];
        for (i = 0; i < geom.coordinates.length; i++) {
            for (j = 0; j < geom.coordinates[i].length; j++) {
                var toAdd = polygonIndexes(geom.coordinates[i][j], {
                    min: 1,
                    max: 30,
                    max_cells: 200
                }, primary);
                for (var k = 0; k < toAdd.length; k++) {
                    indexes.push(toAdd[k]);
                }
            }
        }
        return indexes;
    }
    return [];
}

function pointIndexes(coords, min, max, primary) {
    var id = new s2.S2CellId(new s2.S2LatLng(coords[1], coords[0])),
        strings = [];

    for (var i = min; i <= max; i++) {
        strings.push(cellString(id.parent(i), primary));
    }

    return strings;
}

function polygonIndexes(coords, options, primary) {
    var cover = s2.getCover(coords.map(function(c) {
        return new s2.S2LatLng(c[1], c[0]);
    }), options || {});
    var out = [];
    for (var i = 0; i < cover.length; i++) {
        out.push(cellString(cover[i].id(), primary));
    }
    return out;
}

function cellString(cell, primary) {
    if (primary !== undefined) {
        return 'cell!' + cell.toToken() + '!' + primary;
    } else {
        return 'cell!' + cell.toToken();
    }
}
