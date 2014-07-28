var s2 = require('s2'),
    through = require('through2'),
    _ = require('lodash'),
    geojsonStream = require('geojson-stream'),
    concat = require('concat-stream'),
    geojsonCover = require('geojson-cover'),
    uniq = require('uniq'),
    geobuf = require('geobuf'),
    log = require('debug')('cardboard'),
    queue = require('queue-async'),
    Dyno = require('dyno');

var emptyFeatureCollection = {
    type: 'FeatureCollection',
    features: []
};

module.exports = Cardboard;

function Cardboard(c) {
    if (!(this instanceof Cardboard)) return new Cardboard(c);
    this.dyno = Dyno(c);
}

Cardboard.prototype.insert = function(primary, feature, layer, cb) {
    var indexes = geojsonCover.geometryIndexes(feature.geometry);
    var dyno = this.dyno;
    log('indexing ' + primary + ' with ' + indexes.length + ' indexes');
    var q = queue(50);
    indexes.forEach(function(index) {
        console.log('putting', 'cell!' + index, layer);
        q.defer(dyno.putItem, {
            id: 'cell!' + index,
            layer: layer,
            val: geobuf.featureToGeobuf(feature).toBuffer()
        });
    });
    q.awaitAll(function(err, res) {
        cb(err);
    });
};

Cardboard.prototype.createTable = function(tableName, callback) {
    var table = require('./lib/table.json');
    table.TableName = tableName;
    this.dyno.createTable(table, callback);
};

Cardboard.prototype.bboxQuery = function(input, layer, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input);
    var q = queue(100);
    var dyno = this.dyno;
    log('querying with ' + indexes.length + ' indexes');
    console.time('query');
    indexes.forEach(function(idx) {
        console.log('getting', 'cell!' + idx, layer);
        q.defer(dyno.getItem, {
            layer: layer,
            id: 'cell!' + idx
        });
    });
    q.awaitAll(function(err, res) {
        console.timeEnd('query');
        if (err) return callback(err);

        res = res.map(function(r) {
            if (r && r.Item) {
                r.Item.val = geobuf.geobufToFeature(r.Item.val)
            }
            return r.Item;
        });

        var flat = _(res).chain().flatten().compact().sortBy(function(a) {
            return a.id.split('!')[1];
        }).value();

        flat = uniq(flat, function(a, b) {
            return a.id.split('!')[1] !== b.id.split('!')[1];
        }, true);

        callback(err, flat);
    });
};

Cardboard.prototype.dump = function(cb) {
    return this.dyno.scan(cb);
};

Cardboard.prototype.dumpGeoJSON = function(callback) {
    return this.dyno.scan(function(err, res) {
        if (err) return callback(err);
        return callback(null, {
            type: 'FeatureCollection',
            features: res.items.map(function(f) {
                return {
                    type: 'Feature',
                    properties: {
                        key: f.key
                    },
                    geometry: new s2.S2Cell(new s2.S2CellId()
                        .fromToken(
                            f.key.split('!')[1])).toGeoJSON()
                };
            })
        });
    });
};

Cardboard.prototype.export = function(_) {
    return this.dyno.scan()
        .pipe(through({ objectMode: true }, function(data, enc, cb) {
             this.push(geobuf.geobufToFeature(data.val));
             cb();
        }))
        .pipe(geojsonStream.stringify());
};

Cardboard.prototype.geojsonCover = geojsonCover;
