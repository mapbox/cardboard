var s2 = require('s2'),
    through = require('through2'),
    _ = require('lodash'),
    geojsonStream = require('geojson-stream'),
    concat = require('concat-stream'),
    geojsonCover = require('./lib/geojsoncover'),
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

Cardboard.prototype.insert = function(primary, feature, cb) {
    var indexes = geojsonCover.geometry(feature.geometry);
    var dyno = this.dyno;
    log('indexing ' + primary + ' with ' + indexes.length + ' indexes');
    var q = queue(50);
    indexes.forEach(function(index) {
        q.defer(dyno.putItem, {
            id: 'cell!' + index + '!' + primary,
            layer: 'default',
            val: geobuf.featureToGeobuf(feature).toBuffer()
        });
    });
    q.awaitAll(function(err, res) {
        cb(err);
    });
};

Cardboard.prototype.bboxQuery = function(input, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input);
    var q = queue(100);
    var dyno = this.dyno;
    log('querying with ' + indexes.length + ' indexes');
    indexes.forEach(function(idx) {
        q.defer(dyno.query,
            { id: {'BETWEEN': ['cell!' + idx[0], 'cell!' + idx[1]]},
              layer: {'EQ': 'default'}
          });
    });
    q.awaitAll(function(err, res) {
        if (err) return callback(err);

        res = res.map(function(r) {
            return r.items.map(function(i){
                i.val = geobuf.geobufToFeature(i.val);
                return i;
            });
        });

        var flat = _(res).chain().flatten().sortBy(function(a){
            return a.id.split('!')[2];
        }).value();

        flat = uniq(flat, function(a, b) {
            return a.id.split('!')[2] !== b.id.split('!')[2];
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
