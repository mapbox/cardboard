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

var MAX_ENTRY_BYTES = 64 * 1000; // 64KB

module.exports = Cardboard;

function Cardboard(c) {
    if (!(this instanceof Cardboard)) return new Cardboard(c);
    this.dyno = Dyno(c);
}

Cardboard.prototype.insert = function(primary, feature, layer, cb) {
    var indexes = geojsonCover.geometryIndexes(feature.geometry);
    var dyno = this.dyno;
    var q = queue(50);
    var buf = geobuf.featureToGeobuf(feature).toBuffer();

    indexes.forEach(writeIndex);
    writeIndex(null);

    function writeIndex(index) {
        var id = (index === null) ?
            ('id!' + primary) :
            ('cell!' + index + '!' + primary);
        var chunks = [], part = 0;
        var chunkBytes = MAX_ENTRY_BYTES - id.length;
        for (var start = 0; start < buf.length;) {
            q.defer(dyno.putItem, {
                id: id + '!' + part,
                layer: layer,
                val: buf.slice(start, start + chunkBytes)
            });
            start += chunkBytes;
            part++;
        }
        if (part > 1) {
            log('length: ' + buf.length + ', chunks: ' + part + ', chunkBytes: ' + chunkBytes);
        }
        if (part === 0) {
            log('part of 0!');
        }
    }

    q.awaitAll(function(err, res) {
        cb(err);
    });
};

Cardboard.prototype.createTable = function(tableName, callback) {
    var table = require('./lib/table.json');
    table.TableName = tableName;
    this.dyno.createTable(table, callback);
};

Cardboard.prototype.del = function(primary, layer, callback) {
    var dyno = this.dyno;
    this.get(primary, layer, function(err, res) {
        if (err) return callback(err);
        var indexes = geojsonCover.geometryIndexes(res[0].val.geometry);
        var params = {
            RequestItems: {}
        };
        function deleteId(id) {
            return {
                DeleteRequest: {
                    Key: { id: { S: id }, layer: { S: layer } }
                }
            };
        }
        // TODO: how to get table name properly here.
        params.RequestItems.geo = [
            deleteId('id!' + primary + '!0')
        ];
        var parts = partsRequired(res[0].val);
        for (var i = 0; i < indexes.length; i++) {
            for (var j = 0; j < parts; j++) {
                params.RequestItems.geo.push(deleteId('cell!' + indexes[i] + '!' + primary + '!' + j));
            }
        }
        dyno.batchWriteItem(params, function(err, res) {
            callback(null);
        });
    });
};

Cardboard.prototype.delLayer = function(layer, callback) {
    var dyno = this.dyno;
    this.listIds(layer, function(err, res) {
        var params = {
            RequestItems: {
                geo: res.map(function(id) {
                    return {
                        DeleteRequest: {
                            Key: {
                                layer: { S: layer },
                                id: { S: id }
                            }
                        }
                    }
                })
            }
        };
        dyno.batchWriteItem(params, function(err, res) {
            callback(err);
        });
    });
};

function partsRequired(feature) {
    var buf = geobuf.featureToGeobuf(feature).toBuffer();
    return Math.ceil(buf.length / MAX_ENTRY_BYTES);
}

Cardboard.prototype.get = function(primary, layer, callback) {
    var dyno = this.dyno;
    dyno.query({
        id: { 'BEGINS_WITH': 'id!' + primary },
        layer: { 'EQ': layer }
    }, { pages: 0 }, function(err, res) {
        if (err) return callback(err);
        callback(err, parseQueryResponse([res]));
    });
};

Cardboard.prototype.list = function(layer, callback) {
    var dyno = this.dyno;
    dyno.query({
        layer: { 'EQ': layer },
        id: { 'BEGINS_WITH': 'id!' }
    }, function(err, res) {
        if (err) return callback(err);
        callback(err, parseQueryResponseId([res]));
    });
};

Cardboard.prototype.listIds = function(layer, callback) {
    var dyno = this.dyno;
    dyno.query({
        layer: { 'EQ': layer }
    }, {
        attributes: ['id']
    }, function(err, res) {
        if (err) return callback(err);
        callback(err, res.items.map(function(_) {
            return _.id;
        }));
    });
};

Cardboard.prototype.listLayers = function(callback) {
    var dyno = this.dyno;
    dyno.scan(
        {
            attributes: ['layer']
        },
        function(err, res) {
            if (err) return callback(err);
            var layers = _.uniq(res.items.map(function(item){
                return item.layer;
            }));
            callback(err, layers);
        }
    );
};

Cardboard.prototype.bboxQuery = function(input, layer, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input);
    var q = queue(100);
    var dyno = this.dyno;
    log('querying with ' + indexes.length + ' indexes');
    indexes.forEach(function(idx) {
        q.defer(
            dyno.query, {
                id: { 'BETWEEN': [ 'cell!' + idx[0], 'cell!' + idx[1] ] },
                layer: { 'EQ': layer }
            },
            { pages: 0 }
        );
    });
    q.awaitAll(function(err, res) {
        if (err) return callback(err);
        callback(err, parseQueryResponse(res));
    });
};

function parseQueryResponseId(res) {
    res = res.map(function(r) {
        return r.items.map(function(i) {
            i.id_parts = i.id.split('!');
            return i;
        });
    });

    var flat = _(res).chain().flatten().sortBy(function(a) {
        return a.id_parts[1];
    }).value();

    flat = uniq(flat, function(a, b) {
        return a.id_parts[1] !== b.id_parts[1] ||
            a.id_parts[2] !== b.id_parts[2];
    }, true);

    flat = _.groupBy(flat, function(_) {
        return _.id_parts[1];
    });

    flat = _.values(flat);

    flat = flat.map(function(_) {
        var concatted = Buffer.concat(_.map(function(i) {
            return i.val;
        }));
        _[0].val = concatted;
        return _[0];
    });

    return flat.map(function(i) {
        i.val = geobuf.geobufToFeature(i.val);
        return i;
    });
}

function parseQueryResponse(res) {
    res = res.map(function(r) {
        return r.items.map(function(i) {
            i.id_parts = i.id.split('!');
            return i;
        });
    });

    var flat = _(res).chain().flatten().sortBy(function(a) {
        return a.id_parts[2];
    }).value();

    flat = uniq(flat, function(a, b) {
        return a.id_parts[2] !== b.id_parts[2] ||
            a.id_parts[3] !== b.id_parts[3];
    }, true);

    flat = _.groupBy(flat, function(_) {
        return _.id_parts[2];
    });

    flat = _.values(flat);

    flat = flat.map(function(_) {
        var concatted = Buffer.concat(_.map(function(i) {
            return i.val;
        }));
        _[0].val = concatted;
        return _[0];
    });

    return flat.map(function(i) {
        i.val = geobuf.geobufToFeature(i.val);
        return i;
    });
}

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
