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
    AWS = require('aws-sdk');

var coverOpts = {};
coverOpts.MAX_QUERY_CELLS = 100;
coverOpts.QUERY_MIN_LEVEL = 5;
coverOpts.QUERY_MAX_LEVEL = 5;
coverOpts.MAX_INDEX_CELLS = 100;
coverOpts.INDEX_MIN_LEVEL = 5;
coverOpts.INDEX_MAX_LEVEL = 5;
coverOpts.INDEX_POINT_LEVEL = 15;

module.exports = Cardboard;

function Cardboard(c) {
    if (!(this instanceof Cardboard)) return new Cardboard(c);

    AWS.config.update({
        accessKeyId: c.awsKey,
        secretAccessKey: c.awsSecret,
        region: c.region || 'us-east-1',

    });
    this.bucket = c.bucket || 'mapbox-s2';
    this.prefix = c.prefix || 'dev';
    this.s3 = new AWS.S3();
}

Cardboard.prototype.insert = function(primary, feature, layer, cb) {
    var indexes = geojsonCover.geometryIndexes(feature.geometry, coverOpts);
    var s3 = this.s3;
    var bucket = this.bucket;
    var prefix = this.prefix;
    if(!feature.properties) feature.properties = {};
    feature.properties.id = primary;
    log('indexing ' + primary + ' with ' + indexes.length + ' indexes');
    var q = queue(1);

    function updateCell(key, feature, cb) {
        s3.getObject({Key:key, Bucket: bucket}, getObjectResp);
        function getObjectResp(err, data) {
            if (err && err.code !== 'NoSuchKey') {
                console.log('Error Read', err);
                throw err;
            }
            var fc;
            if (data && data.Body) {
                fc = geobuf.geobufToFeatureCollection(data.Body);
                fc.features.push(feature);

            } else {
                fc = {type:'FeatureCollection', features:[feature]};
            }
            s3.putObject(
                {
                    Key:key,
                    Bucket:bucket,
                    Body: geobuf.featureCollectionToGeobuf(fc).toBuffer()
                },
                putObjectResp);

        }
        function putObjectResp(err, data) {
            if(err) console.log(err)
            cb(err, data)
        }
    }


    indexes.forEach(function(index) {
        var key =  [prefix, layer, 'cell', index].join('/');
        q.defer(updateCell, key, feature);
    });
    q.awaitAll(function(err, res) {
        cb(err);
    });
};

// Cardboard.prototype.createTable = function(tableName, callback) {
//     var table = require('./lib/table.json');
//     table.TableName = tableName;
//     this.dyno.createTable(table, callback);
// };

Cardboard.prototype.bboxQuery = function(input, layer, callback) {
    var indexes = geojsonCover.bboxQueryIndexes(input, false, coverOpts);
    var q = queue(100);
    var s3 = this.s3;
    var prefix = this.prefix;
    var bucket = this.bucket;
    log('querying with ' + indexes.length + ' indexes');
    console.time('query');
    indexes.forEach(function(idx) {

         function getCell(k, cb) {
            s3.getObject({
                Key: k,
                Bucket: bucket
            }, function(err, data){
                if(err && err.code !== 'NoSuchKey') {
                    console.error(err);
                    throw err;
                }
                cb(null, data);
            });
        }
        var key = [prefix, layer, 'cell', idx].join('/');
        q.defer(getCell, key);
    });
    q.awaitAll(function(err, res) {
        console.timeEnd('query');
        if (err) return callback(err);
        console.time('parse');


        var features = [];

        res = res.forEach(function(r) {
            if (r && r.Body) {
                features = features.concat(geobuf.geobufToFeatureCollection(r.Body).features);
            }
        });

        var features = _(features).compact().sortBy(function(a) {
             return a.properties.id;
        }).value();

        features = uniq(features, function(a, b) {
            return a.properties.id !== b.properties.id;
        }, true);
        console.timeEnd('parse');

        features= features.map(function(f){
            return {val:f};
        })
        callback(err, features);
    });
};

// Cardboard.prototype.dump = function(cb) {
//     return this.dyno.scan(cb);
// };
//
// Cardboard.prototype.dumpGeoJSON = function(callback) {
//     return this.dyno.scan(function(err, res) {
//         if (err) return callback(err);
//         return callback(null, {
//             type: 'FeatureCollection',
//             features: res.items.map(function(f) {
//                 return {
//                     type: 'Feature',
//                     properties: {
//                         key: f.key
//                     },
//                     geometry: new s2.S2Cell(new s2.S2CellId()
//                         .fromToken(
//                             f.key.split('!')[1])).toGeoJSON()
//                 };
//             })
//         });
//     });
// };
//
// Cardboard.prototype.export = function(_) {
//     return this.dyno.scan()
//         .pipe(through({ objectMode: true }, function(data, enc, cb) {
//              this.push(geobuf.geobufToFeature(data.val));
//              cb();
//         }))
//         .pipe(geojsonStream.stringify());
// };

Cardboard.prototype.geojsonCover = geojsonCover;
