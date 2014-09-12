var s2 = require('s2');
var through = require('through2');
var _ = require('lodash');
var geojsonStream = require('geojson-stream');
var geojsonNormalize = require('geojson-normalize')
var concat = require('concat-stream');
var geojsonCover = require('geojson-cover');
var coverOpts = require('./lib/coveropts');
var Metadata = require('./lib/metadata');
var uniq = require('uniq');
var geobuf = require('geobuf');
var log = require('debug')('cardboard');
var queue = require('queue-async');
var Dyno = require('dyno');
var AWS = require('aws-sdk');
var extent = require('geojson-extent');
var distance = require('turf-distance');
var point = require('turf-point');
var cuid = require('cuid');

var MAX_GEOMETRY_SIZE = 1024*10;  //10KB
var LARGE_INDEX_DISTANCE = 50; //bbox more then 100 miles corner to corner.


module.exports = function Cardboard(c) {
    var cardboard = {};

    AWS.config.update({
        accessKeyId: c.awsKey,
        secretAccessKey: c.awsSecret,
        region: c.region || 'us-east-1',
    });

    // allow for passed in config object to override s3 objects for mocking in tests
    var s3 = c.s3 || new AWS.S3();
    var dyno = Dyno(c);
    if(!c.bucket) throw new Error('No bucket set');
    var bucket = c.bucket;
    if(!c.prefix) throw new Error('No s3 prefix set');
    var prefix = c.prefix;

    cardboard.put = function(feature, dataset, callback) {
        var isUpdate = feature.hasOwnProperty('id'),
            f = isUpdate ? _.clone(feature) : _.extend({ id: cuid() }, feature),
            metadata = Metadata(dyno, dataset),
            info = metadata.getFeatureInfo(f),
            timestamp = (+new Date()),
            primary = f.id,
            buf = geobuf.featureToGeobuf(f).toBuffer(),
            cell = 'cell', // TODO: replace with function call that gets a single-cell index token 
            useS3 = buf.length > MAX_GEOMETRY_SIZE,
            s3Key = [prefix, dataset, primary, timestamp].join('/'),
            s3Params = { Bucket: bucket, Key: s3Key, Body: buf };
        
        var item = {
            dataset: dataset,
            id: 'id!' + primary,
            cell: cell,
            size: info.size,
            west: info.west,
            south: info.south,
            east: info.east,
            north: info.north
        };

        if (f.properties.id) item.usid = f.properties.id;
        if (useS3) item.geometryid = s3Key;
        else item.val = buf;

        var condition = { expected: {} };
        condition.expected.id = {
            ComparisonOperator: isUpdate ? 'NOT_NULL' : 'NULL'
        };

        var q = queue(1);
        if (useS3) q.defer(s3.putObject.bind(s3), s3Params);
        q.defer(dyno.putItem, item, condition);
        q.await(function(err) {
            if (err && err.code === 'ConditionalCheckFailedException') {
                var msg = isUpdate ? 'Feature does not exist' : 'Feature already exists';
                err = new Error(msg);
            }
            callback(err, primary);
        });
    };

    cardboard.del = function(primary, dataset, callback) {
        var key = { dataset: dataset, id: 'id!' + primary };

        dyno.deleteItems([ key ], function(err) {
            if (err) return callback(err, true);
            else callback();
        });
    };

    cardboard.get = function(primary, dataset, callback) {
        var key = { dataset: dataset, id: 'id!' + primary };

        dyno.getItem(key, function(err, res) {
            if (err) return callback(err);
            if (!res.Item) return callback(null, featureCollection());
            resolveFeature(res.Item, function(err, feature) {
                if (err) return callback(err);
                callback(null, featureCollection([feature]));
            });
        });
    };

    cardboard.getBySecondaryId = function(id, dataset, callback) {
        var query = { dataset: { EQ: dataset }, usid: { EQ: id } },
            opts = { index: 'usid', attributes: ['val', 'geometryid'], pages: 0 };

        dyno.query(query, opts, function(err, res) {
            if (err) return callback(err);
            resolveFeatures(res.items, function(err, features) {
                if (err) return callback(err);
                callback(null, featureCollection(features));
            });
        });
    };

    cardboard.createTable = function(tableName, callback) {
        var table = require('./lib/table.json');
        table.TableName = tableName;
        dyno.createTable(table, callback);
    };

    cardboard.delDataset = function(dataset, callback) {
        cardboard.listIds(dataset, function(err, res) {
            var keys = res.map(function(id){
                return { dataset: dataset, id: id };
            });

            dyno.deleteItems(keys, function(err, res) {
                callback(err);
            });
        });
    };

    cardboard.list = function(dataset, callback) {
        var query = { dataset: { EQ: dataset }, id: { BEGINS_WITH: 'id!' } },
            opts = { pages: 0 };

        dyno.query(query, opts, function(err, res) {
            if (err) return callback(err);
            resolveFeatures(res, function(err, features) {
                if (err) return callback(err);
                callback(null, featureCollection(features));
            });
        });
    };

    cardboard.listIds = function(dataset, callback) {
        var query = { dataset: { EQ: dataset } },
            opts = { attributes: ['id'], pages: 0 };

        dyno.query(query, opts, function(err, res) {
            if (err) return callback(err);
            callback(err, res.items.map(function(_) {
                return _.id;
            }));
        });
    };

    cardboard.listDatasets = function(callback) {
        var opts = { attributes: ['dataset'], pages:0 };

        dyno.scan(opts, function(err, res) {
            if (err) return callback(err);
            var datasets = _.uniq(res.items.map(function(item){
                return item.dataset;
            }));
            callback(err, datasets);
        });
    };

    cardboard.getDatasetInfo = function(dataset, callback) {
        Metadata(dyno, dataset).getInfo(callback);
    };

    cardboard.calculateDatasetInfo = function(dataset, callback) {
        Metadata(dyno, dataset).calculateInfo(callback);
    };

    cardboard.bboxQuery = function(input, dataset, callback) {
        var q = queue(100);

        function queryIndexLevel(level) {
            var indexes = geojsonCover.bboxQueryIndexes(input, true, coverOpts[level]);

            log('querying level:', level, ' with ', indexes.length, 'indexes');
            indexes.forEach(function(idx) {
                q.defer(
                    dyno.query, {
                        id: { 'BETWEEN': [ 'cell!'+level+'!' + idx[0], 'cell!'+level+'!' + idx[1] ] },
                        dataset: { 'EQ': dataset }
                    },
                    { pages: 0 }
                );
            });
        }

        [0,1].forEach(queryIndexLevel);

        q.awaitAll(function(err, res) {
            if (err) return callback(err);
            
            var res = parseQueryResponse(res);
            resolveFeatures(res, function(err, data) {
                if (err) return callback(err);
                callback(err, featureCollection(data));
            });
        });
    };

    cardboard.dump = function(cb) {
        return dyno.scan(cb);
    };

    cardboard.dumpGeoJSON = function(callback) {
        var opts = { attributes: ['id', 'cell'], pages: 0 };

        dyno.scan(opts, function(err, res) {
            if (err) return callback(err);

            var features = res.items.reduce(function(memo, item) {
                if (item.id.indexOf('id!') === 0) memo.push(toFeature(item));
                return memo;
            }, []);

            callback(null, featureCollection(features));
        });

        function toFeature(item) {
            return {
                type: 'Feature',
                properties: { key: item.key },
                geometry: new s2.S2Cell(new s2.S2CellId()
                    .fromToken(item.cell)).toGeoJSON()
            };
        }
    };

    cardboard.export = function(_) {
        return dyno.scan()
            .pipe(through({ objectMode: true }, function(data, enc, cb) {
                var output = this.push.bind(this);
                if (data.id.indexOf('id!') === 0) {
                    return resolveFeature(data, function(err, feature) {
                        output(feature);
                        cb();
                    });
                }
                cb();
            }))
            .pipe(geojsonStream.stringify());
    };

    function parseQueryResponse(res) {

        res = res.map(function(r) {
            return r.items;
        });

        var flat = _(res).chain().flatten().sortBy(function(a) {
            return a.primary;
        }).value();

        flat = uniq(flat, function(a, b) {
            return a.primary !== b.primary
        }, true);

        flat = _.values(flat);

        return flat;
    }

    function resolveFeature(item, callback) {
        var val = item.val,
            geometryid = item.geometryid;

        // Geobuf is stored in dynamo
        if (val) return callback(null, geobuf.geobufToFeature(val));
        
        // Geobuf is stored on S3
        if (geometryid) {
            return s3.getObject({
                Bucket: bucket,
                Key: geometryid
            }, function(err, data) {
                if (err) return callback(err);
                callback(null, geobuf.geobufToFeature(data.Body));
            });
        }
        
        callback(new Error('No defined geometry'));
    }

    function resolveFeatures(items, callback) {
        var q = queue(100);
        items.forEach(function(item) {
            q.defer(resolveFeature, item);
        });
        q.awaitAll(callback);
    }

    return cardboard;
};

function indexLevel(feature) {
    var bbox = extent(feature);
    var sw = point(bbox[0], bbox[1]);
    var ne = point(bbox[2], bbox[3]);
    var dist = distance(sw, ne, 'miles');
    return dist >= LARGE_INDEX_DISTANCE ? 0 : 1;
}

function featureCollection(features) {
    return {
        type: 'FeatureCollection',
        features: features || []
    };
}
