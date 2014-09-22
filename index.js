var s2 = require('s2');
var through = require('through2');
var _ = require('lodash');
var geojsonStream = require('geojson-stream');
var geojsonNormalize = require('geojson-normalize')
var concat = require('concat-stream');
var coverOpts = require('./lib/coveropts');
var Metadata = require('./lib/metadata');
var uniq = require('uniq');
var geobuf = require('geobuf');
var log = require('debug')('cardboard');
var queue = require('queue-async');
var Dyno = require('dyno');
var AWS = require('aws-sdk');
var extent = require('geojson-extent');
var cuid = require('cuid');
var url = require('url');
var tilebelt = require('tilebelt');

var MAX_GEOMETRY_SIZE = 1024*10;  //10KB
var LARGE_INDEX_DISTANCE = 50; //bbox more then 100 miles corner to corner.


module.exports = function Cardboard(c) {
    var cardboard = {};

    AWS.config.update(c);

    // allow for passed in config object to override s3 objects for mocking in tests
    var s3 = c.s3 || new AWS.S3();
    var dyno = Dyno(c);
    if (!c.bucket) throw new Error('No bucket set');
    var bucket = c.bucket;
    if (!c.prefix) throw new Error('No s3 prefix set');
    var prefix = c.prefix;

    cardboard.put = function(feature, dataset, callback) {
        var isUpdate = feature.hasOwnProperty('id'),
            f = isUpdate ? _.clone(feature) : _.extend({ id: cuid() }, feature),
            metadata = Metadata(dyno, dataset),
            info = metadata.getFeatureInfo(f),
            timestamp = (+new Date()),
            primary = f.id,
            buf = geobuf.featureToGeobuf(f).toBuffer(),
            tile = tilebelt.bboxToTile([info.west, info.south, info.east, info.north]),
            cell = tilebelt.tileToQuadkey(tile),
            useS3 = buf.length > MAX_GEOMETRY_SIZE,
            s3Key = [prefix, dataset, primary, timestamp].join('/'),
            s3Params = { Bucket: bucket, Key: s3Key, Body: buf };

        var item = {
            dataset: dataset,
            id: 'id!' + primary,
            cell: 'cell!' + cell,
            size: info.size,
            west: info.west,
            south: info.south,
            east: info.east,
            north: info.north,
            s3url: ['s3:/', bucket, s3Key].join('/')
        };

        if (f.properties.id) item.usid = f.properties.id;
        if (buf.length < MAX_GEOMETRY_SIZE) item.val = buf;

        var condition = { expected: {} };
        condition.expected.id  = isUpdate ? {'NOT_NULL': []} : {'NULL': []};

        var q = queue(1);
        q.defer(s3.putObject.bind(s3), s3Params);
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
            opts = { index: 'usid', attributes: ['val', 's3url'], pages: 0 };

        dyno.query(query, opts, function(err, res) {
            if (err) return callback(err);
            resolveFeatures(res.items, function(err, features) {
                if (err) return callback(err);
                callback(null, featureCollection(features));
            });
        });
    };

    cardboard.createTable = function(callback) {
        var table = require('./lib/table.json');
        table.TableName = c.table;
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

        // Force queries that touch the equator/prime meridian into one of
        // four quadrants
        var bbox = _.clone(input);
        if (bbox[0] === 0) bbox[0] = 0.00000001;
        if (bbox[1] === 0) bbox[1] = 0.00000001;
        if (bbox[2] === 0) bbox[2] = -0.00000001;
        if (bbox[3] === 0) bbox[3] = -0.00000001;

        // If a query crosses the equator/prime meridian, we need to split it
        // into separate queries. Otherwise we will end up querying the z0 tile
        var bboxes = [bbox];
        var splitX = bbox[0] < 0 && bbox[2] > 0;
        var splitY = bbox[1] < 0 && bbox[3] > 0;

        if (splitX) bboxes = bboxes.reduce(function(memo, bbox) {
            memo.push([bbox[0], bbox[1], -0.0001, bbox[3]]);
            memo.push([0.0001, bbox[1], bbox[2], bbox[3]]);
            return memo;
        }, []);

        if (splitY) bboxes = bboxes.reduce(function(memo, bbox) {
            memo.push([bbox[0], bbox[1], bbox[2], -0.0001]);
            memo.push([bbox[0], 0.0001, bbox[2], bbox[3]]);
            return memo;
        }, []);

        var tiles = bboxes.map(function(bbox) {
            return tilebelt.bboxToTile(bbox);
        });

        tiles.forEach(function(tile) {
            var tileKey = tilebelt.tileToQuadkey(tile);

            // First find features indexed in children of this tile
            var query = {
                cell: { 'BEGINS_WITH': 'cell!' + tileKey },
                dataset: { 'EQ': dataset }
            };

            var options = {
                pages: 0,
                index: 'cell',
                filter : {
                    west: { 'LE': input[2] },
                    east: { 'GE': input[0] },
                    north: { 'GE': input[1] },
                    south: { 'LE': input[3] }
                }
            };
            q.defer(dyno.query, query, options);

            // Travel up the parent tiles, finding features indexed in each
            var parentTile = tilebelt.getParent(tile);


            while (parentTile[2] > -1) {
                query.cell = { 'EQ': 'cell!' + tilebelt.tileToQuadkey(parentTile) };
                q.defer(dyno.query, query, options);
                parentTile = tilebelt.getParent(parentTile);
            }
        });

        q.awaitAll(function(err, res) {
            if (err) return callback(err);

            var resp = _.flatten(res.map(function(r) {
                return r.items;
            }));

            // Reduce the response's records to the set of
            // records with unique ids.
            uniq(resp, function(a, b) {
                return a.id !== b.id
            });

            resolveFeatures(resp, function(err, data) {
                if (err) return callback(err);
                callback(err, featureCollection(data));
            });
        });
    };

    cardboard.dump = function(cb) {
        return dyno.scan(cb);
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

    function resolveFeature(item, callback) {
        var val = item.val;

        // Geobuf is stored in dynamo
        if (val) return callback(null, geobuf.geobufToFeature(val));

        // Get geobuf from S3
        var uri = url.parse(item.s3url);
        s3.getObject({
            Bucket: uri.host,
            Key: uri.pathname.substr(1)
        }, function(err, data) {
            if (err) return callback(err);
            callback(null, geobuf.geobufToFeature(data.Body));
        });
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
