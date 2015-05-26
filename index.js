var through = require('through2');
var _ = require('lodash');
var geojsonStream = require('geojson-stream');
var geojsonNormalize = require('geojson-normalize');
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

    // allow for passed in config object to override s3 objects for mocking in tests
    var s3 = c.s3 || new AWS.S3(c);
    // pass in a pre configured dyno object, or create one based on config
    var dyno = c.dyno || Dyno(c);
    if (!c.bucket) throw new Error('No bucket set');
    var bucket = c.bucket;
    if (!c.prefix) throw new Error('No s3 prefix set');
    var prefix = c.prefix;

    cardboard.put = function(featureCollection, dataset, callback) {
        featureCollection = geojsonNormalize(featureCollection);
        var q = queue(150);

        featureCollection.features.forEach(function(f) {
            q.defer(putFeature, f, dataset);
        });

        q.awaitAll(callback);
    };

    function putFeature(feature, dataset, callback) {
        var f = feature.hasOwnProperty('id') ? _.clone(feature) : _.extend({ id: cuid() }, feature);
        var primary = f.id;

        if (!f.geometry || !f.geometry.coordinates) {
            var msg = 'Unlocated features can not be stored.';
            var err = new Error(msg);
            return callback(err, primary);
        }

        var metadata = Metadata(dyno, dataset),
            info = metadata.getFeatureInfo(f),
            timestamp = (+new Date()),
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
            west: truncateNum(info.west),
            south: truncateNum(info.south),
            east: truncateNum(info.east),
            north: truncateNum(info.north),
            s3url: ['s3:/', bucket, s3Key].join('/')
        };

        if (buf.length < MAX_GEOMETRY_SIZE) item.val = buf;

        var q = queue(1);
        q.defer(s3.putObject.bind(s3), s3Params);
        q.defer(dyno.putItem, item);
        q.await(function(err) {
            callback(err, primary);
        });
    }

    cardboard.del = function(primary, dataset, callback) {
        var key = { dataset: dataset, id: 'id!' + primary };

        dyno.deleteItem(key, { expected: { id: 'NOT_NULL'} }, function(err, res) {
            if (err && err.code === 'ConditionalCheckFailedException') return callback(new Error('Feature does not exist'));
            if (err) return callback(err, true);
            else callback();
        });
    };

    cardboard.get = function(primary, dataset, callback) {
        var key = { dataset: dataset, id: 'id!' + primary };

        dyno.getItem(key, function(err, item) {
            if (err) return callback(err);
            if (!item) return callback(null, featureCollection());
            resolveFeature(item, function(err, feature) {
                if (err) return callback(err);
                callback(null, featureCollection([feature]));
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
                return { dataset: dataset, id: 'id!'+id };
            });
            keys.push({ dataset: dataset, id: 'metadata!'+dataset });

            dyno.deleteItems(keys, function(err, res) {
                callback(err);
            });
        });
    };

    cardboard.list = function(dataset, pageOptions, callback) {
        var opts = {};

        if (typeof pageOptions === 'function') {
            callback = pageOptions;
            opts.pages = 0;
            pageOptions = {};
        }

        if (pageOptions.start) opts.start = pageOptions.start;
        if (pageOptions.maxFeatures) opts.limit = pageOptions.maxFeatures;

        var query = { dataset: { EQ: dataset }, id: { BEGINS_WITH: 'id!' } };
        dyno.query(query, opts, function(err, items) {
            if (err) return callback(err);
            resolveFeatures(items, function(err, features) {
                if (err) return callback(err);
                callback(null, featureCollection(features));
            });
        });
    };

    cardboard.listIds = function(dataset, callback) {
        var query = { dataset: { EQ: dataset }, id: {BEGINS_WITH: 'id!'} },
            opts = { attributes: ['id'], pages: 0 };

        dyno.query(query, opts, function(err, items) {
            if (err) return callback(err);
            callback(err, items.map(function(_) {
                return _.id.split('!')[1];
            }));
        });
    };

    cardboard.listDatasets = function(callback) {
        var opts = { attributes: ['dataset'], pages:0 };

        dyno.scan(opts, function(err, items) {
            if (err) return callback(err);
            var datasets = _.uniq(items.map(function(item){
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

    cardboard.bboxQuery = function(bbox, dataset, callback) {
        var q = queue(100);

        var bboxes = [bbox];
        var epsilon = 1E-8;

        // If a query crosses the (W) antimeridian/equator, we split it
        // into separate queries to reduce overall throughput.
        if (bbox[0] <= -180 && bbox[2] >= -180) {
            bboxes = bboxes.reduce(function(memo, bbox) {
                memo.push([bbox[0], bbox[1], -180 - epsilon, bbox[3]]);
                memo.push([-180 + epsilon, bbox[1], bbox[2], bbox[3]]);
                return memo;
            }, []);
            if (bbox[1] <= 0 && bbox[3] >= 0) {
                bboxes = bboxes.reduce(function(memo, bbox) {
                    memo.push([bbox[0], bbox[1], bbox[2], -epsilon]);
                    memo.push([bbox[0], epsilon, bbox[2], bbox[3]]);
                    return memo;
                }, []);
            }
        }

        // Likewise, if a query crosses the (E) antimeridian/equator,
        // we split it.
        else if (bbox[0] <= 180 && bbox[2] >= 180) {
            bboxes = bboxes.reduce(function(memo, bbox) {
                memo.push([bbox[0], bbox[1], 180 - epsilon, bbox[3]]);
                memo.push([180 + epsilon, bbox[1], bbox[2], bbox[3]]);
                return memo;
            }, []);
            if (bbox[1] <= 0 && bbox[3] >= 0) {
                bboxes = bboxes.reduce(function(memo, bbox) {
                    memo.push([bbox[0], bbox[1], bbox[2], -epsilon]);
                    memo.push([bbox[0], epsilon, bbox[2], bbox[3]]);
                    return memo;
                }, []);
            }
        }

        // If a query crosses the equator/prime meridian, we split it.
        else if (bbox[0] <= 0 && bbox[2] >= 0) {
            bboxes = bboxes.reduce(function(memo, bbox) {
                memo.push([bbox[0], bbox[1], -epsilon, bbox[3]]);
                memo.push([epsilon, bbox[1], bbox[2], bbox[3]]);
                return memo;
            }, []);
            if (bbox[1] <= 0 && bbox[3] >= 0) {
                bboxes = bboxes.reduce(function(memo, bbox) {
                    memo.push([bbox[0], bbox[1], bbox[2], -epsilon]);
                    memo.push([bbox[0], epsilon, bbox[2], bbox[3]]);
                    return memo;
                }, []);
            }
        }

        var tiles = bboxes.map(function(bbox) {
            return tilebelt.bboxToTile(bbox);
        });

        // Deduplicate subquery tiles.
        uniq(tiles, function(a, b) {
                return !tilebelt.tilesEqual(a, b);
            });

        if (tiles.length > 1) {
            // Filter out the z0 tile -- we'll always search it eventually.
            tiles = _.filter(tiles, function(item) {
                return item[2] !== 0;
                });
        }

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
                    west: { 'LE': bbox[2] },
                    east: { 'GE': bbox[0] },
                    north: { 'GE': bbox[1] },
                    south: { 'LE': bbox[3] }
                }
            };
            q.defer(dyno.query, query, options);

            // Travel up the parent tiles, finding features indexed in each
            var parentTileKey = tileKey.slice(0, -1);

            while (tileKey.length > 0) {
                query.cell = { 'EQ': 'cell!' + parentTileKey };
                q.defer(dyno.query, query, options);
                if (parentTileKey.length === 0) {
                    break;
                }
                parentTileKey = parentTileKey.slice(0, -1);
            }
        });

        q.awaitAll(function(err, items) {
            if (err) return callback(err);

            items = _.flatten(items);

            // Reduce the response's records to the set of
            // records with unique ids.
            uniq(items, function(a, b) {
                return a.id !== b.id;
            });

            resolveFeatures(items, function(err, data) {
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

function truncateNum(num, digits) {
    digits = digits || 6;
    var exp = Math.pow(10, digits);
    return Math.round(exp * num) / exp;
}
