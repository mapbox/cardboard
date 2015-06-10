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

module.exports = function Cardboard(config) {
    config = config || {};
    config.MAX_GEOMETRY_SIZE = MAX_GEOMETRY_SIZE;

    // Allow caller to pass in aws-sdk clients
    if (!config.s3) config.s3 = new AWS.S3(config);
    if (!config.dyno) config.dyno = Dyno(config);

    if (!config.table && !config.dyno) throw new Error('No table set');
    if (!config.region && !config.dyno) throw new Error('No region set');
    if (!config.bucket) throw new Error('No bucket set');
    if (!config.prefix) throw new Error('No s3 prefix set');

    var utils = require('./lib/utils')(config);
    var cardboard = {
        batch: require('./lib/batch')(config)
    };

    cardboard.put = function(feature, dataset, callback) {
        var encoded;
        try { encoded = utils.toDatabaseRecord(feature, dataset); }
        catch (err) { return callback(err); }

        var q = queue(1);
        q.defer(config.s3.putObject.bind(config.s3), encoded[1]);
        q.defer(config.dyno.putItem, encoded[0]);
        q.await(function(err) {
            var result = JSON.parse(JSON.stringify(feature));
            result.id = encoded[0].id.split('!')[1];
            callback(err, result);
        });
    };

    cardboard.del = function(primary, dataset, callback) {
        var key = { dataset: dataset, id: 'id!' + primary };

        config.dyno.deleteItem(key, { expected: { id: 'NOT_NULL'} }, function(err, res) {
            if (err && err.code === 'ConditionalCheckFailedException') return callback(new Error('Feature does not exist'));
            if (err) return callback(err, true);
            else callback();
        });
    };

    cardboard.get = function(primary, dataset, callback) {
        var key = { dataset: dataset, id: 'id!' + primary };

        config.dyno.getItem(key, function(err, item) {
            if (err) return callback(err);
            if (!item) return callback(new Error('Feature ' + primary + ' does not exist'));
            utils.resolveFeatures([item], function(err, features) {
                if (err) return callback(err);
                callback(null, features.features[0]);
            });
        });
    };

    cardboard.createTable = function(tableName, callback) {
        if (typeof tableName === 'function') {
            callback = tableName;
            tableName = null;
        }
        
        var table = require('./lib/table.json');
        table.TableName = tableName || config.table;
        config.dyno.createTable(table, callback);
    };

    cardboard.delDataset = function(dataset, callback) {
        cardboard.listIds(dataset, function(err, res) {
            var keys = res.map(function(id){
                return { dataset: dataset, id: 'id!'+id };
            });
            keys.push({ dataset: dataset, id: 'metadata!'+dataset });

            config.dyno.deleteItems(keys, function(err, res) {
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
        config.dyno.query(query, opts, function(err, items) {
            if (err) return callback(err);
            utils.resolveFeatures(items, function(err, features) {
                if (err) return callback(err);
                callback(null, features);
            });
        });
    };

    cardboard.listIds = function(dataset, callback) {
        var query = { dataset: { EQ: dataset }, id: {BEGINS_WITH: 'id!'} },
            opts = { attributes: ['id'], pages: 0 };

        config.dyno.query(query, opts, function(err, items) {
            if (err) return callback(err);
            callback(err, items.map(function(_) {
                return _.id.split('!')[1];
            }));
        });
    };

    cardboard.listDatasets = function(callback) {
        var opts = { attributes: ['dataset'], pages:0 };

        config.dyno.scan(opts, function(err, items) {
            if (err) return callback(err);
            var datasets = _.uniq(items.map(function(item){
                return item.dataset;
            }));
            callback(err, datasets);
        });
    };

    cardboard.getDatasetInfo = function(dataset, callback) {
        Metadata(config.dyno, dataset).getInfo(callback);
    };

    cardboard.calculateDatasetInfo = function(dataset, callback) {
        Metadata(config.dyno, dataset).calculateInfo(callback);
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
            q.defer(config.dyno.query, query, options);

            // Travel up the parent tiles, finding features indexed in each
            var parentTileKey = tileKey.slice(0, -1);

            while (tileKey.length > 0) {
                query.cell = { 'EQ': 'cell!' + parentTileKey };
                q.defer(config.dyno.query, query, options);
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

            utils.resolveFeatures(items, function(err, data) {
                if (err) return callback(err);
                callback(err, data);
            });
        });
    };

    cardboard.dump = function(cb) {
        return config.dyno.scan(cb);
    };

    cardboard.export = function(_) {
        return config.dyno.scan()
            .pipe(through({ objectMode: true }, function(data, enc, cb) {
                var output = this.push.bind(this);
                if (data.id.indexOf('id!') === 0) {
                    return utils.resolveFeatures([data], function(err, features) {
                        output(features.features[0]);
                        cb();
                    });
                }
                cb();
            }))
            .pipe(geojsonStream.stringify());
    };

    return cardboard;
};

function indexLevel(feature) {
    var bbox = extent(feature);
    var sw = point(bbox[0], bbox[1]);
    var ne = point(bbox[2], bbox[3]);
    var dist = distance(sw, ne, 'miles');
    return dist >= LARGE_INDEX_DISTANCE ? 0 : 1;
}

function truncateNum(num, digits) {
    digits = digits || 6;
    var exp = Math.pow(10, digits);
    return Math.round(exp * num) / exp;
}
