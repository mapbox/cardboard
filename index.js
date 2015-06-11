var _ = require('lodash');
var Metadata = require('./lib/metadata');
var uniq = require('uniq');
var queue = require('queue-async');
var Dyno = require('dyno');
var AWS = require('aws-sdk');
var extent = require('geojson-extent');
var cuid = require('cuid');
var tilebelt = require('tilebelt');
var geobuf = require('geobuf');
var stream = require('stream');

var MAX_GEOMETRY_SIZE = 1024 * 10;  // 10KB

// /**
//  * Handle the response from inserting or updating a feature
//  * @callback cardboard~putCallback
//  * @param {object} err - an error object, set to null if no error occurred
//  * @param {object} feature - a stored version of the GeoJSON feature that was inserted or updated
//  */

/**
 * @name PaginationOptions
 * @type {object}
 * @property {string} start - start reading features past the provided id
 * @property {number} maxFeatures - maximum number of features to return
 */

/**
 * @name CardboardClientConfiguration
 * @type {object}
 * @property {string} table - the name of a DynamoDB table to connect to
 * @property {string} region - the AWS region containing the DynamoDB table
 * @property {string} bucket - the name of an S3 bucket to use
 * @property {string} prefix - the name of a folder within the indicated S3 bucket
 */

/**
 * Creates a cardboard client
 * @name CardboardClientFactory
 * @param {CardboardClientConfiguration} config - a configuration object
 * @returns {cardboard} a cardboard client
 */
module.exports = function(config) {
    config = config || {};
    config.MAX_GEOMETRY_SIZE = config.MAX_GEOMETRY_SIZE || MAX_GEOMETRY_SIZE;

    // Allow caller to pass in aws-sdk clients
    if (!config.s3) config.s3 = new AWS.S3(config);
    if (!config.dyno) config.dyno = Dyno(config);

    if (!config.table && !config.dyno) throw new Error('No table set');
    if (!config.region && !config.dyno) throw new Error('No region set');
    if (!config.bucket) throw new Error('No bucket set');
    if (!config.prefix) throw new Error('No s3 prefix set');

    var utils = require('./lib/utils')(config);

    /**
     * A client configured to interact with a backend cardboard database
     */
    var cardboard = {
        /**
         * A module for batch requests
         */
        batch: require('./lib/batch')(config)
    };

    /**
     * Insert or update a single GeoJSON feature
     * @param {object} feature - a GeoJSON feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.put = function(feature, dataset, callback) {
        var encoded;
        try { encoded = utils.toDatabaseRecord(feature, dataset); }
        catch (err) { return callback(err); }

        var q = queue(1);
        q.defer(config.s3.putObject.bind(config.s3), encoded[1]);
        q.defer(config.dyno.putItem, encoded[0]);
        q.await(function(err) {
            var result = geobuf.geobufToFeature(encoded[1].Body);
            result.id = encoded[0].id.split('!')[1];
            callback(err, result);
        });
    };

    /**
     * Remove a single GeoJSON feature
     * @param {string} primary - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.del = function(primary, dataset, callback) {
        var key = { dataset: dataset, id: 'id!' + primary };

        config.dyno.deleteItem(key, { expected: { id: 'NOT_NULL'} }, function(err, res) {
            if (err && err.code === 'ConditionalCheckFailedException') return callback(new Error('Feature does not exist'));
            if (err) return callback(err, true);
            else callback();
        });
    };

    /**
     * Retreive a single GeoJSON feature
     * @param {string} primary - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
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

    /**
     * Create a DynamoDB table with Cardboard's schema
     * @param {string} [tableName] - the name of the table to create, if not provided, defaults to the tablename defined in client configuration.
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.createTable = function(tableName, callback) {
        if (typeof tableName === 'function') {
            callback = tableName;
            tableName = null;
        }

        var table = require('./lib/table.json');
        table.TableName = tableName || config.table;
        config.dyno.createTable(table, callback);
    };

    function listIds(dataset, callback) {
        var query = { dataset: { EQ: dataset }, id: {BEGINS_WITH: 'id!'} };
        var opts = { attributes: ['id'], pages: 0 };

        config.dyno.query(query, opts, function(err, items) {
            if (err) return callback(err);
            callback(err, items.map(function(_) {
                return _.id.split('!')[1];
            }));
        });
    }

    cardboard.delDataset = function(dataset, callback) {
        listIds(dataset, function(err, res) {
            var keys = res.map(function(id) {
                return { dataset: dataset, id: 'id!' + id };
            });

            keys.push({ dataset: dataset, id: 'metadata!' + dataset });

            config.dyno.deleteItems(keys, function(err, res) {
                callback(err);
            });
        });
    };

    /**
     * List the GeoJSON features that belong to a particular dataset
     * @param {string} dataset - the name of the dataset
     * @param {PaginationOptions} pageOptions - pagination options
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.list = function(dataset, pageOptions, callback) {
        var opts = {};

        if (typeof pageOptions === 'function') {
            callback = pageOptions;
            opts.pages = 0;
            pageOptions = {};
        }

        pageOptions = pageOptions || {};
        if (pageOptions.start) opts.start = pageOptions.start;
        if (pageOptions.maxFeatures) opts.limit = pageOptions.maxFeatures;

        var query = { dataset: { EQ: dataset }, id: { BEGINS_WITH: 'id!' } };

        if (!callback) {
            var resolver = new stream.Transform({ objectMode: true, highWaterMark: 50 });

            resolver.items = [];

            resolver._resolve = function(callback) {
                utils.resolveFeatures(resolver.items, function(err, collection) {
                    if (err) return callback(err);

                    resolver.items = [];

                    collection.features.forEach(function(feature) {
                        resolver.push(feature);
                    });

                    callback();
                });
            };

            resolver._transform = function(item, enc, callback) {
                resolver.items.push(item);
                if (resolver.items.length < 25) return callback();

                resolver._resolve(callback);
            };

            resolver._flush = function(callback) {
                if (!resolver.items.length) return callback();

                resolver._resolve(callback);
            };

            return config.dyno.query(query)
                .on('error', function(err) { resolver.emit('error', err); })
              .pipe(resolver);
        }

        config.dyno.query(query, opts, function(err, items) {
            if (err) return callback(err);
            utils.resolveFeatures(items, function(err, features) {
                if (err) return callback(err);
                callback(null, features);
            });
        });
    };

    /**
     * List datasets available in this database
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.listDatasets = function(callback) {
        var opts = { attributes: ['dataset'], pages:0 };

        config.dyno.scan(opts, function(err, items) {
            if (err) return callback(err);

            var datasets = _.uniq(items.map(function(item) {
                return item.dataset;
            }));

            callback(err, datasets);
        });
    };

    /**
     * Get cached metadata about a dataset
     * @param {string} dataset - the name of the dataset
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.getDatasetInfo = function(dataset, callback) {
        Metadata(config.dyno, dataset).getInfo(callback);
    };

    /**
     * Calculate metadata about a dataset
     * @param {string} dataset - the name of the dataset
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.calculateDatasetInfo = function(dataset, callback) {
        Metadata(config.dyno, dataset).calculateInfo(callback);
    };

    /**
     * Find GeoJSON features that intersect a bounding box
     * @param {number[]} bbox - the bounding box as `[west, south, east, north]`
     * @param {string} dataset - the name of the dataset
     * @param {function} callback - the callback function to handle the response
     */
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
                cell: { BEGINS_WITH: 'cell!' + tileKey },
                dataset: { EQ: dataset }
            };

            var options = {
                pages: 0,
                index: 'cell',
                filter: {
                    west: { LE: bbox[2] },
                    east: { GE: bbox[0] },
                    north: { GE: bbox[1] },
                    south: { LE: bbox[3] }
                }
            };
            q.defer(config.dyno.query, query, options);

            // Travel up the parent tiles, finding features indexed in each
            var parentTileKey = tileKey.slice(0, -1);

            while (tileKey.length > 0) {
                query.cell = { EQ: 'cell!' + parentTileKey };
                q.defer(config.dyno.query, query, options);
                if (parentTileKey.length === 0) break;
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

    return cardboard;
};
