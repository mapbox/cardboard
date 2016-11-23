var _ = require('lodash');
var Metadata = require('./lib/metadata');
var queue = require('queue-async');
var Dyno = require('dyno');
var AWS = require('aws-sdk');
var Pbf = require('pbf');
var geobuf = require('geobuf');
var stream = require('stream');

var MAX_GEOMETRY_SIZE = 1024 * 10;  // 10KB

module.exports = Cardboard;

/**
 * Cardboard client generator
 * @param {object} config - a configuration object
 * @param {string} config.searchTable - the name of a DynamoDB table to connect to
 * @param {string} config.featureTable - the name of a DynamoDB table to connect to
 * @param {string} config.region - the AWS region containing the DynamoDB table
 * @param {string} config.bucket - the name of an S3 bucket to use
 * @param {string} config.prefix - the name of a folder within the indicated S3 bucket
 * @param {dyno} [config.dyno] - a pre-configured [dyno client](https://github.com/mapbox/dyno) for connecting to DynamoDB
 * @param {s3} [config.s3] - a pre-configured [S3 client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)
 * @returns {cardboard} a cardboard client
 * @example
 * var cardboard = require('cardboard')({
 *   featureTable: 'my-cardboard-table-features',
 *   searchTable: 'my-cardboard-table-search',
 *   region: 'us-east-1',
 *   bucket: 'my-cardboard-bucket',
 *   prefix: 'my-cardboard-prefix'
 * });
 * @example
 * var cardboard = require('cardboard')({
 *   features: require('dyno')(dynoConfig),
 *   search: require('dyno')(dynoConfig),
 *   bucket: 'my-cardboard-bucket',
 *   prefix: 'my-cardboard-prefix'
 * });
 */
function Cardboard(config) {
    config = config || {};
    config.MAX_GEOMETRY_SIZE = config.MAX_GEOMETRY_SIZE || MAX_GEOMETRY_SIZE;

    // Allow caller to pass in aws-sdk clients
    if (!config.s3) config.s3 = new AWS.S3(config);
    if (!config.features) config.features = Dyno({table: config.featureTable, region: config.region, endpoint: config.endpoint});
    if (!config.search) config.search = Dyno({table: config.searchTable, region: config.region, endpoint: config.endpoint});

    if (!config.features && !config.featureTable) throw new Error('No feature table set');
    if (!config.search && !config.searchTable) throw new Error('No search table set');
    if (!config.region && (!config.search || !config.features)) throw new Error('No region set');
    if (!config.bucket) throw new Error('No bucket set');
    if (!config.prefix) throw new Error('No s3 prefix set');

    var utils = require('./lib/utils')(config);

    /**
     * A client configured to interact with a backend cardboard database
     */
    var cardboard = {};

    /**
     * Insert or update a single GeoJSON feature
     * @param {object} feature - a GeoJSON feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     * @example
     * // Create a point, allowing Cardboard to assign it an id.
     * var feature = {
     *   type: 'Feature',
     *   properties: {},
     *   geometry: {
     *     type: 'Point',
     *     coordinates: [0, 0]
     *   }
     * };
     *
     * cardboard.put(feature, 'my-dataset', function(err, result) {
     *   if (err) throw err;
     *   !!result.id; // true: an id has been assigned
     * });
     * @example
     * // Create a point, using a custom id.
     * var feature = {
     *   id: 'my-custom-id',
     *   type: 'Feature',
     *   properties: {},
     *   geometry: {
     *     type: 'Point',
     *     coordinates: [0, 0]
     *   }
     * };
     *
     * cardboard.put(feature, 'my-dataset', function(err, result) {
     *   if (err) throw err;
     *   result.id === feature.id; // true: the custom id was preserved
     * });
     * @example
     * // Create a point, then move it.
     * var feature = {
     *   type: 'Feature',
     *   properties: {},
     *   geometry: {
     *     type: 'Point',
     *     coordinates: [0, 0]
     *   }
     * };
     *
     * cardboard.put(feature, 'my-dataset', function(err, result) {
     *   if (err) throw err;
     *   result.geometry.coordinates = [1, 1];
     *
     *   cardboard.put(result, 'my-dataset', function(err, final) {
     *     if (err) throw err;
     *     final.geometry.coordinates[0] === 1; // true: the feature was moved
     *   });
     * });
     */
    cardboard.put = function(feature, dataset, callback) {
        var encoded;
        try { encoded = utils.toDatabaseRecord(feature, dataset); }
        catch (err) { return callback(err); }

        var q = queue(1);
        if (encoded.s3) q.defer(config.s3.putObject.bind(config.s3), encoded.s3);
        q.defer(function(done) {
            var params = {
                Item: encoded.feature,
                ReturnValues: 'ALL_OLD'
            };
            config.features.putItem(params, function(err) {
                if (err) return done(err);
                config.search.putItem({ Item: encoded.search }, done);
            });
        });
        q.await(function(err) {
            var result = geobuf.decode(new Pbf(encoded.feature.val || encoded.s3.Body));
            result.id = utils.idFromRecord(encoded.feature);
            callback(err, result);
        });
    };

    /**
     * Remove a single GeoJSON feature
     * @param {string} primary - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     * @example
     * // Create a point, then delete it
     * var feature = {
     *   id: 'my-custom-id',
     *   type: 'Feature',
     *   properties: {},
     *   geometry: {
     *     type: 'Point',
     *     coordinates: [0, 0]
     *   }
     * };
     *
     * cardboard.put(feature, 'my-dataset', function(err, result) {
     *   if (err) throw err;
     *
     *   cardboard.del(result.id, 'my-dataset', function(err, result) {
     *     if (err) throw err;
     *     !!result; // true: the feature was removed
     *   });
     * });
     * @example
     * // Attempt to delete a feature that does not exist
     * cardboard.del('non-existent-feature', 'my-dataset', function(err, result) {
     *   err.message === 'Feature does not exist'; // true
     *   !!result; // false: nothing was removed
     * });
     */
    cardboard.del = function(primary, dataset, callback) {
        var key = { index: dataset + '!' + primary };

        config.features.deleteItem({
            Key: key,
            ConditionExpression: 'attribute_exists(#index)',
            ExpressionAttributeNames: { '#index': 'index' }
        }, function(err) {
            if (err && err.code === 'ConditionalCheckFailedException') return callback(new Error('Feature does not exist'));
            if (err) return callback(err, true);
            else callback();
        });
    };

    /**
     * Retrieve a single GeoJSON feature
     * @param {string} primary - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     * @example
     * // Create a point, then retrieve it.
     * var feature = {
     *   type: 'Feature',
     *   properties: {},
     *   geometry: {
     *     type: 'Point',
     *     coordinates: [0, 0]
     *   }
     * };
     *
     * cardboard.put(feature, 'my-dataset', function(err, result) {
     *   if (err) throw err;
     *   result.geometry.coordinates = [1, 1];
     *
     *   cardboard.get(result.id, 'my-dataset', function(err, final) {
     *     if (err) throw err;
     *     final === result; // true: the feature was retrieved
     *   });
     * });
     * @example
     * // Attempt to retrieve a feature that does not exist
     * cardboard.get('non-existent-feature', 'my-dataset', function(err, result) {
     *   err.message === 'Feature non-existent-feature does not exist'; // true
     *   !!result; // false: nothing was retrieved
     * });
     */
    cardboard.get = function(primary, dataset, callback) {
        var key = { index: dataset+'!'+primary };

        config.features.getItem({Key: key}, function(err, data) {
            if (err) return callback(err);
            if (!data.Item) return callback(new Error('Feature ' + primary + ' does not exist'));
            utils.resolveFeatures([data.Item], function(err, features) {
                if (err) return callback(err);
                callback(null, features.features[0]);
            });
        });
    };

    /**
     * Create DynamoDB tables with Cardboard's schema
     * @param {function} callback - the callback function to handle the response
     * @example
     * // Create the cardboard table specified by the client config
     * cardboard.createTables(function(err) {
     *   if (err) throw err;
     * });
     */
    cardboard.createTables = function(callback) {
        var featuresTable = require('./lib/features_table.json');
        featuresTable.TableName = config.features.TableName;
        config.features.createTable(featuresTable, function(err) {
            if (err) return callback(err);
            var searchTable = require('./lib/search_table.json');
            searchTable.TableName = config.search.TableName;
            config.search.createTable(searchTable, callback);
        });
    };

    /**
     * List the ids available in a dataset
     * @private
     * @param {string} dataset - the name of the dataset
     * @param {function} callback - the callback function to handle the response
     */
    function listIds(dataset, callback) {
        var items = [];
        config.search.queryStream({
            ExpressionAttributeNames: { '#index': 'index', '#dataset': 'dataset' },
            ExpressionAttributeValues: { ':index': 'feature_id!', ':dataset': dataset },
            KeyConditionExpression: '#dataset = :dataset AND begins_with(#index, :index)',
            ProjectionExpression: '#index'
        }).on('data', function(d) {
            items.push(d);
        }).on('error', function(err) {
            callback(err);
        }).on('end', function() {
            callback(null, items.map(utils.idFromRecord));
        });
    }

    /**
     * Remove an entire dataset
     * @param {string} dataset - the name of the dataset
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.delDataset = function(dataset, callback) {
        listIds(dataset, function(err, res) {
            if (err) return callback(err);
            var params = { RequestItems: {} };
            params.RequestItems[config.featureTable] = res.map(function(id) {
                return { DeleteRequest: { Key: { index: dataset + '!' + id } } };
            });

            params.RequestItems[config.searchTable] = res.map(function(id) {
                return { DeleteRequest: { Key: { dataset: dataset, index: 'feature_id!'+id}}};     
            });

            params.RequestItems[config.searchTable].push({
                DeleteRequest: { Key: { dataset: dataset, index: 'metadata!' + dataset } }
            });

            config.features.batchWriteItemRequests(params).sendAll(10, callback);
        });
    };

    function createListParams(dataset, pageOptions) {
        var params = {};

        pageOptions = pageOptions || {};
        if (pageOptions.start) params.ExclusiveStartKey = {
            dataset: dataset,
            index: 'feature_id!' + pageOptions.start
        };

        if (pageOptions.maxFeatures) params.Limit = pageOptions.maxFeatures;

        params.ExpressionAttributeNames = { '#index': 'index', '#dataset': 'dataset' };
        params.ExpressionAttributeValues = { ':index': 'feature_id!', ':dataset': dataset };
        params.KeyConditionExpression = '#dataset = :dataset and begins_with(#index, :index)';
        return params;
    }

    /**
     * List the GeoJSON features that belong to a particular dataset
     * @param {string} dataset - the name of the dataset
     * @param {object} [pageOptions] - pagination options
     * @param {string} [pageOptions.start] - start reading features past the provided id
     * @param {number} [pageOptions.maxFeatures] - maximum number of features to return
     * @param {function} [callback] - the callback function to handle the response
     * @returns {object} a readable stream
     * @example
     * // List all the features in a dataset
     * cardboard.list('my-dataset', function(err, collection) {
     *   if (err) throw err;
     *   collection.type === 'FeatureCollection'; // true
     * });
     * @example
     * // List one page with a max of 10 features from a dataset
     * cardboard.list('my-dataset', { maxFeatures: 10 }, function(err, collection) {
     *   if (err) throw err;
     *   collection.type === 'FeatureCollection'; // true
     *   collection.features.length <= 10; // true
     * });
     * @example
     * // Paginate through all the features in a dataset
     * (function list(start) {
     *   cardabord.list('my-dataset', {
     *     maxFeatures: 10,
     *     start: start
     *   }, function(err, collection) {
     *     if (err) throw err;
     *     if (!collection.features.length) return console.log('All done!');
     *     list(collection.features.slice(-1)[0].id);
     *   });
     * })();
     */
    cardboard.list = function(dataset, pageOptions, callback) {

        if (typeof pageOptions === 'function') {
            callback = pageOptions;
            pageOptions = {};
        }

        var params = createListParams(dataset, pageOptions);

        config.search.query(params, function(err, data) {
            if (err) return callback(err);
            var ids = data.Items.map(function(item) { return item.index.replace(/^feature_id!/, ''); });
            utils.resolveFeaturesByIds(dataset, ids, function(err, features) {
                if (err) return callback(err);
                callback(null, features);
            });
        });
    };
    
    /**
     * Lists all the features in a dataset via a stream
     * @example
     * // Stream all the features in a dataset
     * cardboard.list('my-dataset')
     *   .on('data', function(feature) {
     *     console.log('Got feature: %j', feature);
     *   })
     *   .on('end', function() {
     *     console.log('All done!');
     *   });
     */
    cardboard.listStream = function(dataset, pageOptions) {
        var params = createListParams(dataset, pageOptions);
        var resolver = new stream.Transform({ objectMode: true, highWaterMark: 50 });

        resolver.items = [];

        resolver._resolve = function(callback) {
            utils.resolveFeaturesByIds(dataset, resolver.items, function(err, collection) {
                if (err) return callback(err);
                resolver.items = [];

                collection.features.forEach(function(feature) {
                    resolver.push(feature);
                });

                callback();
            });
        };

        resolver._transform = function(item, enc, callback) {
            resolver.items.push(utils.idFromRecord(item));
            if (resolver.items.length < 25) return callback();

            resolver._resolve(callback);
        };

        resolver._flush = function(callback) {
            if (!resolver.items.length) return callback();

            resolver._resolve(callback);
        };

        return config.search.queryStream(params)
            .on('error', function(err) {
                resolver.emit('error', err);
            })
          .pipe(resolver); 
    };

    /**
     * List datasets available in this database
     * @param {function} callback - the callback function to handle the response
     * @example
     * cardboard.listDatasets(function(err, datasets) {
     *   if (err) throw err;
     *   Array.isArray(datasets); // true
     *   console.log(datasets[0]); // 'my-dataset'
     * });
     */
    cardboard.listDatasets = function(callback) {
        var items = [];

        config.search.scanStream({ ProjectionExpression: 'dataset' })
            .on('data', function(d) { items.push(d); })
            .on('error', function(err) { callback(err); })
            .on('end', function() {
                var datasets = _.uniq(items.map(function(item) {
                    return item.dataset;
                }));

                callback(null, datasets);
            });
    };

    /**
     * Get cached metadata about a dataset

     * @param {string} dataset - the name of the dataset
     * @param {function} callback - the callback function to handle the response
     * @example
     * cardboard.getDatasetInfo('my-dataset', function(err, metadata) {
     *   if (err) throw err;
     *   console.log(Object.keys(metadatata));
     *   // [
     *   //   'dataset',
     *   //   'id',
     *   //   'west',
     *   //   'south',
     *   //   'east',
     *   //   'north',
     *   //   'count',
     *   //   'size',
     *   //   'updated'
     *   // ]
     * });
     */
    cardboard.getDatasetInfo = function(dataset, callback) {
        Metadata(config.dyno, dataset).getInfo(callback);
    };

    /**
     * Calculate metadata about a dataset
     * @param {string} dataset - the name of the dataset
     * @param {function} callback - the callback function to handle the response
     * @example
     * cardboard.calculateDatasetInfo('my-dataset', function(err, metadata) {
     *   if (err) throw err;
     *   console.log(Object.keys(metadatata));
     *   // [
     *   //   'dataset',
     *   //   'id',
     *   //   'west',
     *   //   'south',
     *   //   'east',
     *   //   'north',
     *   //   'count',
     *   //   'size',
     *   //   'updated'
     *   // ]
     * });
     */
    cardboard.calculateDatasetInfo = function(dataset, callback) {
        Metadata(config.dyno, dataset).calculateInfo(callback);
    };

    /**
     * A module for incremental metadata adjustments
     * @name cardboard.metadata
     */
    var metadata = {};

    /**
     * Pre-flight function to request information about the size and extent of a feature
     *
     * @param {string} dataset - the name of the dataset
     * @param {object} feature - a GeoJSON feature being added to the dataset
     * @returns {object} an object describing the feature's size and extent
     */
    metadata.featureInfo = function(dataset, feature) {
        return Metadata(config.dyno, dataset).getFeatureInfo(feature);
    };

    /**
     * Incrementally update a dataset's metadata with a new feature. This operation **will** create a metadata record if one does not exist.
     * @static
     * @memberof cardboard.metadata
     * @param {string} dataset - the name of the dataset
     * @param {object} feature - a GeoJSON feature (or backend record) being added to the dataset
     * @param {function} callback - a function to handle the response
     */
    metadata.addFeature = function(dataset, feature, callback) {
        Metadata(config.dyno, dataset).addFeature(feature, callback);
    };

    /**
     *
     * Update a dataset's metadata with a change to a single feature. This operation **will not** create a metadata record if one does not exist.
     * @static
     * @memberof cardboard.metadata
     * @param {string} dataset - the name of the dataset
     * @param {object} from - a GeoJSON feature (or backend record) representing the state of the feature *before* the update
     * @param {object} to - a GeoJSON feature (or backend record) representing the state of the feature *after* the update
     * @param {function} callback - a function to handle the response
     */
    metadata.updateFeature = function(dataset, from, to, callback) {
        Metadata(config.dyno, dataset).updateFeature(from, to, callback);
    };

    /**
     * Given a GeoJSON feature to remove, perform all required metadata updates. This operation **will not** create a metadata record if one does not exist. This operation **will not** shrink metadata bounds.
     * @static
     * @memberof cardboard.metadata
     * @param {string} dataset - the name of the dataset
     * @param {object} feature - a GeoJSON feature (or backend record) to remove from the dataset
     * @param {function} callback - a function to handle the response
     */
    metadata.deleteFeature = function(dataset, feature, callback) {
        Metadata(config.dyno, dataset).deleteFeature(feature, callback);
    };

    /**
     * Perform all required metadata updates for a set of changes to features in a dataset. This operation **will** create a metadata record if one does not exist.
     * @static
     * @memberof cardboard.metadata
     * @param {string} dataset - the name of the dataset
     * @param {array} changes - a set of changes. Each change must have an `.action` property, and `.new`, `.old`, or both.
     * @param {function} callback - a function fired when all changes have been implemented
     */
    metadata.applyChanges = function(dataset, changes, callback) {
        Metadata(config.dyno, dataset).applyChanges(changes, callback);
    };

    cardboard.metadata = metadata;

    return cardboard;
}
