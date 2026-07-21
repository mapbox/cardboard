var DynamoDBClient = require('@aws-sdk/client-dynamodb').DynamoDBClient;
var CreateTableCommand = require('@aws-sdk/client-dynamodb').CreateTableCommand;
var ResourceInUseException = require('@aws-sdk/client-dynamodb').ResourceInUseException;
var DynamoDBDocumentClient = require('@aws-sdk/lib-dynamodb').DynamoDBDocumentClient;
var dynamoBatch = require('./lib/dynamo-batch');

module.exports = Cardboard;

/**
 * Cardboard client generator
 * @param {object} config - a configuration object
 * @param {string} config.mainTable - the name of a DynamoDB table to connect to
 * @param {string} config.region - the AWS region containing the DynamoDB table
 * @param {string} [config.endpoint] - an alternate DynamoDB endpoint to connect to
 * @param {string} [config.accessKeyId] - AWS credentials
 * @param {string} [config.secretAccessKey] - AWS credentials
 * @param {string} [config.sessionToken] - AWS credentials
 * @param {DynamoDBDocumentClient} [config.dynamodb] - a pre-configured [DynamoDBDocumentClient](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/lib-dynamodb/) for connecting to DynamoDB
 * @returns {cardboard} a cardboard client
 */
function Cardboard(config) {
    config = config || {};

    // Allow caller to pass in an aws-sdk client
    if (typeof config.mainTable !== 'string' || config.mainTable.length === 0) throw new Error('"mainTable" must be a string');
    if (!config.dynamodb && !config.region) throw new Error('No region set');
    if (!config.dynamodb) {
        var clientConfig = { region: config.region, endpoint: config.endpoint };
        if (config.accessKeyId && config.secretAccessKey) {
            clientConfig.credentials = {
                accessKeyId: config.accessKeyId,
                secretAccessKey: config.secretAccessKey,
                sessionToken: config.sessionToken
            };
        } else if (config.endpoint) {
            // Local/custom endpoints (dynalite, DynamoDB Local, etc.) don't
            // validate credentials, but the SDK still requires some to be
            // resolvable rather than falling through to the (possibly empty)
            // default provider chain.
            clientConfig.credentials = { accessKeyId: 'local', secretAccessKey: 'local' };
        }
        config.dynamodb = DynamoDBDocumentClient.from(new DynamoDBClient(clientConfig));
    }

    var utils = require('./lib/utils');

    /**
     * A client configured to interact with a backend cardboard database
     */
    var cardboard = {};

    /**
     * Insert or update a single GeoJSON feature
     * @param {(Feature|FeatureCollection)} input - a GeoJSON feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.put = function(input, dataset, callback) {
        if (input.type === 'Feature') input = utils.featureCollection([input]);
        if (input.type !== 'FeatureCollection') return callback(new Error('Must be a Feature or FeatureCollection'));

        var records = [];
        var geobufs = {};

        var encoded;

        for (var i = 0; i < input.features.length; i++) {
            try { encoded = utils.toDatabaseRecord(input.features[i], dataset); }
            catch (err) { return callback(err); }

            records.push(encoded);
            geobufs[encoded.key] = encoded.val;
        }

        var params = { RequestItems: {} };
        params.RequestItems[config.mainTable] = records.map(function(record) {
            return { PutRequest: { Item: record } };
        });

        dynamoBatch.batchWrite(config.dynamodb, config.mainTable, params.RequestItems[config.mainTable], function(err, results) {
            if (err) return callback(err);

            var unprocessed = results.reduce(function(memo, result) {
                var requests = result.UnprocessedItems.RequestItems ? result.UnprocessedItems.RequestItems[config.mainTable] : [];
                return memo.concat(requests);
            }, []);

            var pending = unprocessed.map(function(req) {
                var item = req.PutRequest.Item;
                var buffer = geobufs[item.key];
                delete geobufs[item.key];
                return utils.decodeBuffer(buffer);
            });

            var features = Object.keys(geobufs).map(function(key) {
                return utils.decodeBuffer(geobufs[key]);
            });

            var fc = utils.featureCollection(features);
            if (pending.length > 0) fc.pending = pending;
            callback(null, fc);
        });
    };

    /**
     * Remove a single GeoJSON feature
     * @param {string | [string]} input - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.del = function(input, dataset, callback) {
        if (!Array.isArray(input)) input = [input];
        var params = { RequestItems: {} };

        var err = null;

        params.RequestItems[config.mainTable] = input.map(function(id) {
            if (typeof id !== 'string') return err = new Error('All ids must be strings');
            return { DeleteRequest: { Key: utils.createFeatureKey(dataset, id) } };
        });

        if(err) return callback(err);

        dynamoBatch.batchWrite(config.dynamodb, config.mainTable, params.RequestItems[config.mainTable], function(err, results) {
            if (err) return callback(err);

            var unprocessed = results.reduce(function(memo, result) {
                var requests = result.UnprocessedItems.RequestItems ? result.UnprocessedItems.RequestItems[config.mainTable] : [];
                return memo.concat(requests);
            }, []);

            var ids = unprocessed.map(function(item) {
                return utils.idFromRecord(item.DeleteRequest.Key);
            });

            callback(null, ids);
        });
    };

    /**
     * Retrieve a single GeoJSON feature
     * @param {string|[string]} input - the id for a feature
     * @param {string} dataset - the name of the dataset that this feature belongs to
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.get = function(input, dataset, callback) {
        if (!Array.isArray(input)) input = [input]; 

        var keys = input.map(function(id) { return utils.createFeatureKey(dataset, id); });

        dynamoBatch.batchGet(config.dynamodb, config.mainTable, keys, function(err, results) {
            if (err) return callback(err);
            var features = results.reduce(function(memo, result) {
                var res = result.Responses ? result.Responses[config.mainTable] : [];
                return memo.concat(res);
            }, []);
            var pending = results.reduce(function(memo, result) {
                var res = result.UnprocessedKeys && result.UnprocessedKeys[config.mainTable] ? result.UnprocessedKeys[config.mainTable].Keys : []; 
                return memo.concat(res);
            }, []);

            var fc = null
            try {
                fc = utils.resolveFeatures(features);
                if (pending.length > 0) {
                    fc.pending = pending.map(function(key) { return utils.idFromRecord(key); });
                }
            }
            catch(err) {
                return callback(err);
            }
            return callback(null, fc);
        });
    };

    /**
     * Create DynamoDB tables with Cardboard's schema
     * @param {function} callback - the callback function to handle the response
     */
    cardboard.createTable = function(callback) {
        var tableSchema = require('./lib/main-table.json');
        tableSchema.TableName = config.mainTable;
        config.dynamodb.send(new CreateTableCommand(tableSchema)).then(function(data) {
            callback(null, data);
        }, function(err) {
            // A table with this name already existing is not an error: callers
            // are free to point cardboard at a pre-existing table.
            if (err instanceof ResourceInUseException) return callback(null);
            callback(err);
        });
    };

    return cardboard;
}

Cardboard.streamHelper = require('./lib/stream-helper');
