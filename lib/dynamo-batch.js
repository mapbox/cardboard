var BatchWriteCommand = require('@aws-sdk/lib-dynamodb').BatchWriteCommand;
var BatchGetCommand = require('@aws-sdk/lib-dynamodb').BatchGetCommand;

var WRITE_BATCH_SIZE = 25;
var GET_BATCH_SIZE = 100;
// Bounded retries: any items still unprocessed after this are surfaced to the
// caller as `pending`, matching cardboard's existing partial-success contract.
var MAX_RETRIES = 4;
var CONCURRENCY = 10;

module.exports.batchWrite = batchWrite;
module.exports.batchGet = batchGet;

function chunk(array, size) {
    var chunks = [];
    for (var i = 0; i < array.length; i += size) chunks.push(array.slice(i, i + size));
    return chunks;
}

function backoffDelay(attempt) {
    return Math.min(50 * Math.pow(2, attempt), 5000);
}

// Runs an array of (callback-taking) task functions with a concurrency cap,
// collecting one result per task in the original order.
function runWithConcurrency(tasks, concurrency, callback) {
    var results = new Array(tasks.length);
    var index = 0;
    var active = 0;
    var settled = false;

    if (tasks.length === 0) return callback(null, results);

    function next() {
        if (settled) return;
        if (index >= tasks.length && active === 0) {
            settled = true;
            return callback(null, results);
        }

        while (active < concurrency && index < tasks.length) {
            (function(i) {
                active++;
                tasks[i](function(err, result) {
                    active--;
                    if (settled) return;
                    if (err) {
                        settled = true;
                        return callback(err);
                    }
                    results[i] = result;
                    next();
                });
            })(index++);
        }
    }

    next();
}

/**
 * Writes a set of PutRequest/DeleteRequest objects to a table, chunking into
 * batches of 25 and retrying unprocessed items with exponential backoff.
 * @param {DynamoDBDocumentClient} client
 * @param {string} tableName
 * @param {object[]} requests - PutRequest/DeleteRequest objects
 * @param {function} callback - (err, results), where each result has the shape
 * `{ UnprocessedItems: { RequestItems: { [tableName]: [...requests] } } }`
 */
function batchWrite(client, tableName, requests, callback) {
    var tasks = chunk(requests, WRITE_BATCH_SIZE).map(function(requestChunk) {
        return function(done) {
            sendBatchWriteWithRetry(client, tableName, requestChunk, 0, done);
        };
    });

    runWithConcurrency(tasks, CONCURRENCY, callback);
}

function sendBatchWriteWithRetry(client, tableName, requests, attempt, callback) {
    var params = { RequestItems: {} };
    params.RequestItems[tableName] = requests;

    client.send(new BatchWriteCommand(params)).then(function(data) {
        var unprocessed = (data.UnprocessedItems && data.UnprocessedItems[tableName]) || [];

        if (unprocessed.length === 0 || attempt >= MAX_RETRIES) {
            var result = { UnprocessedItems: { RequestItems: {} } };
            result.UnprocessedItems.RequestItems[tableName] = unprocessed;
            return callback(null, result);
        }

        setTimeout(function() {
            sendBatchWriteWithRetry(client, tableName, unprocessed, attempt + 1, callback);
        }, backoffDelay(attempt));
    }, callback);
}

/**
 * Reads a set of keys from a table, chunking into batches of 100 and retrying
 * unprocessed keys with exponential backoff.
 * @param {DynamoDBDocumentClient} client
 * @param {string} tableName
 * @param {object[]} keys
 * @param {function} callback - (err, results), where each result has the shape
 * `{ Responses: { [tableName]: [...items] }, UnprocessedKeys: { [tableName]: { Keys: [...keys] } } }`
 */
function batchGet(client, tableName, keys, callback) {
    var tasks = chunk(keys, GET_BATCH_SIZE).map(function(keyChunk) {
        return function(done) {
            sendBatchGetWithRetry(client, tableName, keyChunk, [], 0, done);
        };
    });

    runWithConcurrency(tasks, CONCURRENCY, callback);
}

function sendBatchGetWithRetry(client, tableName, keys, collected, attempt, callback) {
    var params = { RequestItems: {} };
    params.RequestItems[tableName] = { Keys: keys };

    client.send(new BatchGetCommand(params)).then(function(data) {
        var responses = (data.Responses && data.Responses[tableName]) || [];
        var allItems = collected.concat(responses);
        var unprocessedKeys = (data.UnprocessedKeys && data.UnprocessedKeys[tableName] && data.UnprocessedKeys[tableName].Keys) || [];

        if (unprocessedKeys.length === 0 || attempt >= MAX_RETRIES) {
            var result = { Responses: {}, UnprocessedKeys: {} };
            result.Responses[tableName] = allItems;
            result.UnprocessedKeys[tableName] = { Keys: unprocessedKeys };
            return callback(null, result);
        }

        setTimeout(function() {
            sendBatchGetWithRetry(client, tableName, unprocessedKeys, allItems, attempt + 1, callback);
        }, backoffDelay(attempt));
    }, callback);
}
