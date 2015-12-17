#!/usr/bin/env node

var Cardboard = require('../');
var parser = require('geojson-stream').parse();
var collector = require('geojson-stream').stringify();
var stream = require('stream');
var args = require('minimist')(process.argv.slice(2));

function configLoader(args, env) {
    var config = {};

    config.region = args.region || env.CardboardRegion;
    if (!config.region) throw new Error('You must provide a region');

    config.table = args.table || env.CardboardTable;
    if (!config.table) throw new Error('You must provide a table name');

    config.bucket = args.bucket || env.CardboardBucket;
    if (!config.bucket) throw new Error('You must provide an S3 bucket');

    config.prefix = args.prefix || env.CardboardPrefix;
    if (!config.prefix) throw new Error('You must provide an S3 prefix');

    if (args.endpoint || env.CardboardEndpoint) {
        config.endpoint = args.endpoint || env.CardboardEndpoint;
    }

    return config;
}

var config;
try { config = configLoader(args, process.env); }
catch (err) {
    console.error(err.message);
    process.exit(1);
}

var command = args._[0];
if (['put', 'get', 'list', 'bbox'].indexOf(command) < 0) {
    console.error(command + ' is not a valid command');
    process.exit(1);
}

var dataset = args._[1];
if (!dataset) {
    console.error('You must provide the name of the dataset to interact with');
    process.exit(1);
}

var id = args._[2];
if (command === 'get' && !id) {
    console.error('You must provide the id of the feature to get');
    process.exit(1);
}

if (command === 'bbox') {
    var bbox = process.argv.slice(2).filter(function(arg) {
        return arg.split(',').length === 4;
    })[0];

    if (!bbox) {
        console.error('You must provide a bounding box for your query');
        process.exit(1);
    }

    bbox = bbox.split(',').map(function(coord) {
        return Number(coord);
    });
}

var cardboard = Cardboard(config);

cardboard.createTable(function(err){
    if (err) throw err;

    if (command === 'get') {
        return cardboard.get(id, dataset, function(err, item) {
            if (err) throw err;
            console.log(JSON.stringify(item));
        });
    }

    if (command === 'list') {
        return cardboard.list(dataset)
            .on('error', function(err) { throw err; })
          .pipe(collector)
            .on('error', function(err) { throw err; })
          .pipe(process.stdout);
    }

    if (command === 'bbox') {
        return cardboard.bboxQuery(bbox, dataset, function(err, collection) {
            if (err) throw err;
            console.log(JSON.stringify(collection));
        });
    }

    if (command === 'put') {
        var aggregator = new stream.Writable({ objectMode: true, highWaterMark: 75 });

        aggregator.features = [];
        aggregator.count = 0;
        aggregator.pending = 0;

        aggregator.collection = function() {
            return {
                type: 'FeatureCollection',
                features: aggregator.features
            };
        };

        aggregator._write = function(feature, enc, callback) {
            aggregator.features.push(feature);
            if (aggregator.features.length < 25) return callback();

            aggregator.pending++;
            cardboard.batch.put(aggregator.collection(), dataset, function(err) {
                aggregator.pending--;
                if (err) return callback(err);
                aggregator.count += aggregator.features.length;
                aggregator.features = [];
                callback();
            });
        };

        aggregator.done = aggregator.end.bind(aggregator);

        aggregator.end = function() {
            if (aggregator.pending) return setImmediate(aggregator.end);
            if (!aggregator.features.length) return aggregator.done();

            cardboard.batch.put(aggregator.collection(), dataset, function(err) {
                if (err) return aggregator.emit('error', err);
                aggregator.count += aggregator.features.length;
                aggregator.done();
            });
        };

        return process.stdin
          .pipe(parser)
            .on('error', function(err) { throw err; })
          .pipe(aggregator)
            .on('error', function(err) { throw err; })
            .on('finish', function() {
                console.log('Inserted %s features', aggregator.count);
            });
    }
});
