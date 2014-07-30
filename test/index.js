var test = require('tap').test,
    fs = require('fs'),
    queue = require('queue-async'),
    concat = require('concat-stream'),
    Cardboard = require('../'),
    geojsonExtent = require('geojson-extent'),
    fixtures = require('./fixtures'),
    AWS = require('aws-sdk');



if(!fs.existsSync('../.env.test')) {
    console.log('AWSKey and AWSSecret must be set to run tests. You can do that .env.test');
    process.exit(1);
}

var dotenv = require('dotenv');
dotenv._getKeysAndValuesFromEnvFilePath('../.env.test');
dotenv._setEnvs();

if(!process.env.AWSKey || ! process.env.AWSSecret) {
    console.log('AWSKey and AWSSecret must be set to run tests. You can do that .env.test');
    process.exit(1);
}

var config = {
    awsKey: process.env.AWSKey,
    awsSecret: process.env.AWSSecret,
    bucket: process.env.Bucket || 'mapbox-s2',
    prefix: 'test',
    layer: 'default'
};

AWS.config.update({
    accessKeyId: config.awsKey,
    secretAccessKey: config.awsSecret,
    region: 'us-east-1'
});
var s3 = new AWS.S3();


function clearLayer(cb) {
    var params = {
        Bucket: config.bucket,
        Delimiter: '/',
        Prefix: config.prefix+'/'+config.layer+'/cell/'
    };

    s3.listObjects(params, listResp);

    function listResp(err, data) {
        if (err) throw err;
        var keys = data.Contents.map(function(c){
            return {Key:c.Key};
        });

        if(keys.length === 0) {
            return cb();
        }
        params = {
            Bucket: config.bucket,
            Delete: {
                Objects: keys
            }
        };
        s3.deleteObjects(params, delResp);
    }
    function delResp(err, data){
        if(err) throw err;
        cb();
    }
}

function clear() {
    test('clear', function(t) {
        clearLayer(function(){
            t.end();
        });
    });
}
clear(test);
test('insert & dump', function(t) {
    var cardboard = new Cardboard(config);

    cardboard.insert('hello', fixtures.nullIsland, 'default', function(err) {
        t.equal(err, null);
        t.pass('inserted');
        t.end();
    });
});

clear();

test('insert & query', function(t) {
    var queries = [
        {
            query: [-10, -10, 10, 10],
            length: 1
        },
        {
            query: [30, 30, 40, 40],
            length: 0
        },
        {
            query: [10, 10, 20, 20],
            length: 0
        },
        {
            query: [-76.0, 38.0, -79, 40],
            length: 1
        }
    ];
    var cardboard = new Cardboard(config);
    var insertQueue = queue(1);

    [['nullisland', fixtures.nullIsland],
    ['dc', fixtures.dc]].forEach(function(fix) {
        insertQueue.defer(cardboard.insert.bind(cardboard), fix[0], fix[1], 'default');
    });

    insertQueue.awaitAll(inserted);

    function inserted() {
        var q = queue(1);
        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, data) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() {
            t.end(); });
    }
});

clear();

test('insert polygon', function(t) {
    var cardboard = new Cardboard(config);
    cardboard.insert('us', fixtures.haiti, 'default', inserted);

    function inserted() {
        var queries = [
            {
                query: [-10, -10, 10, 10],
                length: 0
            },
            {
                query: [-76.0, 38.0, -79, 40],
                length: 0
            }
        ];
        var q = queue(1);
        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, data) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() { t.end(); });
    }
});

clear();

test('insert linestring', function(t) {
    var cardboard = new Cardboard(config);
    cardboard.insert('us', fixtures.haitiLine, 'default', inserted);

    function inserted() {
        var queries = [
            {
                query: [-10, -10, 10, 10],
                length: 0
            },
            {
                query: [-76.0, 38.0, -79, 40],
                length: 0
            }
        ];
        var q = queue(1);
        queries.forEach(function(query) {
            q.defer(function(query, callback) {
                t.equal(cardboard.bboxQuery(query.query, 'default', function(err, data) {
                    t.equal(err, null, 'no error for ' + query.query.join(','));
                    t.equal(data.length, query.length, 'finds ' + query.length + ' data with a query');
                    callback();
                }), undefined, '.bboxQuery');
            }, query);
        });
        q.awaitAll(function() { t.end(); });
    }
});
clear();
