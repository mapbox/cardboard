var Cardboard = require('../');
var _ = require('lodash');
var DynamoDBClient = require('@aws-sdk/client-dynamodb').DynamoDBClient;
var DynamoDBDocumentClient = require('@aws-sdk/lib-dynamodb').DynamoDBDocumentClient;

var mainTable = require('@mapbox/dynamodb-test')(require('tape'), 'cardboard', require('../lib/main-table.json'));

var config = {
    region: 'test',
    mainTable: mainTable.tableName,
    endpoint: 'http://localhost:4567'
};

mainTable.test('pass preconfigured dynamodb client', function(assert) {

    var omitConfig = _.omit(config, ['accessKeyId', 'secretAccessKey', 'endpoint', 'region']);

    var featuresConfig = {
        accessKeyId: 'fake',
        secretAccessKey: 'fake',
        region: 'us-east-1',
        endpoint: 'http://localhost:4567'
    };

    omitConfig.dynamodb = DynamoDBDocumentClient.from(new DynamoDBClient({
        region: featuresConfig.region,
        endpoint: featuresConfig.endpoint,
        credentials: {
            accessKeyId: featuresConfig.accessKeyId,
            secretAccessKey: featuresConfig.secretAccessKey
        }
    }));
    var cardboard = Cardboard(omitConfig);
    var geojson = {type: 'Feature', properties: {}, geometry: {type: 'Point', coordinates:[1, 2]}};

    cardboard.put(geojson, 'default', function(err, fc) {
        assert.ifError(err);
        assert.deepEqual(geojson.geometry, fc.features[0].geometry);
        assert.end();
    });
});

mainTable.close();

