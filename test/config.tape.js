var Cardboard = require('../');
var _ = require('lodash');
var Dyno = require('dyno');

var mainTable = require('dynamodb-test')(require('tape'), 'cardboard', require('../lib/main_table.json'));

var config = {
  region: 'test',
  mainTable: mainTable.tableName,
  endpoint: 'http://localhost:4567'
};

mainTable.test('pass preconfigured dyno object', function(assert) {

    var omitConfig = _.omit(config, ['accessKeyId', 'secretAccessKey', 'table', 'endpoint', 'region']);

    var featuresConfig = {
        accessKeyId: 'fake',
        secretAccessKey: 'fake',
        region: 'us-east-1',
        table: 'features',
        endpoint: 'http://localhost:4567'
    };

    omitConfig.dyno = Dyno(featuresConfig, featuresConfig);
    var cardboard = Cardboard(omitConfig);
    var geojson = {type: 'Feature', properties: {}, geometry: {type: 'Point', coordinates:[1, 2]}};

    cardboard.put(geojson, 'default', function(err, fc) {
        assert.ifError(err);
        assert.deepEqual(geojson.geometry, fc.features[0].geometry);
        assert.end();
    });
});

mainTable.close();

