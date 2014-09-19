# cardboard

[![build status](https://secure.travis-ci.org/mapbox/cardboard.png)](http://travis-ci.org/mapbox/cardboard)

Cardboard provides the query, indexing, and storage logic for GeoJSON feature
storage with CRUD on DynamoDB and S3.

## install

    npm install cardboard
    # or globally
    npm install -g cardboard

## api

```js
var c = Cardboard({
    accessKeyId: config.awsKey,
    secretAccessKey: config.awsSecret,
    sessionToken: config.sessionToken,
    table: config.DynamoDBTable,
    endpoint: 'http://localhost:4567',
    bucket: 'test',
    prefix: 'test'
});
```

`accessKeyId`, `secretAccessKey`, and `sessionToken` are optional. See
[Configuring the SDK in Node.js][config] for more configuration options.

[config]:http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html

Initialize a new cardboard database connector given a config object that is
sent to [dyno](http://github.com/mapbox/dyno).

```js
c.createTable(tableName, callback);
```

Create a cardboard table with the specified name.

```js
c.insert(primarykey: string, feature: object, layer: string, callback: fn);
```

Insert a single feature, indexing it with a primary key in a given layer.

```js
// query a bbox, callback-return array of geojson
c.bboxQuery(bbox: array, layer: string, callback: fn);
// delete a feature
c.del(primarykey: string, layer: string, callback: fn);
c.dump(); // -> stream
c.export(); // -> stream
// get information about a dataset (feature count, size, extent)
c.getDatasetInfo(dataset: string, callback: fn);
```

## Approach

This project aims to create a simple, fast geospatial index as a layer on top
of [DynamoDB](https://aws.amazon.com/dynamodb/). This means that the index will
not be built into the database or contained in a single R-Tree - it will be
baked into the indexes by which data is stored.

While the whole index in kept in DynamoDB, the larger geometries are stored on
S3.

Support target:

* [All GeoJSON geometry types](http://tools.ietf.org/html/draft-butler-geojson-04#section-2.1) should be storable
* BBOX queries should be supported
