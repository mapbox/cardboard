## `Cardboard`

Cardboard client generator

### Parameters

* `config` **`object`** a configuration object
* `config.table` **`string`** the name of a DynamoDB table to connect to
* `config.region` **`string`** the AWS region containing the DynamoDB table
* `config.bucket` **`string`** the name of an S3 bucket to use
* `config.prefix` **`string`** the name of a folder within the indicated S3 bucket
* `config.dyno` **`[dyno]`** a pre-configured [dyno client](https://github.com/mapbox/dyno) for connecting to DynamoDB
* `config.s3` **`[s3]`** a pre-configured [S3 client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)


### Examples

```js
var cardboard = require('cardboard')({
  table: 'my-cardboard-table',
  region: 'us-east-1',
  bucket: 'my-cardboard-bucket',
  prefix: 'my-cardboard-prefix'
});
```
```js
var cardboard = require('cardboard')({
  dyno: require('dyno')(dynoConfig),
  bucket: 'my-cardboard-bucket',
  prefix: 'my-cardboard-prefix'
});
```

Returns `cardboard` a cardboard client


## `addFeature`

Given a GeoJSON feature, perform all required metadata updates. This operation **will** create a metadata record if one does not exist.

### Parameters

* `feature` **`object`** a GeoJSON feature being added to the dataset
* `callback` **`function`** a function to handle the response





## `bboxQuery`

Find GeoJSON features that intersect a bounding box

### Parameters

* `bbox` **`Array<number>`** the bounding box as `[west, south, east, north]`
* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response


### Examples

```js
var bbox = [-120, 30, -115, 32]; // west, south, east, north
carboard.bboxQuery(bbox, 'my-dataset', function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
});
```



## `calculateDatasetInfo`

Calculate metadata about a dataset

### Parameters

* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response


### Examples

```js
cardboard.calculateDatasetInfo('my-dataset', function(err, metadata) {
  if (err) throw err;
  console.log(Object.keys(metadatata));
  // [
  //   'dataset',
  //   'id',
  //   'west',
  //   'south',
  //   'east',
  //   'north',
  //   'count',
  //   'size',
  //   'updated'
  // ]
});
```



## `calculateInfo`

Find all features in a dataset and bring metadata record up-to-date

### Parameters

* `callback` **`function`** a function to handle the response





## `cardboard`

A client configured to interact with a backend cardboard database






## `cardboard.batch`

A module for batch requests






## `createTable`

Create a DynamoDB table with Cardboard's schema

### Parameters

* `tableName` **`[string]`** the name of the table to create, if not provided, defaults to the tablename defined in client configuration.
* `callback` **`function`** the callback function to handle the response


### Examples

```js
// Create the cardboard table specified by the client config
cardboard.createTable(function(err) {
  if (err) throw err;
});
```
```js
// Create the another cardboard table
cardboard.createTable('new-cardboard-table', function(err) {
  if (err) throw err;
});
```



## `del`

Remove a single GeoJSON feature

### Parameters

* `primary` **`string`** the id for a feature
* `dataset` **`string`** the name of the dataset that this feature belongs to
* `callback` **`function`** the callback function to handle the response


### Examples

```js
// Create a point, then delete it
var feature = {
  id: 'my-custom-id',
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;

  cardboard.del(result.id, 'my-dataset', function(err, result) {
    if (err) throw err;
    !!result; // true: the feature was removed
  });
});
```
```js
// Attempt to delete a feature that does not exist
cardboard.del('non-existent-feature', 'my-dataset', function(err, result) {
  err.message === 'Feature does not exist'; // true
  !!result; // false: nothing was removed
});
```



## `delDataset`

Remove an entire dataset

### Parameters

* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response





## `deleteFeature`

Given a GeoJSON feature to remove, perform all required metadata updates. This operation **will not** create a metadata record if one does not exist. This operation **will not** shrink metadata bounds.

### Parameters

* `feature` **`object`** a GeoJSON feature to remove from the dataset
* `callback` **`function`** a function to handle the response





## `get`

Retreive a single GeoJSON feature

### Parameters

* `primary` **`string`** the id for a feature
* `dataset` **`string`** the name of the dataset that this feature belongs to
* `callback` **`function`** the callback function to handle the response


### Examples

```js
// Create a point, then retrieve it.
var feature = {
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  result.geometry.coordinates = [1, 1];

  cardboard.get(result.id, 'my-dataset', function(err, final) {
    if (err) throw err;
    final === result; // true: the feature was retrieved
  });
});
```
```js
// Attempt to retrieve a feature that does not exist
cardboard.get('non-existent-feature', 'my-dataset', function(err, result) {
  err.message === 'Feature non-existent-feature does not exist'; // true
  !!result; // false: nothing was retrieved
});
```



## `getDatasetInfo`

Get cached metadata about a dataset

### Parameters

* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response


### Examples

```js
cardboard.getDatasetInfo('my-dataset', function(err, metadata) {
  if (err) throw err;
  console.log(Object.keys(metadatata));
  // [
  //   'dataset',
  //   'id',
  //   'west',
  //   'south',
  //   'east',
  //   'north',
  //   'count',
  //   'size',
  //   'updated'
  // ]
});
```



## `getFeatureInfo`

Return the details for a given GeoJSON feature

### Parameters

* `feature` **`object`** a GeoJSON feature



Returns `object` an object describing the feature's size and extent


## `getInfo`

Return dataset metadata or an empty object

### Parameters

* `callback` **`function`** a callback function to handle the response





## `list`

List the GeoJSON features that belong to a particular dataset

### Parameters

* `dataset` **`string`** the name of the dataset
* `pageOptions` **`[object]`** pagination options
* `pageOptions.start` **`[string]`** start reading features past the provided id
* `pageOptions.maxFeatures` **`[number]`** maximum number of features to return
* `callback` **`function`** the callback function to handle the response


### Examples

```js
// List all the features in a dataset
cardboard.list('my-dataset', function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
});
```
```js
// Stream all the features in a dataset
cardboard.list('my-dataset')
  .on('data', function(feature) {
    console.log('Got feature: %j', feature);
  })
  .on('end', function() {
    console.log('All done!');
  });
```
```js
// List one page with a max of 10 features from a dataset
cardboard.list('my-dataset', { maxFeatures: 10 }, function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
  collection.features.length <= 10; // true
});
```
```js
// Paginate through all the features in a dataset
(function list(startAfter) {
  var options = { maxFeatures: 10 };
  if (startAfter) options.start = startFrom;
  cardabord.list('my-dataset', options, function(err, collection) {
    if (err) throw err;
    if (!collection.features.length) return console.log('All done!');

    var lastId = collection.features.slice(-1)[0].id;
    list(lastId);
  });
})();
```



## `listDatasets`

List datasets available in this database

### Parameters

* `callback` **`function`** the callback function to handle the response


### Examples

```js
cardboard.listDatasets(function(err, datasets) {
  if (err) throw err;
  Array.isArray(datasets); // true
  console.log(datasets[0]); // 'my-dataset'
});
```



## `metadata`

A client for interacting with the metadata for a dataset






## `put`

Insert or update a single GeoJSON feature

### Parameters

* `feature` **`object`** a GeoJSON feature
* `dataset` **`string`** the name of the dataset that this feature belongs to
* `callback` **`function`** the callback function to handle the response


### Examples

```js
// Create a point, allowing Cardboard to assign it an id.
var feature = {
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  !!result.id; // true: an id has been assigned
});
```
```js
// Create a point, using a custom id.
var feature = {
  id: 'my-custom-id',
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  result.id === feature.id; // true: the custom id was preserved
});
```
```js
// Create a point, then move it.
var feature = {
  type: 'Feature',
  properties: {},
  geometry: {
    type: 'Point',
    coordinates: [0, 0]
  }
};

cardboard.put(feature, 'my-dataset', function(err, result) {
  if (err) throw err;
  result.geometry.coordinates = [1, 1];

  cardboard.put(result, 'my-dataset', function(err, final) {
    if (err) throw err;
    final.geometry.coordinates[0] === 1; // true: the feature was moved
  });
});
```



## `put`

Insert or update a set of GeoJSON features

### Parameters

* `collection` **`object`** a GeoJSON FeatureCollection containing features to insert and/or update
* `dataset` **`string`** the name of the dataset that these features belongs to
* `callback` **`function`** the callback function to handle the response





## `remove`

Remove a set of features

### Parameters

* `ids` **`Array<string>`** an array of feature ids to remove
* `dataset` **`string`** the name of the dataset that these features belong to
* `callback` **`function`** the callback function to handle the response





## `resolveFeatures`

Convert a set of backend records into a GeoJSON features

### Parameters

* `dynamoRecords` **`Array<object>`** an array of items returned from DynamoDB in simplified JSON format
* `callback` **`function`** a callback function to handle the response





## `toDatabaseRecord`

Converts a single GeoJSON feature into backend format

### Parameters

* `feature` **`object`** a GeoJSON feature
* `dataset` **`string`** the name of the dataset the feature belongs to



Returns  the first element is a DynamoDB record suitable for inserting via `dyno.putItem`, the second are parameters suitable for uploading via `s3.putObject`.


## `updateFeature`

Given before and after states of a GeoJSON feature, perform all required metadata adjustments. This operation **will not** create a metadata record if one does not exist.

### Parameters

* `from` **`object`** a GeoJSON feature representing the state of the feature *before* the update
* `to` **`object`** a GeoJSON feature representing the state of the feature *after* the update
* `callback` **`function`** a function to handle the response





## `utils`

A module containing internal utility functions






