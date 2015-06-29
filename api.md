## [Cardboard](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L39-L599)

Cardboard client generator

### Parameters

* `config` **`object`** a configuration object


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





## [cardboard](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L57-L59)

A client configured to interact with a backend cardboard database








### [bboxQuery](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L473-L596)

Find GeoJSON features that intersect a bounding box

#### Parameters

* `bbox` **`Array<number>`** the bounding box as `[west, south, east, north]`
* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response


#### Examples

```js
var bbox = [-120, 30, -115, 32]; // west, south, east, north
carboard.bboxQuery(bbox, 'my-dataset', function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
});
```



### [calculateDatasetInfo](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L457-L459)

Calculate metadata about a dataset

#### Parameters

* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response


#### Examples

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



### [createTable](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L235-L244)

Create a DynamoDB table with Cardboard's schema

#### Parameters

* `tableName` **`[string]`** the name of the table to create, if not provided, defaults to the tablename defined in client configuration.
* `callback` **`function`** the callback function to handle the response


#### Examples

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



### [del](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L165-L173)

Remove a single GeoJSON feature

#### Parameters

* `primary` **`string`** the id for a feature
* `dataset` **`string`** the name of the dataset that this feature belongs to
* `callback` **`function`** the callback function to handle the response


#### Examples

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



### [delDataset](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L269-L281)

Remove an entire dataset

#### Parameters

* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response





### [get](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L207-L218)

Retreive a single GeoJSON feature

#### Parameters

* `primary` **`string`** the id for a feature
* `dataset` **`string`** the name of the dataset that this feature belongs to
* `callback` **`function`** the callback function to handle the response


#### Examples

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



### [getDatasetInfo](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L432-L434)

Get cached metadata about a dataset

#### Parameters

* `dataset` **`string`** the name of the dataset
* `callback` **`function`** the callback function to handle the response


#### Examples

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



### [list](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L326-L385)

List the GeoJSON features that belong to a particular dataset

#### Parameters

* `dataset` **`string`** the name of the dataset
* `pageOptions` **`[object]`** pagination options
* `callback` **`function`** the callback function to handle the response


#### Examples

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



### [listDatasets](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L397-L409)

List datasets available in this database

#### Parameters

* `callback` **`function`** the callback function to handle the response


#### Examples

```js
cardboard.listDatasets(function(err, datasets) {
  if (err) throw err;
  Array.isArray(datasets); // true
  console.log(datasets[0]); // 'my-dataset'
});
```



### [put](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/index.js#L118-L131)

Insert or update a single GeoJSON feature

#### Parameters

* `feature` **`object`** a GeoJSON feature
* `dataset` **`string`** the name of the dataset that this feature belongs to
* `callback` **`function`** the callback function to handle the response


#### Examples

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




## [cardboard.batch](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/batch.js#L17-L17)

A module for batch requests








### [put](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/batch.js#L27-L54)

Insert or update a set of GeoJSON features

#### Parameters

* `collection` **`object`** a GeoJSON FeatureCollection containing features to insert and/or update
* `dataset` **`string`** the name of the dataset that these features belongs to
* `callback` **`function`** the callback function to handle the response





### [remove](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/batch.js#L64-L72)

Remove a set of features

#### Parameters

* `ids` **`Array<string>`** an array of feature ids to remove
* `dataset` **`string`** the name of the dataset that these features belong to
* `callback` **`function`** the callback function to handle the response






## [metadata](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/metadata.js#L10-L10)

A client for interacting with the metadata for a dataset








### [addFeature](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/metadata.js#L166-L177)

Given a GeoJSON feature, perform all required metadata updates. This operation **will** create a metadata record if one does not exist.

#### Parameters

* `feature` **`object`** a GeoJSON feature being added to the dataset
* `callback` **`function`** a function to handle the response





### [calculateInfo](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/metadata.js#L87-L120)

Find all features in a dataset and bring metadata record up-to-date

#### Parameters

* `callback` **`function`** a function to handle the response





### [deleteFeature](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/metadata.js#L202-L208)

Given a GeoJSON feature to remove, perform all required metadata updates. This operation **will not** create a metadata record if one does not exist. This operation **will not** shrink metadata bounds.

#### Parameters

* `feature` **`object`** a GeoJSON feature to remove from the dataset
* `callback` **`function`** a function to handle the response





### [getFeatureInfo](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/metadata.js#L48-L58)

Return the details for a given GeoJSON feature

#### Parameters

* `feature` **`object`** a GeoJSON feature



Returns `object` an object describing the feature's size and extent


### [getInfo](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/metadata.js#L35-L41)

Return dataset metadata or an empty object

#### Parameters

* `callback` **`function`** a callback function to handle the response





### [updateFeature](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/metadata.js#L185-L195)

Given before and after states of a GeoJSON feature, perform all required metadata adjustments. This operation **will not** create a metadata record if one does not exist.

#### Parameters

* `from` **`object`** a GeoJSON feature representing the state of the feature *before* the update
* `to` **`object`** a GeoJSON feature representing the state of the feature *after* the update
* `callback` **`function`** a function to handle the response






## [utils](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/utils.js#L14-L14)

A module containing internal utility functions








### [resolveFeatures](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/utils.js#L21-L45)

Convert a set of backend records into a GeoJSON features

#### Parameters

* `dynamoRecords` **`Array<object>`** an array of items returned from DynamoDB in simplified JSON format
* `callback` **`function`** a callback function to handle the response





### [toDatabaseRecord](https://github.com/mapbox/cardboard/blob/bd91103dc02c9a240b166fdb23b625a253da38af/lib/utils.js#L63-L92)

Converts a single GeoJSON feature into backend format

#### Parameters

* `feature` **`object`** a GeoJSON feature
* `dataset` **`string`** the name of the dataset the feature belongs to



Returns  the first element is a DynamoDB record suitable for inserting via `dyno.putItem`, the second are parameters suitable for uploading via `s3.putObject`.



