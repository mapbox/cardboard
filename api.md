# Cardboard

[index.js:39-645](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L39-L645 "Source code on GitHub")

Cardboard client generator

**Parameters**

-   `config` **object** a configuration object
    -   `config.table` **string** the name of a DynamoDB table to connect to
    -   `config.region` **string** the AWS region containing the DynamoDB table
    -   `config.bucket` **string** the name of an S3 bucket to use
    -   `config.prefix` **string** the name of a folder within the indicated S3 bucket
    -   `config.dyno` **[dyno]** a pre-configured [dyno client](https://github.com/mapbox/dyno) for connecting to DynamoDB
    -   `config.s3` **[s3]** a pre-configured [S3 client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)

**Examples**

```javascript
var cardboard = require('cardboard')({
  table: 'my-cardboard-table',
  region: 'us-east-1',
  bucket: 'my-cardboard-bucket',
  prefix: 'my-cardboard-prefix'
});
```

```javascript
var cardboard = require('cardboard')({
  dyno: require('dyno')(dynoConfig),
  bucket: 'my-cardboard-bucket',
  prefix: 'my-cardboard-prefix'
});
```

Returns **cardboard** a cardboard client

# addFeature

[index.js:475-477](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L475-L477 "Source code on GitHub")

Incrementally update a dataset's metadata with a new feature. This operation **will** create a metadata record if one does not exist.

**Parameters**

-   `dataset` **string** the name of the dataset
-   `feature` **object** a GeoJSON feature (or backend record) being added to the dataset
-   `callback` **function** a function to handle the response

# deleteFeature

[index.js:501-503](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L501-L503 "Source code on GitHub")

Given a GeoJSON feature to remove, perform all required metadata updates. This operation **will not** create a metadata record if one does not exist. This operation **will not** shrink metadata bounds.

**Parameters**

-   `dataset` **string** the name of the dataset
-   `feature` **object** a GeoJSON feature (or backend record) to remove from the dataset
-   `callback` **function** a function to handle the response

# updateFeature

[index.js:489-491](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L489-L491 "Source code on GitHub")

Update a dataset's metadata with a change to a single feature. This operation **will not** create a metadata record if one does not exist.

**Parameters**

-   `dataset` **string** the name of the dataset
-   `from` **object** a GeoJSON feature (or backend record) representing the state of the feature _before_ the update
-   `to` **object** a GeoJSON feature (or backend record) representing the state of the feature _after_ the update
-   `callback` **function** a function to handle the response

# put

[lib/batch.js:29-68](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/lib/batch.js#L29-L68 "Source code on GitHub")

Insert or update a set of GeoJSON features

**Parameters**

-   `collection` **object** a GeoJSON FeatureCollection containing features to insert and/or update
-   `dataset` **string** the name of the dataset that these features belongs to
-   `callback` **function** the callback function to handle the response

# remove

[lib/batch.js:78-93](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/lib/batch.js#L78-L93 "Source code on GitHub")

Remove a set of features

**Parameters**

-   `ids` **Array&lt;string&gt;** an array of feature ids to remove
-   `dataset` **string** the name of the dataset that these features belong to
-   `callback` **function** the callback function to handle the response

# cardboard

[index.js:57-57](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L57-L57 "Source code on GitHub")

A client configured to interact with a backend cardboard database

## bboxQuery

[index.js:519-642](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L519-L642 "Source code on GitHub")

Find GeoJSON features that intersect a bounding box

**Parameters**

-   `bbox` **Array&lt;number&gt;** the bounding box as `[west, south, east, north]`
-   `dataset` **string** the name of the dataset
-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
var bbox = [-120, 30, -115, 32]; // west, south, east, north
carboard.bboxQuery(bbox, 'my-dataset', function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
});
```

## calculateDatasetInfo

[index.js:457-459](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L457-L459 "Source code on GitHub")

Calculate metadata about a dataset

**Parameters**

-   `dataset` **string** the name of the dataset
-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
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

## createTable

[index.js:234-243](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L234-L243 "Source code on GitHub")

Create a DynamoDB table with Cardboard's schema

**Parameters**

-   `tableName` **[string]** the name of the table to create, if not provided, defaults to the tablename defined in client configuration.
-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
// Create the cardboard table specified by the client config
cardboard.createTable(function(err) {
  if (err) throw err;
});
```

```javascript
// Create the another cardboard table
cardboard.createTable('new-cardboard-table', function(err) {
  if (err) throw err;
});
```

## del

[index.js:164-172](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L164-L172 "Source code on GitHub")

Remove a single GeoJSON feature

**Parameters**

-   `primary` **string** the id for a feature
-   `dataset` **string** the name of the dataset that this feature belongs to
-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
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

```javascript
// Attempt to delete a feature that does not exist
cardboard.del('non-existent-feature', 'my-dataset', function(err, result) {
  err.message === 'Feature does not exist'; // true
  !!result; // false: nothing was removed
});
```

## delDataset

[index.js:266-278](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L266-L278 "Source code on GitHub")

Remove an entire dataset

**Parameters**

-   `dataset` **string** the name of the dataset
-   `callback` **function** the callback function to handle the response

## get

[index.js:206-217](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L206-L217 "Source code on GitHub")

Retrieve a single GeoJSON feature

**Parameters**

-   `primary` **string** the id for a feature
-   `dataset` **string** the name of the dataset that this feature belongs to
-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
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

```javascript
// Attempt to retrieve a feature that does not exist
cardboard.get('non-existent-feature', 'my-dataset', function(err, result) {
  err.message === 'Feature non-existent-feature does not exist'; // true
  !!result; // false: nothing was retrieved
});
```

## getDatasetInfo

[index.js:432-434](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L432-L434 "Source code on GitHub")

Get cached metadata about a dataset

**Parameters**

-   `dataset` **string** the name of the dataset
-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
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

## list

[index.js:323-385](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L323-L385 "Source code on GitHub")

List the GeoJSON features that belong to a particular dataset

**Parameters**

-   `dataset` **string** the name of the dataset
-   `pageOptions` **[object]** pagination options
    -   `pageOptions.start` **[string]** start reading features past the provided id
    -   `pageOptions.maxFeatures` **[number]** maximum number of features to return
-   `callback` **[function]** the callback function to handle the response

**Examples**

```javascript
// List all the features in a dataset
cardboard.list('my-dataset', function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
});
```

```javascript
// Stream all the features in a dataset
cardboard.list('my-dataset')
  .on('data', function(feature) {
    console.log('Got feature: %j', feature);
  })
  .on('end', function() {
    console.log('All done!');
  });
```

```javascript
// List one page with a max of 10 features from a dataset
cardboard.list('my-dataset', { maxFeatures: 10 }, function(err, collection) {
  if (err) throw err;
  collection.type === 'FeatureCollection'; // true
  collection.features.length <= 10; // true
});
```

```javascript
// Paginate through all the features in a dataset
(function list(start) {
  cardabord.list('my-dataset', {
    maxFeatures: 10,
    start: start
  }, function(err, collection) {
    if (err) throw err;
    if (!collection.features.length) return console.log('All done!');
    list(collection.features.slice(-1)[0].id);
  });
})();
```

Returns **object** a readable stream

## listDatasets

[index.js:397-409](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L397-L409 "Source code on GitHub")

List datasets available in this database

**Parameters**

-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
cardboard.listDatasets(function(err, datasets) {
  if (err) throw err;
  Array.isArray(datasets); // true
  console.log(datasets[0]); // 'my-dataset'
});
```

## put

[index.js:117-130](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L117-L130 "Source code on GitHub")

Insert or update a single GeoJSON feature

**Parameters**

-   `feature` **object** a GeoJSON feature
-   `dataset` **string** the name of the dataset that this feature belongs to
-   `callback` **function** the callback function to handle the response

**Examples**

```javascript
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

```javascript
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

```javascript
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

# cardboard.batch

[lib/batch.js:19-19](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/lib/batch.js#L19-L19 "Source code on GitHub")

A module for batch requests

# cardboard.metadata

[index.js:465-465](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/index.js#L465-L465 "Source code on GitHub")

A module for incremental metadata adjustments

# utils

[lib/utils.js:16-16](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/lib/utils.js#L16-L16 "Source code on GitHub")

A module containing internal utility functions

## idFromRecord

[lib/utils.js:106-111](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/lib/utils.js#L106-L111 "Source code on GitHub")

Strips database-information from a DynamoDB record's id

**Parameters**

-   `record` **object** a DynamoDB record

Returns **string** id - the feature's identifier

## resolveFeatures

[lib/utils.js:23-48](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/lib/utils.js#L23-L48 "Source code on GitHub")

Convert a set of backend records into a GeoJSON features

**Parameters**

-   `dynamoRecords` **Array&lt;object&gt;** an array of items returned from DynamoDB in simplified JSON format
-   `callback` **function** a callback function to handle the response

## toDatabaseRecord

[lib/utils.js:65-99](https://github.com/mapbox/cardboard/blob/3b3ae0afbaf603775988d942e10339a7e67986cb/lib/utils.js#L65-L99 "Source code on GitHub")

Converts a single GeoJSON feature into backend format

**Parameters**

-   `feature` **object** a GeoJSON feature
-   `dataset` **string** the name of the dataset the feature belongs to

Returns **Array&lt;object&gt;** the first element is a DynamoDB record suitable for inserting via `dyno.putItem`, the second are parameters suitable for uploading via `s3.putObject`.
