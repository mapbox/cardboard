# Cardboard

[index.js:37-579](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L37-L579 "Source code on GitHub")

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

[index.js:473-475](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L473-L475 "Source code on GitHub")

Incrementally update a dataset's metadata with a new feature. This operation **will** create a metadata record if one does not exist.

**Parameters**

-   `dataset` **string** the name of the dataset
-   `feature` **object** a GeoJSON feature (or backend record) being added to the dataset
-   `callback` **function** a function to handle the response

# deleteFeature

[index.js:499-501](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L499-L501 "Source code on GitHub")

Given a GeoJSON feature to remove, perform all required metadata updates. This operation **will not** create a metadata record if one does not exist. This operation **will not** shrink metadata bounds.

**Parameters**

-   `dataset` **string** the name of the dataset
-   `feature` **object** a GeoJSON feature (or backend record) to remove from the dataset
-   `callback` **function** a function to handle the response

# updateFeature

[index.js:487-489](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L487-L489 "Source code on GitHub")

Update a dataset's metadata with a change to a single feature. This operation **will not** create a metadata record if one does not exist.

**Parameters**

-   `dataset` **string** the name of the dataset
-   `from` **object** a GeoJSON feature (or backend record) representing the state of the feature _before_ the update
-   `to` **object** a GeoJSON feature (or backend record) representing the state of the feature _after_ the update
-   `callback` **function** a function to handle the response

# put

[lib/batch.js:29-68](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/lib/batch.js#L29-L68 "Source code on GitHub")

Insert or update a set of GeoJSON features

**Parameters**

-   `collection` **object** a GeoJSON FeatureCollection containing features to insert and/or update
-   `dataset` **string** the name of the dataset that these features belongs to
-   `callback` **function** the callback function to handle the response

# remove

[lib/batch.js:78-93](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/lib/batch.js#L78-L93 "Source code on GitHub")

Remove a set of features

**Parameters**

-   `ids` **Array&lt;string&gt;** an array of feature ids to remove
-   `dataset` **string** the name of the dataset that these features belong to
-   `callback` **function** the callback function to handle the response

# cardboard

[index.js:55-55](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L55-L55 "Source code on GitHub")

A client configured to interact with a backend cardboard database

## bboxQuery

[index.js:521-576](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L521-L576 "Source code on GitHub")

Find GeoJSON features that intersect a bounding box

**Parameters**

-   `bbox` **Array&lt;number&gt;** the bounding box as `[west, south, east, north]`
-   `dataset` **string** the name of the dataset
-   `options` **[Object]** Paginiation options. If omitted, the the bbox will
      return the first page, limited to 100 features
    -   `options.maxFeatures` **[number]** maximum number of features to return
    -   `options.start` **[Object]** Exclusive start key to use for loading the next page. This is a feature id.
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

[index.js:455-457](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L455-L457 "Source code on GitHub")

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

[index.js:232-241](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L232-L241 "Source code on GitHub")

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

[index.js:162-170](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L162-L170 "Source code on GitHub")

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

[index.js:264-276](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L264-L276 "Source code on GitHub")

Remove an entire dataset

**Parameters**

-   `dataset` **string** the name of the dataset
-   `callback` **function** the callback function to handle the response

## get

[index.js:204-215](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L204-L215 "Source code on GitHub")

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

[index.js:430-432](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L430-L432 "Source code on GitHub")

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

[index.js:321-383](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L321-L383 "Source code on GitHub")

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

[index.js:395-407](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L395-L407 "Source code on GitHub")

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

[index.js:115-128](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L115-L128 "Source code on GitHub")

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

[lib/batch.js:19-19](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/lib/batch.js#L19-L19 "Source code on GitHub")

A module for batch requests

# cardboard.metadata

[index.js:463-463](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L463-L463 "Source code on GitHub")

A module for incremental metadata adjustments

# utils

[lib/utils.js:16-16](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/lib/utils.js#L16-L16 "Source code on GitHub")

A module containing internal utility functions

## idFromRecord

[lib/utils.js:118-123](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/lib/utils.js#L118-L123 "Source code on GitHub")

Strips database-information from a DynamoDB record's id

**Parameters**

-   `record` **object** a DynamoDB record

Returns **string** id - the feature's identifier

## resolveFeatures

[lib/utils.js:23-60](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/lib/utils.js#L23-L60 "Source code on GitHub")

Convert a set of backend records into a GeoJSON features

**Parameters**

-   `dynamoRecords` **Array&lt;object&gt;** an array of items returned from DynamoDB in simplified JSON format
-   `callback` **function** a callback function to handle the response

## toDatabaseRecord

[lib/utils.js:77-111](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/lib/utils.js#L77-L111 "Source code on GitHub")

Converts a single GeoJSON feature into backend format

**Parameters**

-   `feature` **object** a GeoJSON feature
-   `dataset` **string** the name of the dataset the feature belongs to

Returns **Array&lt;object&gt;** the first element is a DynamoDB record suitable for inserting via `dyno.putItem`, the second are parameters suitable for uploading via `s3.putObject`.
