# Cardboard

[index.js:37-579](https://github.com/mapbox/cardboard/blob/fd3db9ca7455e6d76ab1654e77cd6e8b6e882734/index.js#L37-L579 "Source code on GitHub")

Cardboard client generator

**Parameters**

-   `config` **object** a configuration object
    -   `config.mainTable` **string** the name of a DynamoDB table to connect to
    -   `config.region` **string** the AWS region containing the DynamoDB table
    -   `config.dyno` **[dyno]** a pre-configured [dyno client](https://github.com/mapbox/dyno) for connecting to DynamoDB

**Examples**

```js
var cardboard = require('cardboard')({
  table: 'my-cardboard-table',
  region: 'us-east-1',
});
```

```js
var cardboard = require('cardboard')({
  dyno: require('dyno')(dynoConfig),
});
```

Returns **cardboard** a cardboard client

## put

**Parameters**

- `input` *Feature|FeatureCollection* a GeoJSON object to write to the database
- `dataset` *string* the dataset to write the feature to
- `callback` *function* the callback for the error and repsonse to be passed to

**Examples**

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
  err === undefined; // no error
  result.features.length === 1; // one feature was added
  result.features[0].id === 'random-id'; // that is a bit more random
});
```

## del

Delete features from a dataset

**Parameters**

- `input` **string|Array(string)** the feature ids
- `dataset` **string** the id of the dataset that these features belong to
- `callback` **function** the callback function to handle the response

**Examples**

```js
carboard.del('feature-id', 'dataset-id', function(err) {
  err === undefined; // unless something is really broken
});
```

```js
carboard.del(['feature-one', 'feature-two'], 'dataset-id', function(err) {
  err === undefined; // unless something is really broken
});
```

## get

Retrieve a feature collection for a list of feature ids

**Parameters**

- `input` **string|Array(string)** the feature ids
- `dataset` **string** the id of the dataset that these features belong to
- `callback` **function** the callback function to handle the response

**Examples**

```js
cardboard.get('feature-id', 'my-dataset', function(err, final) {
  if (err) throw err;
  final.features[0].id === 'feature-id'; // true: the feature was retrieved
});
```

```js
// get multiple features at once
cardboard.get(['one', 'two'], 'my-dataset', function(err, result) {
  err === undefined;
  result.features.length === 2; // a feature for each id
});
```

```js
// Attempt to retrieve a feature that does not exist
cardboard.get('non-existent-feature', 'my-dataset', function(err, result) {
  err === undefined; // this is not an error
  result.features.length === 0; // nothing was found
});
```

## createTable

Create a DynamoDB table with Cardboard's schema

**Parameters**

-   `callback` **function** the callback function to handle the response

**Examples**

```js
// Create the cardboard table specified by the client config
cardboard.createTable(function(err) {
  if (err) throw err;
});
```

