# cardboard

[![Build Status](https://travis-ci.org/mapbox/cardboard.svg?branch=master)](https://travis-ci.org/mapbox/cardboard) [![Coverage Status](https://coveralls.io/repos/mapbox/cardboard/badge.svg?branch=master)](https://coveralls.io/r/mapbox/cardboard?branch=master)

Cardboard is a JavaScript library for managing the storage of GeoJSON features on an AWS backend. It relies on DynamoDB for indexing and small-feature storage, and S3 for large-feature storage. Cardboard provides functions to create, read, update, and delete single features or in batch, as well as simple bounding-box spatial query capabilities.

## Installation

    npm install cardboard
    # or globally
    npm install -g cardboard

## Configuration

Generate a client by passing the following configuration options to cardboard:

option | required | description
--- | --- | ---
mainTable | X | the name of the DynamoDB table to use
region | X | the region containing the given DynamoDB table
accessKeyId | | AWS credentials
secretAccessKey | | AWS credentials
sessionToken | | AWS credentials
dyno | | a pre-configured [dyno client](https://github.com/mapbox/dyno) to use for DynamoDB interactions

Providing AWS credentials is optional. Cardboard depends on the AWS SDK for JavaScript, and so credentials can be provided in any way supported by that library. See [configuring the SDK in Node.js](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html) for more configuration options.

If you provide a preconfigured [dyno client](https://github.com/mapbox/dyno), you do not need to specify `table` and `region` when initializing cardboard.

#### Example

```js
var Cardboard = require('cardboard');
var cardboard = Cardboard({
    mainTable: 'my-cardboard-table',
    region: 'us-east-1',
});- '6.9'
```

## Creating a Cardboard table

Once you've initialized the client, you can use it to create a table for you:

```js
cardboard.createTable(callback);
```

You don't have to create the table each time; you can provide the name of a pre-existing table to your configuration options to use that table.

## API documentation

See [api.md](https://github.com/mapbox/cardboard/blob/master/api.md).

## Concepts

### Datasets

Most cardboard functions require you to specify a `dataset`. This is a way of grouping sets of features within a single Cardboard table. It is similar in concept to "layers" in many other GIS systems, but there are no restrictions on the types of features that can be associated with each other in a single `dataset`. Each feature managed by cardboard can only belong to one `dataset`.

### Identifiers

Features within a single `dataset` must each have a unique `id`. Cardboard uses a GeoJSON feature's top-level `id` property to determine and persist the feature's identifier. If you provide a cardboard function with a GeoJSON feature that does not have an `id` property, it will assign one for you, otherwise, it will use the `id` that you provide. *Be aware* that inserting two features to a single dataset with the same `id` value will result in only the last feature being persisted in cardboard.

### Collections

Whenever dealing with individual GeoJSON features, cardboard will expect or return a GeoJSON object of type `Feature`. In batch situations, or in any request that returns multiple features, cardboard will expect/return a `FeatureCollection`.

### Precision

Cardboard retains the precision of a feature's coordinates to six decimal places.
