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
table | X | the name of the DynamoDB table to use
region | X | the region containing the given DynamoDB table
bucket | X | the name of an S3 bucket to use for large-object storage
prefix | X | a folder prefix to use within the S3 bucket
accessKeyId | | AWS credentials
secretAccessKey | | AWS credentials
sessionToken | | AWS credentials
dyno | | a pre-configured [dyno client](https://github.com/mapbox/dyno) to use for DynamoDB interactions
s3 | | a pre-configured [s3 client](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html) to use for S3 interactions

Providing AWS credentials is optional. Cardboard depends on the AWS SDK for JavaScript, and so credentials can be provided in any way supported by that library. See [configuring the SDK in Node.js](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html) for more configuration options.

If you provide a preconfigured [dyno client](https://github.com/mapbox/dyno), you do not need to specify `table` and `region` when initializing cardboard.

#### Example

```js
var Cardboard = require('cardboard');
var cardboard = Cardboard({
    table: 'my-cardboard-table',
    region: 'us-east-1',
    bucket: 'my-cardboard-bucket',
    prefix: 'test'
});
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

### Pagination

As datasets become large, retrieving all the features they contain can become a prohibitively expensive / slow operation. Functions in cardboard that may return large numbers of features allow you to provide pagination options, allowing you to gather all the features in a single dataset through a series of consecutive requests.

Pagination options are an object with two properties:

option | type | description
--- | --- | ---
maxFeatures | number | instructs cardboard to provide *no more than* this many features in a single `.list()` request
start | string | [optional] instructs cardboard to begin providing results *after* the specified key.

Cardboard will attempt to return `maxFeatures` number of results per paginated request. However, if the individual features in the dataset are very large, or you've specifed `maxFeatures` very high, cardboard may return fewer results. It will never return more than this number of features.

Once you've received a set of results, find the id of the last feature in the FeatureCollection, i.e.

```js
var lastId = featureCollection.features.pop().id;
```

By using this as the `start` option for the next request, cardboard will provide you with the next set of results.

You have received all the features when the request returns a FeatureCollection with no features in it.

#### Example: paginated cardboard.list()

```js
var Cardboard = require('cardboard');
var cardboard = Cardboard({
    table: 'my-cardboard-table',
    region: 'us-east-1',
    bucket: 'my-cardboard-bucket',
    prefix: 'test'
});

var features = [];
getFeatures();

function getFeatures(start) {
    var options = { maxFeatures: 10 };
    if (start) options.start = start;

    cardboard.list('my-dataset', options, function(err, featureCollection) {
        if (err) throw err;
        if (!featureCollection.features.length) return;

        features = features.concat(featureCollection.features);

        var lastId = featureCollection.features.pop().id;
        getFeatures(lastId);
    });
}
```

### Metadata

Metadata can be stored pertaining to each dataset in the cardboard table:

property | description
--- | ---
west | west-bound of dataset's extent
south | south-bound of dataset's extent
east | east-bound of dataset's extent
north | north-bound of dataset's extent
count | number of features in the dataset
size | approximate size (in bytes) of the entire dataset
updated | unix timestamp of the last update to this metadata record

Use the `cardboard.datasets.info` function to retrieve a dataset's metadata. By default, dataset metadata *is not* updated incrementally as features are added, updated, or removed. The metadata record can be updated by calling `cardboard.datasets.calculateInfo`. This operation gathers all the features in the dataset and recalculates the metadata cache.

`cardboard.datasets.addFeature`, `cardboard.datasets.updateFeature`, and `cardboard.datasets.removeFeature` provide mechanisms to incrementally adjust metadata information on a per-feature basis. Note that these operations *will only expand* the extent information. If you've performed numerous deletes and need to contract the extent, use `cardboard.datasets.calculateInfo`.

### Precision

Cardboard retains the precision of a feature's coordinates to six decimal places.
