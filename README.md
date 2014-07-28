# cardboard

[![build status](https://secure.travis-ci.org/mapbox/cardboard.png)](http://travis-ci.org/mapbox/cardboard)

Demo platform for [s2](https://github.com/mapbox/node-s2) on LevelDB.
This provides query, indexing, and storage logic. Look to `node-s2` for
bindings and higher level code for interfaces.

## install

    npm install cardboard
    # or globally
    npm install -g cardboard

## api

```js
var c = Cardboard(config)
```

Initialize a new cardboard database connector given a config object that is
sent to dyno.

```js
c.insert(primarykey: string, feature: object, layer: string, callback: fn);
```

Insert a single feature, indexing it with a primary key in a given layer.

```js
c.bboxQuery(bbox: array, layer: string, callback: fn);
c.dumpGeoJSON(callback: fn);
c.dump(); // -> stream
c.export(); // -> stream
```

## Approach

This project aims to create a simple, fast geospatial index as a layer on top
of [LevelDB](http://code.google.com/p/leveldb/) and [DynamoDB](https://aws.amazon.com/dynamodb/). This
means that the index will not be built into the database or
contained in a single R-Tree - it will be baked into the indexes by which data is stored.

Support target:

* [All GeoJSON geometry types](http://geojson.org/geojson-spec.html#geometry-objects) should be storable
* BBOX queries should be supported
