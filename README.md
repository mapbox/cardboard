# cardboard

Demo platform for `s2-index` on leveldb.

## Approach

This project aims to create a simple, fast geospatial index as a layer on top of LevelDB. This means that the index will not be built into the database or contained in a single R-Tree - it will be baked into the indexes by which data is stored.

Support target:

* [All GeoJSON geometry types](http://geojson.org/geojson-spec.html#geometry-objects) should be storable
* BBOX queries should be supported

### How are polygons indexed?

We generate an S2 cover of `S2CellId`s that covers non-point shapes with viable minimums of cells.

### How do partial overlaps work?

Not sure yet.

### How is data stored?

Currently as stringified GeoJSON

### How are points stored

As S2CellIds that cover the point for a range of S2 levels

### What are the keys like?

You need to specify a stringable primary key on the way in. The final keys look like

    cell!S2CELLID!PRIMARYKEY

## What about [MongoDB](http://www.mongodb.org/)?

MongoDB [uses S2 for its spherical indexes](http://blog.mongodb.org/post/50984169045/new-geo-features-in-mongodb-2-4).

Where [mongo turns cells into query parameters](https://github.com/mongodb/mongo/blob/f5ed485c97b08490f59234bc1ddef2c80c2c88b9/src/mongo/db/index/expression_index.h#L42-161).

## What kinds of queries are supported?

* intersects
* contains

These query types should be roughly equivalent to the PostGIS [ST_Within](http://postgis.refractions.net/documentation/manual-1.4/ST_Within.html)
and [ST_Intersects](http://postgis.org/docs/ST_Intersects.html) queries.
