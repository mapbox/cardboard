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

### How are >1 items per key stored?

Not sure yet.
