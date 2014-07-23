### How are polygons indexed?

We generate an S2 cover of `S2CellId`s that covers non-point shapes with viable minimums of cells.

### How do partial overlaps work?

Not sure yet.

### How is data stored?

Currently as stringified GeoJSON

### How are points stored

As S2CellIds that cover the point on level 30.

### What are the keys like?

You need to specify a stringable primary key on the way in. The final keys look like

    cell ! (s2 cell id as token) ! (primary key)

Since this indexing scheme relies on range queries, it's unlikely primary IDs
will be used as hash keys in DynamoDB because you
can [only do a range query within a single hash key](http://0x74696d.com/posts/falling-in-and-out-of-love-with-dynamodb-part-ii/).
Instead, hash keys in DynamoDB will be more like database or layer names.

## What about [MongoDB](http://www.mongodb.org/)?

MongoDB [uses S2 for its spherical indexes](http://blog.mongodb.org/post/50984169045/new-geo-features-in-mongodb-2-4).

Where [mongo turns cells into query parameters](https://github.com/mongodb/mongo/blob/f5ed485c97b08490f59234bc1ddef2c80c2c88b9/src/mongo/db/index/expression_index.h#L42-161).

## What about Amazon DynamoDB-geo?

It only supports point queries, and is written in Java.

## What kinds of queries are supported?

* intersects
* contains

These query types should be roughly equivalent to the PostGIS [ST_Within](http://postgis.refractions.net/documentation/manual-1.4/ST_Within.html)
and [ST_Intersects](http://postgis.org/docs/ST_Intersects.html) queries.

## About DynamoDB

* [Hot hash keys](http://nate.io/dynamodb-and-hot-hash-keys/)
* [We should not](http://simondlr.com/post/26360955465/dynamodb-is-awesome-but) use [the scan operation](http://blog.coredumped.org/2012/01/amazon-dynamodb.html) for anything.

## What Levels Should you use?

* For points, as high as possible (30?)
* For rectanges, [twofishes default?](https://github.com/foursquare/twofishes/blob/master/util/src/main/scala/GeometryUtils.scala#L10-14) - 8 to 12, mod 2

## Cell IDs as strings or tokens?

* Looks like Google [uses tokens in mustang](https://github.com/mapbox/node-s2/blob/69b063dc2ef7a3e41d1d0b3079599105d29ddec6/geometry/s2cellid.cc#L168-187) (or did, mustang is retired, I think)
  but these are only accessible via CellId.
* We can't have them as numbers, because JavaScript does not tolerate 64-bit
  ints. Ints can only have 52 bits of precision in JS.
* Current approach is to use tokens

## Building Loops

* GeoJSON Polygons / rings always have a duplicate vertex at the end. S2Loop doesn't like that.

## Get or Scan?

DynamoDB treats Scan operations on tables with small keys very well:

> You can use the Query and Scan operations in DynamoDB to retrieve multiple consecutive items from a table in a single request. With these operations, DynamoDB uses the cumulative size of the processed items to calculate provisioned throughput. For example, if a Query operation retrieves 100 items that are 1 KB each, the read capacity calculation is not (100 × 4 KB) = 100 read capacity units, as if those items were retrieved individually using GetItem or BatchGetItem. Instead, the total would be only 25 read capacity units ((100 * 1024 bytes) = 100 KB, which is then divided by 4 KB).
