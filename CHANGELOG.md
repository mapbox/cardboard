### v2.1.0

- Adds `cardboard.metadata.applyChanges` to apply a batch of feature adjustments to the dataset's metadata

### v2.0.0

- Adds a `GeometryCollection specific error message.

### v1.6.4

- Fixes a bug where number property values would sometimes change when encoding and decoding them.

### v1.6.3

- Fixes a bug in handling unprocessed item responses from DynamoDB.

### v1.6.2

- Fixes a bug that only updated dataset metadata if a metadata document did not already exist.

### v1.6.1

- Fixes bug where editcount was being over written when recalculating the other metadata.

### v1.6.0

- Adds editcount to metadata. This can be used to know if a derived source is from and older version of the dataset or the current one.

### v1.5.0

- Add cardboard.metadata.featureInfo(), a pre-flight function to check metadata about a feature
- Adjusts size calculations to be based on geobuf sizes instead of JSON.stringify(feature).length

### v1.4.0

- Made BBOX queries simpler (and slower) but added paging to BBOX. -- future index changes help bbox speed.

### v1.3.1

- Wrap geobuf parsing inside a try/catch.

### v1.3.0

- Features are now only stored in S3 if they don't fit in DynamoDB
- Fix feature pagination

### v1.2.2

- Switch back to https://github.com/ericelliott/cuid for id generation -- some downstream applications expect ordered ids

### v1.2.1

- Switch to https://github.com/substack/node-hat for id generation

### v1.2.0

- Calculates an ideal min/max zoom for a dataset when metadata is read

### v1.1.0

- Exposes methods for incremental updates to dataset metadata

### v1.0.2

- Improves responses in the event of partial failure during a `cardboard.batch.put` or `cardboard.batch.remove` operation

### v1.0.1

- Fixes ambiguity in handling of edge-case user-provided IDs (`null` and numeric values)
- Fixes issues caused by user-provided IDs containing an exclamation point (`!`)
