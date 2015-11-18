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
