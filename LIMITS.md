# DynamoDB

BatchGetItem

* 100 items
* 1MB

Each key in DynamoDB

* 64kb

* Batch queries will always consume read operations, even if the results
  are sparse (most things in them don't exist):
* Counts cost as many read operations as if you were just retrieving the items

> If you perform a read operation on an item that does not exist, DynamoDB will still consume provisioned read throughput: A strongly consistent read request consumes one read capacity unit, while an eventually consistent read request consumes 0.5 of a read capacity unit.

http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#CapacityUnitCalculations

# Why DynamoDB

* S3: way faster for lookups
* SimpleDB: limited to 10GB for tables, not on SSD
