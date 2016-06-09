module.exports = ids;

var IDS_PER_RESERVATION = 1000;

function ids(dyno) {
    var reserved = {};
    var numbers = {};

    numbers.get = function(dataset, callback) {
        if (reserved[dataset]) {
            var val = reserved[dataset].get();
            if (val) return callback(null, val);
        }

        numbers.reserve(dataset, function(err, values) {
            if (err) return callback(err);
            reserved[dataset] = new Range(values);
            callback(null, reserved[dataset].get());
        });
    };

    numbers.reserve = function(dataset, callback) {
        dyno.updateItem({
            Key: {
                dataset: dataset,
                id: 'autoid'
            },
            ExpressionAttributeNames: { '#v': 'value' },
            ExpressionAttributeValues: { ':v': IDS_PER_RESERVATION },
            UpdateExpression: 'add #v :v',
            ReturnValues: 'ALL_NEW'
        }, function(err, data) {
            if (err) return callback(err);
            var max = data.Attributes.value;
            return callback(null, [max - (IDS_PER_RESERVATION - 1), max]);
        });
    };

    return numbers;
}

function Range(values) {
    this.next = values[0];
    this.max = values[1];
}

Range.prototype.get = function() {
    if (this.next + 1 > this.max) return null;
    return ++this.next;
};
