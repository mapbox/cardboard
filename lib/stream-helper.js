var unmarshall = require('@aws-sdk/util-dynamodb').unmarshall;

/**
 * Sets up a stream handler by filtering out actions that don't target the desired actions
 * converting the features into plain JS records
 * and passing them onto a user provided recordHandler if there are features
 * that match all of the above filters
 */
module.exports = function streamHelper(allowedActions, recordHandler) {
    return function(event, callback) {
        var records = event.Records.map(function(record) {
            var change = {};
            change.before = record.dynamodb.OldImage ?
                unmarshall(record.dynamodb.OldImage) : undefined;
            change.after = record.dynamodb.NewImage ?
                unmarshall(record.dynamodb.NewImage) : undefined;
            change.action = record.eventName;
            return change;
        }).filter(function(change) {
            return allowedActions.indexOf(change.action) !== -1;
        });

        if (records.length === 0) return setTimeout(callback, 0);

        recordHandler(records, callback);
    };
};


