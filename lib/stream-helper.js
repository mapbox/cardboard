var Dyno = require('dyno');

/**
 * Sets up a stream handler by filtering out actions that don't target the desired actions
 * converting the features into Dyno records
 * and passing them onto a user provided recordHandler if there are features
 * that match all of the above filters
 */
module.exports = function streamHelper(allowedActions, recordHandler) {
    return function(event, callback) {
        var records = event.Records.map(function(record) {
            var change = {};
            change.before = record.dynamodb.OldImage ?
                Dyno.deserialize(JSON.stringify(record.dynamodb.OldImage)) : undefined;
            change.after = record.dynamodb.NewImage ?
                Dyno.deserialize(JSON.stringify(record.dynamodb.NewImage)) : undefined;
            change.action = record.eventName;
            return change;
        }).filter(function(change) {
            return allowedActions.indexOf(change.action) !== -1;
        });

        if (records.length === 0) return setTimeout(callback, 0);

        recordHandler(records, callback);
    };
};


