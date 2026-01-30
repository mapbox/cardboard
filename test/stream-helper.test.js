var test = require('tape');
var Dyno = require('@mapbox/dyno');
var streamHelper = require('../lib/stream-helper');

test('handlers removes', function(assert) {
    var e = toEvent('REMOVE', [{id:'test'}]);
    streamHelper(['INSERT', 'MODIFY', 'REMOVE'], function(records, cb) {
        assert.equal(1, records.length, 'did not filter out records we want');     
        assert.equal('REMOVE', records[0].action, 'action type comes throw');
        assert.equal(typeof records[0].before, 'object', 'has before object');
        setTimeout(cb, 0);
    })(e, function() {
        assert.end();
    });
});

test('handlers modifies', function(assert) {
    var e = toEvent('MODIFY', [{id:'test'}]);
    streamHelper(['INSERT', 'MODIFY', 'REMOVE'], function(records, cb) {
        assert.equal(1, records.length, 'did not filter out records we want');     
        assert.equal(typeof records[0].after, 'object', 'has after object');
        assert.equal(typeof records[0].before, 'object', 'has before object');
        setTimeout(cb, 0);
    })(e, function() {
        assert.end();
    });
});

test('handlers inserts', function(assert) {
    var e = toEvent('INSERT', [{id:'test'}]);
    streamHelper(['INSERT', 'MODIFY', 'REMOVE'], function(records, cb) {
        assert.equal(typeof records[0].after, 'object', 'has after object');
        assert.equal(1, records.length, 'did not filter out records we want');     
        setTimeout(cb, 0);
    })(e, function() {
        assert.end();
    });
});

test('filter out some events', function(assert) {
    var e = toEvent('INSERT', [{id:'insert'}]);
    e.Records = e.Records.concat(toEvent('MODIFY', [{id:'modify'}]).Records);
    e.Records = e.Records.concat(toEvent('REMOVE', [{id:'remove'}]).Records);
    streamHelper(['REMOVE'], function(records, cb) {
        assert.equal(1, records.length, 'did not filter out records we want');     
        setTimeout(cb, 0);
    })(e, function() {
        assert.end();
    });
});


test('removes actions we dont want', function(assert) {
    var e = toEvent('INSERT', [{id:'test'}]);
    streamHelper(['MODIFY', 'REMOVE'], function(records, cb) {
        assert.fail('this should be skipped');
        setTimeout(cb, 0);
    })(e, function() {
        assert.pass('dont run if there are no actions we want');
        assert.end();
    });
});


function toEvent(action, records) {
    var out = records.map(function(mainRecord) {
        var serialized = JSON.parse(Dyno.serialize(mainRecord));
        var record = { eventName: action };
        record.dynamodb = {};
        record.dynamodb.OldImage = action !== 'INSERT' ? serialized : undefined;
        record.dynamodb.NewImage = action !== 'REMOVE' ? serialized : undefined;
        return record;
    });

    return {
        Records: out
    };
}
