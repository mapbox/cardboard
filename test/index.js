var test = require('tap').test,
    Cardboard = require('../');

test('Cardboard', function(t) {
    var cb = new Cardboard('test.db');
    t.end();
});
