var assert = require('assert');

var constants = {};

// the maximum number of S2 cells used for any query coverage.
// - More = more complex queries
// - Fewer = less accurate queries
constants.MAX_QUERY_CELLS = 100;

// The largest size of a cell permissable in a query.
constants.QUERY_MIN_LEVEL = 1;

// The smallest size of a cell permissable in a query.
// - This must be >= INDEX_MAX_LEVEL
constants.QUERY_MAX_LEVEL = 8;

// the maximum number of S2 cells used for any index coverage.
// - More = more accurate indexes
// - Fewer = more compact queries
constants.MAX_INDEX_CELLS = 100;

// The largest size of a cell permissable in an index.
// - This must be <= QUERY_MIN_LEVEL
constants.INDEX_MIN_LEVEL = 8;

// The smallest size of a cell permissable in an index.
constants.INDEX_MAX_LEVEL = 12;

// The index level for point features only.
constants.INDEX_POINT_LEVEL = 15;

assert.ok(constants.QUERY_MAX_LEVEL >= constants.INDEX_MIN_LEVEL,
    'query level and index level must correspond');

module.exports = constants;
