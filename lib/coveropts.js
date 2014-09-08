var assert = require('assert');


// index at 2 levels, for large and small geometries.
var coveropts = [{},{}];


/// Big things first level of the index


// the maximum number of S2 cells used for any query coverage.
// - More = more complex queries
// - Fewer = less accurate queries
coveropts[0].max_query_cells = 20;

// The largest size of a cell permissable in a query.
coveropts[0].query_min_level = 1;

// The smallest size of a cell permissable in a query.
// - This must be >= INDEX_MAX_LEVEL
coveropts[0].query_max_level = 5;

// the maximum number of S2 cells used for any index coverage.
// - More = more accurate indexes
// - Fewer = more compact queries
coveropts[0].max_index_cells = 100;

// The largest size of a cell permissable in an index.
// - This must be <= QUERY_MIN_LEVEL
coveropts[0].index_min_level = 5;

// The smallest size of a cell permissable in an index.
coveropts[0].index_max_level = 20;

// The index level for point features only.
coveropts[0].index_point_level = 20;


/// Small things second level of the index

coveropts[1].max_query_cells = 20;

// The largest size of a cell permissable in a query.
coveropts[1].query_min_level = 1;

// The smallest size of a cell permissable in a query.
// - This must be >= INDEX_MAX_LEVEL
coveropts[1].query_max_level = 12;

// the maximum number of S2 cells used for any index coverage.
// - More = more accurate indexes
// - Fewer = more compact queries
coveropts[1].max_index_cells = 50;

// The largest size of a cell permissable in an index.
// - This must be <= QUERY_MIN_LEVEL
coveropts[1].index_min_level = 12;

// The smallest size of a cell permissable in an index.
coveropts[1].index_max_level = 20;

// The index level for point features only.
coveropts[1].index_point_level = 20;

assert.ok(coveropts[0].query_max_level >= coveropts[0].index_min_level,
    'query level and index level must correspond');

assert.ok(coveropts[1].query_max_level >= coveropts[1].index_min_level,
    'query level and index level must correspond');


module.exports = coveropts;
