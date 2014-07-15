var s2 = require('s2'),
    normalize = require('geojson-normalize'),
    assert = require('assert');

var config = {};

// the maximum number of S2 cells used for any query coverage.
// - More = more complex queries
// - Fewer = less accurate queries
config.MAX_QUERY_CELLS = 100;

// The largest size of a cell permissable in a query.
config.QUERY_MIN_LEVEL = 1;

// The smallest size of a cell permissable in a query.
// - This must be >= INDEX_MAX_LEVEL
config.QUERY_MAX_LEVEL = 8;

// the maximum number of S2 cells used for any index coverage.
// - More = more accurate indexes
// - Fewer = more compact queries
config.MAX_INDEX_CELLS = 100;

// The largest size of a cell permissable in an index.
// - This must be <= QUERY_MIN_LEVEL
config.INDEX_MIN_LEVEL = 8;

// The smallest size of a cell permissable in an index.
config.INDEX_MAX_LEVEL = 12;

// The index level for point features only.
config.INDEX_POINT_LEVEL = 15;

assert.ok(config.QUERY_MAX_LEVEL >= config.INDEX_MIN_LEVEL,
    'query level and index level must correspond');

var serialization = 'toToken';

module.exports.bboxQueryIndexes = function(bbox) {
    var latLngRect = new s2.S2LatLngRect(
        new s2.S2LatLng(bbox[1], bbox[0]),
        new s2.S2LatLng(bbox[3], bbox[2]));

    return s2.getCover(latLngRect, {
        min: config.QUERY_MIN_LEVEL,
        max: config.QUERY_MAX_LEVEL,
        max_cells: config.MAX_INDEX_CELLS
    }).map(function(cell) {
        return [
            cell.id().rangeMin().toToken(),
            cell.id().rangeMax().toToken()
        ];
    });
};

module.exports.geometry = function(input) {
    var geom = normalize(input).features[0].geometry;
    switch (geom.type) {
        case 'Point':
            return pointIndex(geom.coordinates);
        case 'Polygon':
            return polygonIndex(geom.coordinates);
        default:
            return [];
    }
};

function pointIndex(coords) {
    var id = new s2.S2CellId(new s2.S2LatLng(coords[1], coords[0]));
    return [id.parent(config.INDEX_POINT_LEVEL).toToken()];
}

function polygonIndex(coords, level) {
    // GeoJSON
    return s2.getCover(coords[0].slice(1).map(function(c) {
        var latLng = (new s2.S2LatLng(c[1], c[0])).normalized();
        return latLng.toPoint();
    }), {
        min: config.INDEX_MIN_LEVEL,
        max: config.INDEX_MAX_LEVEL,
        max_cells: config.MAX_INDEX_CELLS
    }).map(function(cell) {
        return cell.id().toToken();
    });
}
