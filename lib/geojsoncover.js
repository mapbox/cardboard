var s2 = require('s2'),
    normalize = require('geojson-normalize'),
    constants = require('./constants');

var serialization = 'toToken';

module.exports.bboxQueryIndexes = function(bbox) {

    var latLngRect = new s2.S2LatLngRect(
        new s2.S2LatLng(bbox[1], bbox[0]),
        new s2.S2LatLng(bbox[3], bbox[2]));

    var cover_options = {
        min: constants.QUERY_MIN_LEVEL,
        max: constants.QUERY_MAX_LEVEL,
        max_cells: constants.MAX_INDEX_CELLS
    };

    return s2.getCover(latLngRect, cover_options).map(function(cell) {
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
    return [id.parent(constants.INDEX_POINT_LEVEL).toToken()];
}

function polygonIndex(coords, level) {
    var cover_options = {
        min: constants.INDEX_MIN_LEVEL,
        max: constants.INDEX_MAX_LEVEL,
        max_cells: constants.MAX_INDEX_CELLS
    };

    // GeoJSON
    return s2.getCover(coords[0].slice(1).map(function(c) {
        var latLng = (new s2.S2LatLng(c[1], c[0])).normalized();
        return latLng.toPoint();
    }), cover_options).map(function(cell) {
        return cell.id().toToken();
    });
}
