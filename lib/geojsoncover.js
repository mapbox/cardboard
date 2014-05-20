var s2 = require('s2'),
    normalize = require('geojson-normalize');

var config = {
    MAX_RING_CELLS: 100
};

var serialization = 'toToken';

module.exports.bboxQueryIndexes = function(bbox) {
    var latLngRect = new s2.S2LatLngRect(
        new s2.S2LatLng(bbox[1], bbox[0]),
        new s2.S2LatLng(bbox[3], bbox[2]));

    var cells = s2.getCover(latLngRect, {
        min: 3,
        max: 12,
        mod: 2,
        max_cells: config.MAX_RING_CELLS
    });

    return cells.map(function(cell) {
        return [cell.id().rangeMin().toToken(), cell.id().rangeMax().toToken()];
    });
};

module.exports.geometry = function(input) {
    var geom = normalize(input).features[0].geometry;
    switch (geom.type) {
        case 'Point':
            return pointIndex(geom.coordinates, 30);
        case 'Polygon':
            return polygonIndex(geom.coordinates);
        default:
            return [];
    }
};

function pointIndex(coords, level) {
    var id = new s2.S2CellId(new s2.S2LatLng(coords[1], coords[0])),
        strings = [];

    return [id.parent(level).toToken()];
}

function polygonIndex(coords, level) {
    // GeoJSON
    return s2.getCover(coords[0].slice(1).map(function(c) {
        var latLng = (new s2.S2LatLng(c[1], c[0])).normalized();
        return latLng.toPoint();
    }), {
        min: 8,
        max: 12,
        mod: 2
    }).map(function(cell) {
        return cell.id().toToken();
    });
}
