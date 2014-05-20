var s2 = require('s2');

var config = {
    MAX_RING_CELLS: 100
};

module.exports = function(geom) {
    switch (geom.type) {
        case 'Point':
            return pointIndex(geom.coordinates, 28, 30);
        case 'Polygon':
            return polygonIndex(geom);
        case 'MultiPolygon':
            return multiPolygonIndex(geom);
        default:
            return [];
    }
};

function polygonIndex(geom) {
    var indexes = ringCells(geom.coordinates);
    return indexes;
}

function multiPolygonIndex(geom) {
    var indexes = [];
    for (i = 0; i < geom.coordinates.length; i++) {
        indexes = indexes.concat(ringCells(geom.coordinates[i]));
    }
    return indexes;
}

function ringCells(ring) {
    var indexes = [];
    for (i = 0; i < ring.length; i++) {
        indexes = indexes.concat(getCover(ring[i], {
            min: 8,
            max: 12,
            mod: 2,
            max_cells: config.MAX_RING_CELLS
        }));
    }
    return indexes;
}

function getCover(coords, options) {
    var cover = s2.getCover(coords.map(function(c) {
        return new s2.S2LatLng(c[1], c[0]);
    }), options || {});
    var out = [];
    for (var i = 0; i < cover.length; i++) {
        out.push(cover[i].id().toString());
    }
    return out;
}
function pointIndex(coords, level) {
    var id = new s2.S2CellId(new s2.S2LatLng(coords[1], coords[0])),
        strings = [];

    return [id.parent(level).id().toString()];
}
