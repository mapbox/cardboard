var geobuf = require('geobuf');
var Pbf = require('pbf');

/**
 * Normalize a feature decoded from geobuf to ensure consistent API output.
 * geobuf 3.x omits empty properties and preserves numeric IDs as numbers.
 * This function ensures backwards compatibility with cardboard's external API.
 * @param {object} feature - a GeoJSON feature decoded from geobuf
 * @returns {object} the normalized feature
 */
function normalizeFeature(feature) {
    if (!feature) return feature;
    if (!feature.properties) feature.properties = {};
    // geobuf 3.x preserves numeric IDs; convert to strings for backwards compatibility
    if (typeof feature.id === 'number') feature.id = String(feature.id);
    return feature;
}

/**
 * Encode a GeoJSON feature to geobuf format
 * @param {object} feature - a GeoJSON feature
 * @returns {Buffer} the encoded geobuf buffer
 */
function encodeFeature(feature) {
    return Buffer.from(geobuf.encode(feature, new Pbf()));
}

/**
 * Decode a geobuf buffer to a GeoJSON feature, with normalization for API consistency
 * @param {Buffer} buffer - a geobuf encoded buffer
 * @returns {object} the decoded and normalized GeoJSON feature
 */
function decodeFeature(buffer) {
    return normalizeFeature(geobuf.decode(new Pbf(buffer)));
}

module.exports = {
    normalizeFeature: normalizeFeature,
    encodeFeature: encodeFeature,
    decodeFeature: decodeFeature
};
