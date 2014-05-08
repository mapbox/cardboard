var levelup = require('levelup'),
    s2index = require('s2-index'),
    through = require('through2'),
    geojsonStream = require('geojson-stream'),
    normalize = require('geojson-normalize');

module.exports = Cardboard;

function Cardboard(name) {
    if (!(this instanceof Cardboard)) {
        return new Cardboard(name);
    }

    this.db = levelup(name);
}

Cardboard.prototype.importGeoJSON = function(_) {
    var features = normalize(_).features,
        ws = this.db.createWriteStream();

    features.forEach(getIndexes);

    function getIndexes(feature) {
        var indexes = indexGeoJSON(feature.geometry),
            featureStr = JSON.stringify(feature);

        indexes.forEach(writeFeature);
        ws.end();

        function writeFeature(index) {
            ws.write({ key: index, value: featureStr });
        }
    }
};

Cardboard.prototype.dump = function(_) {
    return this.db.createReadStream();
};

Cardboard.prototype.export = function(_) {
    return this.db.createReadStream()
        .pipe(through({ objectMode: true }, function(data, enc, cb) {
            this.push(JSON.parse(data.value));
            cb();
        }))
        .pipe(geojsonStream.stringify());
};

function indexGeoJSON(geom) {
    if (geom.type === 'Point') {
        return s2index.point(geom.coordinates);
    }
    if (geom.type === 'Polygon') {
        return s2index.polygon(geom.coordinates[0]);
    }
    return [];
}
