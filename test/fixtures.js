var random = require('geojson-random');

module.exports.random = function() {
    return random.polygon.apply(random, arguments);
};

module.exports.USA = {
    type: 'Feature',
    geometry: {
        type: 'Polygon',
        coordinates: [
            [
                [
                    -127.265625,
                    22.26876403907398
                ],
                [
                    -127.265625,
                    51.6180165487737
                ],
                [
                    -60.8203125,
                    51.6180165487737
                ],
                [
                    -60.8203125,
                    22.26876403907398
                ],
                [
                    -127.265625,
                    22.26876403907398
                ]
            ]
        ]
    }
};

module.exports.nullIsland = {
    type: 'Feature',
    geometry: {
        type: 'Point',
        coordinates: [0, 0]
    }
};

module.exports.dc = {
    type: 'Feature',
    geometry: {
        type: 'Point',
        coordinates: [
            -77.02875137329102,
            38.93337493490118
        ]
    }
};

module.exports.haiti = {
    type: 'Feature',
    properties: {
        id: 'haitipolygonid'
    },
    geometry: {
        type: 'Polygon',
        coordinates: [
            [
                [
                    -73.388671875,
                    18.771115062337024
                ],
                [
                    -73.388671875,
                    19.80805412808859
                ],
                [
                    -72.1142578125,
                    19.80805412808859
                ],
                [
                    -72.1142578125,
                    18.771115062337024
                ],
                [
                    -73.388671875,
                    18.771115062337024
                ]
            ]
        ]
    }
};

module.exports.haitiLine = {
    type: 'Feature',
    geometry: {
        type: 'LineString',
        coordinates: [
            [
                -72.388671875,
                18.771115062337024
            ],
            [
                -72.388671875,
                18.80805412808859
            ],
            [
                -72.1142578125,
                18.80805412808859
            ],
            [
                -72.1142578125,
                18.771115062337024
            ],
            [
                -72.388671875,
                18.771115062337024
            ]
        ]
    }
};
