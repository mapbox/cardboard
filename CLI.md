usage:

import geojson file

    cardboard file.cb < file.geojson

export as geojson featurecollection

    cardboard file.cb --export > foo.geojson

dump raw structure of key,value

    cardboard file.cb --dump > foo.json

query by a point or polygon. shorthand form is `[lng,lat]`, which
gets exported to a Point.

    cardboard file.cb --query="[10,20]"
