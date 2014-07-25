usage:

import geojson file

    cardboard [table] < file.geojson

export as geojson featurecollection

    cardboard [table] --export > foo.geojson

dump raw structure of key,value

    cardboard [table] --dump > foo.json

query by a bbox. shorthand form is `lng,lat,lng,lat`

    cardboard [table] --query="1,2,3,4"

cardboard takes Env vars.

AWSKey -
AWSSecret -
Endpoint   - defaults to 'http://localhost4567'
Region - defaults to undefined
