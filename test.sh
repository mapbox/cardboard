rm -rf foo.db
./cardboard foo.db < countries.geojson
./cardboard foo.db --dumpGeoJSON > patches.geojson
# ./cardboard foo.db --query="[90,90]"
