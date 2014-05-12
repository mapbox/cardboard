rm -f food.db
./cardboard foo.db < countries.geojson
./cardboard foo.db --dump
./cardboard foo.db --query="[10,0]"
