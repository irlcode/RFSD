# Run docker image from https://github.com/mediagis/nominatim-docker/tree/master/4.4
# with Nominatim

mkdir -p /local/path/to/nominatim-data/share
mkdir -p /local/path/to/nominatim-data/postgresql
mkdir -p /local/path/to/nominatim-data/flatnode

cd /local/path/to/nominatim-data/

# Configuration example: https://github.com/mediagis/nominatim-docker/blob/master/4.4/example.md
# This way we ensure data persistency: https://github.com/mediagis/nominatim-docker/tree/master/4.4#persistent-container-data
docker run -it \
  -e PBF_URL="https://download.geofabrik.de/russia-latest.osm.pbf" \
  -e REPLICATION_URL="https://download.geofabrik.de/russia-updates/" \
  -e THREADS=10 \
  -e IMPORT_WIKIPEDIA=true \
  -e IMPORT_TIGER_ADDRESSES=false \
  -e NOMINATIM_PASSWORD \
  -e NOMINATIM_PASSWORD=12345 \
  -e IMPORT_STYLE=full \
  -v /local/path/to/nominatim-data/postgresql:/var/lib/postgresql/14/main \
  -v /local/path/to/nominatim-data/flatnode:/nominatim/flatnode \
  -v /local/path/to/nominatim-data/share:/nominatim/share \
  --shm-size=100g \
  -p 8080:8080 \
  --name nominatim \
  mediagis/nominatim:4.3
