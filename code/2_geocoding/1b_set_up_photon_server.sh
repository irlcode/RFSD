# Download and install Photon jar from https://github.com/komoot/photon (requires Java, version 21+)
wget https://github.com/komoot/photon/releases/download/1.2.1/photon-1.2.1.jar

# Download latest Europe database (13.2 GB)
wget https://download1.graphhopper.com/public/europe/photon-dump-europe-1.0-latest.jsonl.zst

# Prepare database for photon
zstd --stdout -d photon-dump-europe-1.0-latest.jsonl.zst | java -jar photon-1.2.1.jar import -languages en -country-codes ru,ua -import-file -

# Run photon server (it will be available at http://localhost:2322)
java -jar photon-1.2.1.jar serve
