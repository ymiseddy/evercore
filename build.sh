#/usr/bin/env bash

set -e
# cargo tarpaulin --output-dir ./.metrics/ --out Json -- --test-threads=1
# cargo build --profile=release
tokei --output json > ./.metrics/tokei.json
