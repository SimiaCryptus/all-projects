#!/bin/bash
for f in `./modules.sh`; do pushd $f;git $@;popd; done;