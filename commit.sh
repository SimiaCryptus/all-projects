#!/bin/bash
for f in `./modules.sh`; do pushd $f;git add . && git commit -a -m "$1";popd; done;