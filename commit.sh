#!/bin/bash
for f in `./modules.sh | tail -n +1`; do
    pushd $f;
    git add . && git commit -a -m "$1";
    popd;
done;
git add . && git commit -a -m "$1";
