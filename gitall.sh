#!/bin/bash
for f in `./modules.sh | sort | tail -n +2`; do
    pushd $f;
    git $@;
    popd;
done;
git add . && git commit -a -m "$1";
