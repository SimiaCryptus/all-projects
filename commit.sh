#!/bin/bash
for f in $(./modules.sh | sort | tail -n +2); do
  pushd $f
  git add . && git commit -a -m "$1"
  popd
done
git add . && git commit -a -m "$1"
