#!/bin/bash
for f in $(./modules.sh | sort | tail -n +2); do
  pushd $f
  git checkout "$1"
  popd
done
