#!/bin/bash
for f in $(find . -name '.git'); do dirname $f; done
