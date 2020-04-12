#!/bin/bash
for f in $(find . -name '.git' | grep -v third-party); do dirname $f; done
