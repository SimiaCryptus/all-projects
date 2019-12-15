#!/bin/bash
for f in `./modules.sh`; do 
	pushd $f;
	git filter-branch --force --index-filter "git rm --cached --ignore-unmatch -rf target/" --prune-empty --tag-name-filter cat -- master;
	popd; 
done;