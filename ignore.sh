#!/bin/bash
for f in `./modules.sh`; do
	pushd $f;
	echo $1 >> .gitignore;
	cp .gitignore .gitignore.bak;
	cat .gitignore.bak | sort | uniq > .gitignore;
	rm .gitignore.bak;
	popd; 
done;