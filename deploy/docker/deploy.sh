#!/bin/bash
cd /src || {
	echo "cd failed"
	exit
}

git config --global --add safe.directory '*'
git config --global user.name "Zhuokun Ding"
git config --global user.email "zkding@outlook.com"
cd /notebooks || {
	echo "cd failed"
	exit
}
jupyter lab --ip=0.0.0.0 --allow-root --NotebookApp.token="${JUPYTER_PASSWORD:-}" --no-browser
