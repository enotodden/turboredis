#!/bin/bash

# Based on: https://gist.github.com/brantfaircloth/791759

set -e;
set -x;

rm -rf doc/_build/html

mkdir -p _build/html

git clone git@github.com:enotodden/turboredis.git doc/_build/html

cd doc/_build/html

git symbolic-ref HEAD refs/heads/gh-pages
rm -f .git/index
git clean -fdx

make -C ../../ html

touch .nojekyll
git add .
git commit -m 'docs to gh-pages'
git push -f origin gh-pages
