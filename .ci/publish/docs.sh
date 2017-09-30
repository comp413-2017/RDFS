#!/usr/bin/env bash

set -ex

GIT_SHA=$(git rev-parse HEAD)
GIT_BRANCH=$(if [ "$TRAVIS_PULL_REQUEST" == "false" ]; then echo $TRAVIS_BRANCH; else echo $TRAVIS_PULL_REQUEST_BRANCH; fi)

# Only publish if building on develop.
if [ "$GIT_BRANCH" == "develop" ]; then
  # Set up CI git user
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
  # Generate docs
  doxygen > /dev/null 2>&1
  mv html /tmp/rdfs-docs
  # Push back up to Github
  git config --replace-all remote.origin.fetch +refs/heads/*:refs/remotes/origin/*
  git fetch
  git checkout gh-pages
  rm -rf *
  mv /tmp/rdfs-docs/* .
  git add .
  git commit -m "[autocommit] [docs] ${GIT_SHA} (build $TRAVIS_BUILD_NUMBER)" > /dev/null
  git remote set-url origin git@github.com:comp413-2017/RDFS.git
  git push origin HEAD
fi
