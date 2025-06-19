#!/bin/bash

# Read the initial commit ID from the file
initial_commit=$(cat initial-public-commit.txt)

# Collect all commit messages since the initial commit, excluding "Change-Id" and commit ID lines
git log --pretty=format:"- %s%n%b%n" $initial_commit..HEAD | grep -v -e "Change-Id" > CHANGES.md

# Add a header to the changelog
sed -i '1i# Changelog\n\n## Changes\n' CHANGES.md
