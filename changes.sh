#!/bin/bash

# Read the initial commit ID from the file
initial_commit=$(cat initial-public-commit.txt)

# recreate an empty file with a header
echo -e '# Changelog\n\n## Changes\n' > CHANGES.md

# Collect all commit messages since the initial commit, excluding "Change-Id" and commit ID lines
git log --pretty=format:"- %s%n%b%n" $initial_commit..HEAD | grep -v -e "Change-Id" >> CHANGES.md
