#!/bin/bash

# Info: Be aware from where you are invoking this script due to relative paths.
#       For example, 'gradlew' has to be in the same directory as where you are
#       invoking this script from.
#       E.g.: ../crate>./devtools/create_tag.sh

# check if everything is committed
CLEAN=`git status -s`
if [ ! -z "$CLEAN" ]
then
   echo "Working directory not clean. Please commit all changes before tagging"
   echo "Aborting."
   exit -1
fi

echo "Fetching origin..."
git fetch origin > /dev/null

# get current branc
BRANCH=`git branch | grep "^*" | cut -d " " -f 2`
echo "Current branch is $BRANCH."

# check if BRANCH == origin/BRANCH
LOCAL_COMMIT=`git show --format="%H" $BRANCH`
ORIGIN_COMMIT=`git show --format="%H" origin/$BRANCH`

if [ "$LOCAL_COMMIT" != "$ORIGIN_COMMIT" ]
then
   echo "Local $BRANCH is not up to date. "
   echo "Aborting."
   exit -1
fi

# install locally so we can get the version
./gradlew installDist
# get the version
VERSION=`./app/build/install/crate/bin/crate -v | cut -d " " -f 2 | tr -d ','`

# check if tag to create has already been created
EXISTS=`git tag | grep $VERSION`

if [ "$VERSION" == "$EXISTS" ]
then
   echo "Revision $VERSION already tagged."
   echo "Aborting."
   exit -1
fi

# check if VERSION is in head of CHANGES.txt
REV_NOTE=`grep "[0-9/]\{10\} $VERSION" CHANGES.txt`
if [ -z "$REV_NOTE" ]
then
    echo "No notes for revision $VERSION found in CHANGES.txt"
    echo "Aborting."
    exit -1
fi

echo "Creating tag $VERSION..."
git tag -a "$VERSION" -m "Tag release for revision $VERSION"
git push --tags
echo "Done."
