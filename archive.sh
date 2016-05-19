#!/usr/bin/env bash

JEMALLOC=deps/jemalloc
DIR=$(mktemp -d /tmp/corvus.XXXXXX)

version=$(git describe --exact-match --tags 2> /dev/null)
if [ -z "$version" ]
then
    version=$(git rev-parse --short HEAD)
else
    version=${version:1}
fi

git ls-files | grep -v "$JEMALLOC\|archive.sh" > $DIR/files
(cd $JEMALLOC; git ls-files | sed "s#^#$JEMALLOC/#") >> $DIR/files
echo $JEMALLOC/configure >> $DIR/files
echo $JEMALLOC/VERSION >> $DIR/files

# tar --transform "s#^#corvus-$version/#" -cjf corvus-$version.tar.bz2 `cat $FILES`

mkdir $DIR/corvus-$version
rsync -R $(cat $DIR/files) $DIR/corvus-$version
tar -C $DIR -cjf corvus-$version.tar.bz2 corvus-$version
rm -rf $DIR
