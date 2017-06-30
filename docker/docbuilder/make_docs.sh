PWD=$(pwd)
CONTAINER_NAME=faust-docbuilder
BUILD_IMAGE_NAME=faust-docbuilder
DOCKER_BUILDDIR=/tmp/_build
HTML_TARGET=Documentation/

docker run --name $CONTAINER_NAME -v "$PWD:/faust" $BUILD_IMAGE_NAME \
    make html BUILDDIR=$DOCKER_BUILDDIR
RESULT=$?

# Copy out html and cleanup.
docker cp "$CONTAINER_NAME:$DOCKER_BUILDDIR/html" $HTML_TARGET
echo "HTML pages copied out of container to $HTML_TARGET"
docker rm $CONTAINER_NAME > /dev/null

# Exit with the exit code of the doc build run.
exit $RESULT
