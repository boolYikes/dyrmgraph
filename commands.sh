#!/bin/bash

# TODO: Parameterize commands 

source ./.env

cd $PROJ_PATH

# pack_up_project()
zip -r $ZIP_PATH ./ -x@./.zipignore

chmod 644 $ZIP_PATH

scp -i $IDENT_PATH -P $TARGET_PORT $ZIP_PATH $TARGET_HOST:$TARGET_HOST_PATH

echo "Done!"