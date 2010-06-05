#!/bin/bash

EXPFILE=/tmp/pocomq-fastexport

bzr fast-export --plain . $EXPFILE && \
(cd ../git.mainline && git fast-import < $EXPFILE && git commit -m "Importing from bzr")
rm -f $EXPFILE

