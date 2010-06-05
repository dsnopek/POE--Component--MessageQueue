#!/bin/bash

EXPFILE=/tmp/pocomq-fastexport

bzr fast-export --plain . $EXPFILE && \
(cd ../git.mainline && git fast-import < $EXPFILE && git push)
rm -f $EXPFILE

