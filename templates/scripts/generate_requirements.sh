#!/bin/bash

if [ -d $1/tmp/$2/ ]
then
    echo "Directory exists, proceed on"
else
    echo "Creating temporary directory"
    mkdir -p $1/tmp/$2/
fi

pipreqs $1 --savepath $1/tmp/$2/requirements_tmp.txt --force

if cmp --silent -- $1/src/requirements.txt $1/tmp/$2/requirements_tmp.txt; then
    echo "Files are identical"
    echo "Keeping current file"

else
    if [ -f $1/src/requirements.txt ]; then
        echo "Files are different"
        echo "Replacing current file with new file"
        rm $1/src/requirements.txt
    fi
    cp $1/tmp/$2/requirements_tmp.txt $1/src/requirements.txt
fi