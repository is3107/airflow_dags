#!/bin/bash

pipreqs $1 --savepath $1/requirements_tmp.txt --force

if cmp --silent -- $1/requirements.txt $1/requirements_tmp.txt; then
    echo "Files are identical"
    echo "Keeping current file, deleting new file"
    rm $1/requirements_tmp.txt
else
    echo "Files are different"
    echo "Replacing current file with new file"
    rm $1/requirements.txt
    mv $1/requirements_tmp.txt $1/requirements.txt
fi