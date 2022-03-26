#!/bin/bash

if [[ -z "$VIRTUAL_ENV" ]]; then
  source /home/is3107/is3107/bin/activate 
fi

rm requirements/requirements.txt

pip freeze | grep --invert-match pkg_resources > requirements/requirements.txt

while [ ! -f requirements/requirements.txt ]
do
  sleep 0.1
done


git add requirements/requirements.txt