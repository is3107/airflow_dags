#!/bin/bash

if [[ -z "$VIRTUAL_ENV" ]]; then
    source /home/is3107/is3107/bin/activate 
fi

cd /home/is3107/airflow/airflow_dags/tests

pytest tests.py -v -o log_cli=true