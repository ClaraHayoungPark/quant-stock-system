#!/bin/bash

export AIRFLOW_HOME=$(pwd)/.airflow

airflow scheduler &
airflow api-server --port 8080