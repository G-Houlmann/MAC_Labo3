#!/usr/bin/env bash
#
# run-jupyter-docker.sh - Run Jupyter almond container in Docker
PORT=8888

docker run \
       -p $PORT:8888 \
       --rm \
       -v "/$PWD/notebooks:/home/jovyan/notebooks" \
       -e JUPYTER_ENABLE_LAB=yes \
       -v "/$PWD/data:/home/jovyan/data" \
       --name almondsh \
       thrudhvangr/jupyter-almond-preloaded
