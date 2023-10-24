#! /bin/bash
docker pull at-docker:5000/datascience-notebook:cuda11.8-python3.10-torch2
docker compose down
docker build --no-cache .
docker compose up -d
