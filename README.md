# Outline
This project is intended to be ran locally for demo purposes. I would consider using AWS
lambdas through SST or similar if it was in the context of a startup, else it can be
containarized and put into cloud service of choice.

# Running the code

## Pre-reqs
- Install poetry on your machine (https://python-poetry.org/docs/)

## Running the service
- poetry run python3 subgraph_demo/orchestrator.py

## How to run tests
- poetry install
- poetry run pytest