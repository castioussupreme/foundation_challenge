# Outline
This project is intended to be ran locally for demo purposes. I would consider using AWS
lambdas through SST or similar if it was in the context of a startup, else it can be
containarized and put into cloud service of choice

## Database
For running locally with minimal setup sql lite was chosen - postgresSQL would be appropriate
for a production system. Postgres would benefit from using varchar type instead of TEXT for
indexing

## Improvements
Had a bit of trouble with the in memory db for SQL lite - moved on by manually checking data
through the SQL viewer in VSCode. This blocked testing for writing/reading from db.

# Running the code

## Pre-reqs
- Install poetry on your machine (https://python-poetry.org/docs/)

## Running the service
- poetry run python3 subgraph_demo/orchestrator.py

## How to run tests
- poetry install
- poetry run pytest