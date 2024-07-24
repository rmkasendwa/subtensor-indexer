# Subtensor Indexer

NOTE: This project is a work in progress and is not yet ready for production use.

Indexer for the Subtensor blockchain.

Archive nodes are notoriously slow and expensive to query. The aim of this project is to allow indexing of key information into an extremely fast DB to query and analyze data.

[Clickhouse](https://clickhouse.com/docs/en/intro) was chosen as the database because it is extremely fast and efficient at storing and retrieving large amounts of time series data.

## Overview

At the root of this project is a `docker-compose.yml` file which is responsible for setting up the Clickhouse database and an array of "shovels".

A shovel is a program responsible for scraping a particular class of data and indexing it into Clickhouse.

Shovels are written in Python, and share common functions found under `scraper_service/shared`.

All a shoves needs to define is a function to execute for every block number. Shared logic such as connecting to the Clickhouse DB, walking historical blocks, and keeping up with new finalized blocks.

## Shovels

- [x] Events
- [ ] Account Balance and Stake
- [ ] Metagraph

### Implementors Guide

Want to add a new shovel? READ THIS!

TODO

## Improvements

- [ ] Store metadata for a block_number such as the block_hash and timestamp in a table and cache locally to reduce load on the archive node and speed up the initial sync
- [ ] Establish a pattern for scraping block range chunks concurrently to speed up the initial sync
- [ ] Implement checkpointing
- [ ] Implement proper logging
- [ ] Observability and alerting

## Dependencies

- Docker

## Usage

1. Configure `.env`

```
cp .env.example .env
vi .env
```

2. Start the indexer

```
docker compose up --build
```
