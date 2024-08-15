# Subtensor Indexer

NOTE: This project is a work in progress.

Indexer for the Subtensor blockchain.

Archive nodes are notoriously slow and expensive to query. The aim of this project is to allow indexing of key data in a DB with extremely fast query and analysis capabilities.

[Clickhouse](https://clickhouse.com/docs/en/intro) was chosen as the database because it is fast and efficient at storing and retrieving large amounts of time series data.

## Overview

At the root of this project is a `docker-compose.yml` file which is responsible for setting up the Clickhouse database and an array of "shovels".

A shovel is a program responsible for scraping a particular class of data and indexing it into Clickhouse.

Shovels are written in Python, and share common functions found under `scraper_service/shared`.

All a shoves needs to define is a function to execute for every block number. Shared logic such as connecting to the Clickhouse DB, walking historical blocks, and keeping up with new finalized blocks is handled under the hood.

## Shovels

- [x] Timestamp Shovel
- [x] Events Shovel
- [x] Extrinsics Shovel
- [x] Hotkey Owner Map Shovel
- [ ] Account Stake Map Shovel
- [ ] Account Balances Map Shovel
- [ ] Aggregate Stake Events Shovel
- [ ] Aggregate Balance Events Shovel
- [ ] Metagraph Shovel

### Implementors Guide

Want to add a new shovel? READ THIS!

1. Create a new Python program with a Dockerfile, see `scraper_service/shovel_events` for a reference
2. Create a new class that inherits the `ShovelBaseClass`, implement the `process_block` method, and call `.start` on it with a unique shovel identifier

```python
class EventsShovel(ShovelBaseClass):
    def process_block(self, n):
        do_process_block(n)


def main():
    EventsShovel(name="events").start()

def do_process_block(n):
    # Put all your block processing logic here.

    # e.g.
    # 1. Scrape data from blockchain
    # 2. Transform it
    # 3. Call `buffer_insert` to get it into Clickhouse

if __name__ == "__main__":
    main()
```

`ShovelBaseClass` contains all the logic for ensuring your `process_block` method is called for every block since genesis, and checkpointing progress so it is not lost when the shovel restarts.

3. Add your new shovel to the `docker-compose.yml`
4. That's it!

### Interacting with Substrate

- Inside your shovel, `import from shared.substrate import get_substrate_client` then call `get_substrate_client()` whenever your want a `SubstrateInterface` instance. It implements the singleton pattern, so is only implemented once and reused.

### Interacting with Clickhouse

- Do not manually make INSERT queries for Clickhouse. Instead, `from shared.clickhouse.batch_insert import buffer_insert` and call `buffer_insert` with the table and a list of rows you want to insert. The `ShovelBaseClass` will handle periodically flushing the buffer, which is much faster and more efficient than inserting row by row.

## TODO

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

2. Start Clickhouse and all the shovels

```
docker compose up --build
```
