from shared.substrate import get_substrate_client
# from shared.clickhouse.batch_insert import buffer_insert
# from shared.clickhouse.utils import get_clickhouse_client, table_exists


def get_block_metadata(n):
    """
    Gets block metadata (timestamp, blockhash) for the latest block.

    TODO: In the future it will be adjusted to first check an in-memory cache, then Clickhouse, then Substrate.
    """

    # # Init the table if it doesn't exist yet
    # if not table_exists("block_metadata"):
    #     query = """
    #     CREATE TABLE IF NOT EXISTS block_metadata (
    #         block_number UInt64,
    #         timestamp DateTime,
    #         block_hash String
    #     ) ENGINE = ReplacingMergeTree()
    #     ORDER BY (block_number, timestamp, block_hash)
    #     """
    #     get_clickhouse_client().execute(query)
    #
    # # Check Clickhouse for metadata
    # query = f"""
    #     SELECT timestamp, block_hash
    #     FROM block_metadata
    #     WHERE block_number = {n}
    # """
    # res = get_clickhouse_client().execute(query)
    #
    # if res:
    #     block_timestamp, block_hash = res[0]
    #     return (block_timestamp.timestamp() * 1000, block_hash)

    substrate = get_substrate_client()
    block_hash = substrate.get_block_hash(n)
    block_timestamp = int(
        substrate.query(
            "Timestamp",
            "Now",
            block_hash=block_hash,
        ).serialize()
        / 1000
    )
    # buffer_insert("block_metadata", [n, block_timestamp, f"'{block_hash}'"])

    return (block_timestamp, block_hash)
