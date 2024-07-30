from shared.substrate import get_substrate_client


def get_block_metadata(n):
    """
    Gets block metadata (timestamp, blockhash) for the latest block.

    TODO: In the future it will be adjusted to first check an in-memory cache, then Clickhouse, then Substrate.
    """

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

    return (block_timestamp, block_hash)
