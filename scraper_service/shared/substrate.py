import os
from substrateinterface import SubstrateInterface
import threading

thread_local = threading.local()


def get_substrate_client():
    if not hasattr(thread_local, "client"):
        url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
        thread_local.client = SubstrateInterface(url)
    return thread_local.client
