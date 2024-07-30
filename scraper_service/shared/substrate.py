import os
from substrateinterface import SubstrateInterface
import threading

thread_local = threading.local()


def get_substrate_client():
    if not hasattr(thread_local, "client"):
        url = os.getenv("SUBSTRATE_ARCHIVE_NODE_URL")
        thread_local.client = SubstrateInterface(url)
    return thread_local.client


def reconnect_substrate():
    print("Reconnecting Substrate...")
    if hasattr(thread_local, "client"):
        del thread_local.client
    get_substrate_client()
