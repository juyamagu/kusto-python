#!/usr/bin/env python3

import re
from azure.kusto.data import (
    KustoClient,
    KustoConnectionStringBuilder,
    ClientRequestProperties,
)
from azure.kusto.data.helpers import dataframe_from_result_table


def get_cluster(query: str) -> str:
    """Get cluster name from query

    Args:
        query (str): Kusto query with cluster specified.

    Returns:
        str: cluster name (without kusto.windows.net) or empty string if not found.
    """
    m = re.search(r"cluster\([\'\"]?([a-zA-Z0-9\.]+)[\'\"]?\)", query)
    if m is None:
        return ""
    cluster = m.group(1)
    # normalize cluster name
    if cluster.endswith(".kusto.windows.net"):
        cluster = cluster[:-18]
    return cluster


def get_database(query: str) -> str:
    """Get database name from query

    Args:
        query (str): Kusto query with database specified

    Returns:
        str: Database name or empty string if not found.
    """
    m = re.search(r"database\([\'\"]?([a-zA-Z0-9_]+)[\'\"]?\)", query)
    if m is None:
        return ""
    database = m.group(1)
    return database


def rebuild_client_for(cluster: str, client: KustoClient) -> KustoClient:
    """Rebuild a new client for the given cluster

    Args:
        cluster (str): cluster name
        client (KustoClient): kusto client which already has been authorized

    Returns:
        KustoClient: A new client.
    """
    if cluster.endswith(".kusto.windows.net"):
        connection_string = cluster
    else:
        connection_string = f"https://{cluster}.kusto.windows.net"
    token = client._auth_provider.acquire_authorization_header()[7:]
    kcsb = KustoConnectionStringBuilder.with_token_provider(
        connection_string=connection_string,
        token_provider=lambda: token,
    )
    return KustoClient(kcsb)


query1 = """cluster('help').database('Samples').Covid19
| where Country == "Japan"
| where isempty(State)
| order by Timestamp asc
| project Timestamp=format_datetime(Timestamp, "yyyy-MM-dd"), Confirmed, Deaths, Recovered, Active
"""

query2 = """cluster('help').database('Samples').ConferenceSessions
| limit 10
"""

if __name__ == "__main__":
    print("Execute the following query")
    print("-" * 20)
    print(query1)
    print("-" * 20)

    # Build a Kusto client via device auth
    cluster, db = get_cluster(query1), get_database(query1)
    kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(
        connection_string=f"https://{cluster}.kusto.windows.net"
    )
    client = KustoClient(kcsb)

    # Execute query
    response = client.execute(db, query1)
    df = dataframe_from_result_table(response.primary_results[0])
    dataframe_from_result_table(response.primary_results[0]).to_csv(
        "./result/query1.csv", index=False
    )

    print("Execute the following query")
    print("-" * 20)
    print(query2)
    print("-" * 20)

    # Re-build a new client from the existing one.
    cluster, db = get_cluster(query2), get_database(query2)
    client = rebuild_client_for(cluster, client)

    # Execute query
    response = client.execute(db, query2)
    dataframe_from_result_table(response.primary_results[0]).to_csv(
        "./result/query2.csv", index=False
    )