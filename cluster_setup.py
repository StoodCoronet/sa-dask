from dask.distributed import Client, SSHCluster
import paramiko

cluster = SSHCluster(
    ['192.168.1.104'],
    connect_options={'username': 'robye'}
)


cluster.start_workers(n=2)

paramiko_client = paramiko.SSHClient()
paramiko_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

client = Client(cluster, ssh=paramiko_client)

print(cluster)

input()

client.close()
paramiko_client.close()
cluster.close()

"""
tcp://192.168.1.102:8786
"""