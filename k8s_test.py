from dask_kubernetes import HelmCluster

cluster = HelmCluster(release_name="my-dask", namespace="dask")
cluster.adapt(minimum=1, maximum=10)

client = cluster.get_client()
print(client)
