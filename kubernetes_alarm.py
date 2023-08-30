from kubernetes import client, config
from kubernetes.stream import stream
import telegram
import json
import asyncio
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def Errorsend():
    await bot.sendMessage(chat_id=id, text="Kubernetes Cluster Connection failed")


k8s_nodes_text = ""
bot = telegram.Bot(token="6505580803:AAE-tZY0NxtkD7jKDaXGgIOi368QWlahPGM")
id = 5741333087

try:
    config.load_kube_config(
        config_file="kube-config"
    )
    api_client = client.CoreV1Api()
    resource = api_client.list_node()
    print("connect success")
except:
    print("kubernetes Cluster Connection failed")
    asyncio.run(Errorsend())
    exit()


resource = api_client.list_node()
ret = api_client.list_pod_for_all_namespaces(watch=False)
top = client.CustomObjectsApi()
k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")

for stats in k8s_nodes['items']:
    node_name = stats['metadata']['name'][3:]
    node_cpu_usage = (str)((int)(stats['usage']['cpu'][:-1])//1000000)+"m"
    node_memory_usage = (int)(stats['usage']['memory'][:-2])//1000
    k8s_nodes_text = k8s_nodes_text + f"{node_name:<12} CPU: {node_cpu_usage:>4}   Mem: {node_memory_usage:>5}Mi\n"

#k8s_pods = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "pods")

#for stats in k8s_pods['items']:
#    print("Pods name: %s\tNamespace: %s\tCPU: %s\tMemory: %s" % (stats['containers'][0]['name'], stats['metadata']['namespace'], stats['containers'][0]['usage']['cpu'], stats['containers'][0]['usage']["memory"]))

error_pod_num = "np           pod                  status\n"
running_pods_num = 0
for info in ret.items:
    running = info.status.container_statuses[0].state.running
    terminated = info.status.container_statuses[0].state.terminated

    namespace = info.metadata.namespace 
    pod_name = info.metadata.name[:12]
    if running != None:
        running_pods_num += 1

    elif running == None and terminated == None:
        error_pod_num += f"{namespace}  {pod_name}  "+ info.status.container_statuses[0].state.waiting.reason + "\n"
        

async def botsend():
    await bot.sendMessage(chat_id=id, text=(k8s_nodes_text + "\n" + error_pod_num))

asyncio.run(botsend())

