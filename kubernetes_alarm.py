from kubernetes import client, config
from kubernetes.stream import stream
import telegram
import json
import asyncio
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardMarkup
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
#asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

#stble node list
k8s_nodes_lists = []
#load node list
k8s_nodes_status = []

bot = Bot(token="6505580803:AAE-tZY0NxtkD7jKDaXGgIOi368QWlahPGM")
id = 5741333087
dp = Dispatcher(bot)
api_client = None

status_color = {
    'Green' : '🟢',
    'Red' : '🔴',
    'Yellow' : '🟡'
}

keyboard_reply = ReplyKeyboardMarkup(
    resize_keyboard=True, one_time_keyboard=True).add("Node_Status", "Pod_Check")

async def err():
    await bot.send_message(id, "Kubernetes is Down\nConnection Failed")

#@dp.message_handler()
async def botsend(message: types.message):
    await bot.send_message(id, message)

#@dp.message_handler()
async def errsend(message: types.message, reason: types.message):
    await bot.send_message(id, message + "\n" + reason)


def check_nodes():
    global k8s_nodes_lists, k8s_nodes_status
    err_node = []
    new_node = []
    print(k8s_nodes_lists)
    print(k8s_nodes_status)

    # if (k8s_nodes_lists == []):
    #     print("k8s_nodes_list is Empty")
    if (len(k8s_nodes_lists) > len(k8s_nodes_status)):
        print("Nodes Down Detecd")
        for i in k8s_nodes_lists:
            if any(i[0] == x[0] for x in k8s_nodes_status):
                continue
            err_node.append(i[0])
            print("Node : " + i[0] + " is down")
    elif (len(k8s_nodes_lists) < len(k8s_nodes_status)):
        print("New Nodes Detecd")
        for i in k8s_nodes_status:
            if any(i[0] == x[0] for x in k8s_nodes_lists):
                continue
            new_node.append(i[0])
            print("Node : " + i[0] + " is New connecting")

    if len(err_node) != 0:
        return "Node Down Detect", err_node
    elif len(new_node) != 0:
        return "New Node Detect", new_node
    else:
        return None, None


@dp.message_handler(commands=['start', 'help'])
async def welcome(message: types.Message):
    await conn_k8s()
    await bot.send_message(id, "K8S Cluster Connect! Start bot!", reply_markup=keyboard_reply)
    await update_nodes_list('update')
    schedule_node = AsyncIOScheduler()
    schedule_node.add_job(schedule_node_check, 'cron', second="*/10")
    schedule_node.start()

    schedule_pod = AsyncIOScheduler()
    schedule_pod.add_job(schedule_pod_check, 'cron', second="*/5")
    schedule_pod.start()
    #await message.reply("Hello! how are you?", reply_markup=keyboard_reply)


@dp.message_handler(commands=['update'])
async def update_nodes_list(message: types.Message):
    top = client.CustomObjectsApi()
    global k8s_nodes_lists
    k8s_nodes_lists=[]
    k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
    for stats in k8s_nodes['items']:
            node_name = stats['metadata']['name'][3:]
            node_cpu_usage = (str)((int)(stats['usage']['cpu'][:-1])//1000000)+"m"
            node_memory_usage = (str)((int)(stats['usage']['memory'][:-2])//1000)+"Mi"
            k8s_nodes_lists.append([node_name, node_cpu_usage, node_memory_usage])
    print("update success")


@dp.message_handler()
async def check_rp(message: types.Message):
    top = client.CustomObjectsApi()
    global k8s_nodes_status, k8s_nodes_lists
    k8s_nodes_status = []
    message_txt = ""
    if message.text == 'Node_Status':
        message_txt = "------------ Lawdians Internal K8S ------------\n"
        # Responding with a message for the first button
        try:
            k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
        except:
            await errsend("K8S Node status View Failed","Reason : Cluster Connect Error")

        for stats in k8s_nodes['items']:
            node_name = stats['metadata']['name'][3:]
            node_cpu_usage = (str)((int)(stats['usage']['cpu'][:-1])//1000000)+"m"
            node_memory_usage = (str)((int)(stats['usage']['memory'][:-2])//1000)+"Mi"
            k8s_nodes_status.append([node_name, node_cpu_usage, node_memory_usage])
            if any(node_name == x[0] for x in k8s_nodes_lists):
            #if any(node_name == x for x in Detect ):
                message_txt = message_txt + f"{status_color['Green']}  {node_name:<11s} CPU: {node_cpu_usage:>4s} Mem: {node_memory_usage:>7s}\n"
        
        Detect, find_nodes = check_nodes()

        if Detect == None:
            await bot.send_message(id, message_txt)
        elif Detect == "Node Down Detect":
            #message_txt = message_txt + "\n----------------err node----------------\nn"
            for i in find_nodes:
                print(i)
                message_txt = message_txt + f"{status_color['Red']}  {i:<11s} CPU:  ??m Mem:  ????Mi\n"
            await bot.send_message(id, message_txt)
        elif Detect == "New Node Detect":
            for i in find_nodes:
                print(i)
                message_txt = message_txt + f"{status_color['Yellow']}  {i:<11s} CPU:  ??m Mem:  ????Mi\n"
            await bot.send_message(id, message_txt)

    elif message.text == 'Pod_Check':
        try:
            top = client.CustomObjectsApi()
            k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
        except:
            await err()
        #message_txt = "------------ Lawdians Internal K8S ------------\n"
        message_txt = "------------- Lawdians Pods List -------------\n"
        err_pods = ""
        ret = api_client.list_pod_for_all_namespaces(watch=False)
        running_pods_num = 0
        for info in ret.items:
            running = info.status.container_statuses[0].state.running
            terminated = info.status.container_statuses[0].state.terminated
            namespace = info.metadata.namespace 
            pod_name = info.metadata.name[:12]
            if running != None:
                running_pods_num += 1
            elif running == None and terminated == None:
                err_pods += f"{namespace}  {pod_name}  "+ info.status.container_statuses[0].state.waiting.reason + "\n"
        if err_pods == "":
            await botsend(f"{message_txt}{status_color['Green']} Running Pods : {running_pods_num}")
        else:
            await botsend(f"{message_txt}{status_color['Green']} Running Pods : {running_pods_num}\n\n{status_color['Red']} Error Pods List\n{err_pods}")
        

async def schedule_node_check():
    print("schedule_node_ckeck start")
    global k8s_nodes_status
    k8s_nodes_status=[]
    try:
        top = client.CustomObjectsApi()
        k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
    except:
        await err()
    
    for stats in k8s_nodes['items']:
        node_name = stats['metadata']['name'][3:]
        node_cpu_usage = (str)((int)(stats['usage']['cpu'][:-1])//1000000)+"m"
        node_memory_usage = (str)((int)(stats['usage']['memory'][:-2])//1000)+"Mi"
        k8s_nodes_status.append([node_name, node_cpu_usage, node_memory_usage])
    
    Detect, find_nodes = check_nodes()
    message_txt =""
    if Detect == "Node Down Detect":
        for i in find_nodes:
            print(i)
            message_txt = message_txt + f"{status_color['Red']}  {i:<11s}         is UNKNOWN        \n"
        await errsend("-----------🚨 Node Down Detect 🚨-----------", message_txt)
    elif Detect == "New Node Detect":
        for i in find_nodes:
            print(i)
            message_txt = message_txt + f"{status_color['Yellow']}  {i:<11s}    is New_connecting   \n"
        await errsend("------------🚨 New Node Detect 🚨------------", message_txt)
    else:
        print("schedule_node_check_Report : Stable")


async def schedule_pod_check():
    try:
        top = client.CustomObjectsApi()
        k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
    except:
        await err()
    
    err_pod_lists = ""
    err_pod_num = 0
    #resource = api_client.list_node()
    ret = api_client.list_pod_for_all_namespaces(watch=False)
    running_pods_num = 0
    for info in ret.items:
        running = info.status.container_statuses[0].state.running
        terminated = info.status.container_statuses[0].state.terminated
        namespace = info.metadata.namespace 
        pod_name = info.metadata.name[:12]

        if running == None and terminated == None:
            err_pod_lists += f"{namespace}  {pod_name}  "+ info.status.container_statuses[0].state.waiting.reason + "\n"
            err_pod_num += 1

    if err_pod_num == 0:
        print("schedule_pod_check_Report : Stable")
    else:
        await errsend("-------------🚨 Pod Error Detect 🚨-------------", err_pod_lists)
    

async def conn_k8s():
    global api_client
    print(k8s_nodes_lists)
    while True:
        try:
            print("try connect")
            config.load_kube_config(
                config_file="view-config"
            )
            api_client = client.CoreV1Api()
            resource = api_client.list_node()
            print("connect success")
            break 
        except:
            print("kubernetes Cluster Connection failed.")
            await errsend("K8S Cluster Connect Error", "Kube-config is not Enable")
            time.sleep(10)

def main():
    conn_k8s()
    #asyncio.run(update_nodes_list('update'))
    sched = BackgroundScheduler()
    sched.start()
    sched.add_job(conn_k8s, 'cron', minute="*/5", id="conn_test")

    alarm_node_status = BackgroundScheduler()
    alarm_node_status.start()
    alarm_node_status.add_job(schedule_node_check, 'cron', second="*/5", id="node_status_checking")

    try:
        executor.start_polling(dp)
    except:
        print("Bot error")
    
if __name__=="__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(dp.start_polling())
    
    loop.run_forever()