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
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

#stble node list
k8s_nodes_lists = []
#load node list
k8s_nodes_status = []

bot = Bot(token="6505580803:AAE-tZY0NxtkD7jKDaXGgIOi368QWlahPGM")
id = 5741333087
dp = Dispatcher(bot)
api_client = None

keyboard_reply = ReplyKeyboardMarkup(
    resize_keyboard=True, one_time_keyboard=True).add("Node_Status", "Pod_Check")

async def err():
    await bot.send_message(id, "Kubernetes is Down\nConnection Failed")

async def botsend(message: types.message):
    await bot.send_message(id, message)

async def errsend(message: types.message, reason: types.message):
    await bot.send_message(id, message + "\nReason : " + reason)

def check_nodes():
    global k8s_nodes_lists, k8s_nodes_status
    err_node = []
    
    for i in k8s_nodes_lists:
        if any(i[0] == x[0] for x in k8s_nodes_status):
            continue
        err_node.append(i[0])
        print(i[0] + "is down")
    
    if len(err_node) != 0:
        return err_node
    elif len(err_node) == 0:
        return None

@dp.message_handler(commands=['start', 'help'])
async def welcome(message: types.Message):
    # Sending a greeting message that includes the reply keyboard
    await message.reply("Hello! how are you?", reply_markup=keyboard_reply)

@dp.message_handler(commands=['update'])
async def update_nodes_list(message: types.Message):
    top = client.CustomObjectsApi()
    global k8s_nodes_lists
    k8s_nodes_lists=[]
    k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
    for stats in k8s_nodes['items']:
            node_name = stats['metadata']['name'][3:]
            node_cpu_usage = (str)((int)(stats['usage']['cpu'][:-1])//1000000)+"m"
            node_memory_usage = (int)(stats['usage']['memory'][:-2])//1000
            k8s_nodes_lists.append([node_name, node_cpu_usage, node_memory_usage])

@dp.message_handler()
async def check_rp(message: types.Message):
    top = client.CustomObjectsApi()
    global k8s_nodes_status
    k8s_nodes_status = []
    message_txt = ""
    if message.text == 'Node_Status':
        #k8s_nodes_status = "----------- Lawdians Internal K8S -----------\n"
        # Responding with a message for the first button
        k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
        for stats in k8s_nodes['items']:
            node_name = stats['metadata']['name'][3:]
            node_cpu_usage = (str)((int)(stats['usage']['cpu'][:-1])//1000000)+"m"
            node_memory_usage = (int)(stats['usage']['memory'][:-2])//1000
            k8s_nodes_status.append([node_name, node_cpu_usage, node_memory_usage])
            message_txt = message_txt + f"{node_name:<12} CPU: {node_cpu_usage:>4}   Mem: {node_memory_usage:>5}Mi\n"
        
        print(k8s_nodes_status)

        err_nodes = check_nodes()
        if err_nodes == None:
            await bot.send_message(message.from_user.id, message_txt)
        elif err_nodes != None:
            message_txt = message_txt + "\n----------------err node----------------\nn"
            for i in err_nodes:
                print(i)
                message_txt += (i + " is Down")   
            await bot.send_message(message.from_user.id, message_txt)

    elif message.text == 'Pod_Check':
        k8s_nodes_lists = ""
        #resource = api_client.list_node()
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
                k8s_nodes_lists += f"{namespace}  {pod_name}  "+ info.status.container_statuses[0].state.waiting.reason + "\n"
        await bot.send_message(message.from_user.id, running_pods_num)
# error_pod_num = "np           pod                  status\n"

def schedule_node_check():
    global k8s_nodes_status
    k8s_nodes_status=[]
    top = client.CustomObjectsApi()
    k8s_nodes = top.list_cluster_custom_object("metrics.k8s.io","v1beta1", "nodes")
    
    for stats in k8s_nodes['items']:
        node_name = stats['metadata']['name'][3:]
        node_cpu_usage = (str)((int)(stats['usage']['cpu'][:-1])//1000000)+"m"
        node_memory_usage = (int)(stats['usage']['memory'][:-2])//1000
        k8s_nodes_status.append([node_name, node_cpu_usage, node_memory_usage])
    
    err_nodes = check_nodes()
    message_txt =""
    if err_nodes != None:
        message_txt = message_txt + "\n----------------err node----------------\nn"
        for i in err_nodes:
            print(i)
            message_txt += (i + " is Down\n")
        try:
            asyncio.run(errsend("Node Down Detect",message_txt))
        except:
            print("error")
        asyncio.run(err())
    elif err_nodes == None:
        print("schedule_node_check_Report : Stable")

def conn_k8s():
    global api_client
    while True:
        try:
            print("try connect")
            config.load_kube_config(
                config_file="vm-kube-config"
                )
            api_client = client.CoreV1Api()
            resource = api_client.list_node()
            print("connect success")
            break 
        except:
            print("kubernetes Cluster Connection failed.")
            try:
                asyncio.run(err())
            except:
                print("error")
            time.sleep(300)

def main():
    conn_k8s()

    # sched = BackgroundScheduler()
    # sched.start()
    # sched.add_job(conn_k8s, 'cron', minute="*/5", id="conn_test")

    # alarm_node_status = BackgroundScheduler()
    # alarm_node_status.start()
    # alarm_node_status.add_job(schedule_node_check, 'cron', second="*/10", id="node_status_checking")

    executor.start_polling(dp)

    # try:
    #     executor.start_polling(dp)
    # except:
    #     print("errrrrrr")
    #     time.sleep(30000)
    
if __name__=="__main__":
    main()