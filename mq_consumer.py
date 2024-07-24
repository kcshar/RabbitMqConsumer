#!/usr/bin/env python3

##############################################################################################
# fileName: mq_consumer.py
# desc:     MQ消费者
#           消费队列中的消息并进行对应的处理
#
#
#
#
# updateLog:
# 2024-07-23  kc  新建
##############################################################################################

import soda_api as soda
import json, datetime, argparse
import message_parse as mc


##############################################################################################
# 公共函数
def nowTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def parse_job_params(ptext):
    """
    对形如"arg1=value1 arg2=value2 ..."的参数解析成字典返回
    """
    params = {}
    if ptext and len(ptext) > 0:
        for seg in ptext.strip().split():
            segs = seg.split('=')
            if len(segs) == 2 and len(segs[0]) > 0:
                params[segs[0]] = segs[1]
    return params


##############################################################################################
# 创建连接
def getUserInfo():
    import pika
    local_cfg = soda.loadLocalConfigs()["nacos_mq"]
    server_addr = local_cfg["server"]
    namespace = local_cfg["namespace"]
    data_id = local_cfg["data_id"]
    cfg = soda.loadNacosConfig(server_addr, namespace, data_id)
    user_name = cfg["rabbitmq"]["user_name"]
    password = cfg["rabbitmq"]["password"]
    host = cfg["rabbitmq"]["host"]
    vhost = cfg["rabbitmq"]["vhost"]
    user_info = pika.PlainCredentials(user_name, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host, 5672, vhost, user_info))
    return connection


def getTestUserInfo():
    import pika
    user_info = pika.PlainCredentials('guest', 'guest')
    host = '192.168.110.101'
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=31312, credentials=user_info))
    return connection


def callbackFunc(ch, method, properties, body, task_name):
    # 接收到消息，则执行callback
    print(f"{nowTime()}: 接收到任务 {task_name}")
    result = body.decode()
    data_info = json.loads(result)['data']
    ack = mc.process(task_name, data_info)
    if ack == 0:
        print(f"{nowTime()}: 成功处理任务 {task_name}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def callbackWithArg(task_name):
    # 回调函数，补充taskName
    def callback(ch, method, properties, body):
        callbackFunc(ch, method, properties, body, task_name)
    return callback


def consumerMain(task_name, queue_name):
    # 生产环境下连接
    connection = getUserInfo()
    # # 测试环境下连接
    # connection = getTestUserInfo()
    channel = connection.channel()
    callback = callbackWithArg(task_name)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    channel.start_consuming()


#####################################################################################################
#   通用入口
def process(taskName, params):
    if "inventoryStaticticsMain" == taskName:
        consumerMain(taskName, 'data.ofc.inventory.product_list')

    else:
        raise Exception(f'not supported task: {taskName}')

    return 0


if __name__ == "__main__":
    """ 需要收集以下参数
    -task taskName
    -p 参数

    :return 0-运行成功，1-失败
    """
    # 定义参数
    parser = argparse.ArgumentParser(description='tracking procesor.')
    parser.add_argument('-task', required=True, help='taskName')
    parser.add_argument('-p', default='', help='参数')
    args = parser.parse_args()
    print(f'call for task={args.task}')

    # 解析参数
    taskName = args.task
    params = parse_job_params(args.p)

    # 执行
    result = process(taskName, params)
    print(f"result is {result}")
    exit(result)