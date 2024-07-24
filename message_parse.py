#!/usr/bin/env python3

##############################################################################################
# fileName: message_parse.py
# desc:     消息解析
#           解析队列中的信息
#
#
#
#
# updateLog:
# 2024-07-23  kc  新建
##############################################################################################

import datetime, time
import soda_api as soda


#####################################################################################################
#   公共函数
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


def nowTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def getAttr(item, attr, defaultValue=None):
    return item[attr] if attr in item else defaultValue


#####################################################################################################
#   业务函数



#####################################################################################################
#   解析函数
def inventoryStaticticsMain(conn, data_info):
    return_result = 2
    for item in data_info:
        try:
            if return_result == 2 or return_result == 0:
                return_result = 0
        except Exception as e:
            print(e)
            return_result = 1
    return return_result


#####################################################################################################
#   通用入口
def process(taskName, params):
    conn = soda.getJuiceConn()
    if "inventoryStaticticsMain" == taskName:
        i = inventoryStaticticsMain(conn, params)
        if conn: conn.close()
    else:
        raise Exception(f'not supported task: {taskName}')

    return i
