#!/usr/bin/env python3

##############################################################################################
# fileName: soda_api.py
# desc:     soda-api封装
#
#
# updateLog:
# 2024-07-24 kc  新建
##############################################################################################

import json, requests, datetime, time, base64, hmac, hashlib, os, urllib, yaml

globalConf = None
juiceConn = None


#####################################################################################################
#   公共函数
#   autoapi(apiCode, params, method='GET', debug=False): 请求autoapi接口
#   message(msgCode, content, debug=False): 发送钉钉消息


def loadConfigs():
    """
    获取配置信息
    """
    global globalConf
    if not globalConf:
        cfg = loadLocalConfigs()['nacos']
        (server_addr, namespace, data_id) = (cfg['server'], cfg['namespace'], cfg['data_id'])
        globalConf = loadNacosConfig(server_addr, namespace, data_id)
        if globalConf: return globalConf
        globalConf = loadLocalConfigs()
        if globalConf: return globalConf
        raise Exception(f'Failed to load config!')  # 配置异常
    else:
        return globalConf


def loadLocalConfigs():
    """
    获取配置, $TRACKING_HOME/config.yaml
    """
    # 载入配置及脚本目录
    TRACKING_HOME = os.environ.get('TRACKING_HOME')
    TRACKING_HOME = TRACKING_HOME if TRACKING_HOME else os.path.dirname(os.path.realpath(__file__))
    config_file = f'{TRACKING_HOME}/config.yaml'
    cfg = None
    try:
        with open(file=config_file, mode='r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
            print(f"loaded config from local file.")
            return cfg
    except FileNotFoundError:
        print(f"Failed to load config from local file.")
    return None


def loadNacosConfig(server_addr, namespace, data_id, group='DEFAULT_GROUP'):
    try:
        url = f'http://{server_addr}/nacos/v1/cs/configs?dataId={data_id}&group={group}&tenant={namespace}'
        response = requests.get(url, headers={})
        if response.status_code == 200:
            cfg = yaml.safe_load(response.text)
            print(f"loaded config from Nacos.")
            return cfg
        else:
            print(f"Failed to load config from Nacos. Status code: {response.status_code}")
            return None
    except:
        print(f"Failed to load config from Nacos.")
    return None


def nowTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def timediff(date1, date2):
    d1 = datetime.datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.datetime.strptime(date2, "%Y-%m-%d %H:%M:%S")
    duration = d1 - d2
    return int(duration.total_seconds())


def getAttr(item, attr, defaultValue=None):
    return item[attr] if attr in item else defaultValue


def getSign(appId, appKey, timestamp, message):
    # 生成签名
    data64 = base64.b64encode(message.encode('UTF8')).decode('UTF8')
    rawCode = f"{appId}:{data64}:{timestamp}"
    keyBytes = appKey.encode('UTF8')
    codeBytes = rawCode.encode('UTF8')
    sign = base64.b64encode(hmac.new(keyBytes, codeBytes, digestmod=hashlib.sha256).digest()).decode('UTF8')
    return sign


def makeHeader(appId, appKey, timestamp, message):
    # 构造header
    sign = getSign(appId, appKey, timestamp, message)
    headers = {'Content-Type': 'application/json', 'appId': appId, 'ts': str(timestamp), 'sign': sign}
    return headers


def getDbConn(dbName):
    # 数据库连接
    import pymysql
    cfg = loadConfigs()[dbName]
    conn = pymysql.connect(host=cfg['host'], port=cfg['port'], user=cfg['user'], password=cfg['password'],
                           database=cfg['database'])
    return conn


def getJuiceConn():
    # juice数据库连接
    global juiceConn
    if not juiceConn:
        juiceConn = getDbConn(dbName='juice')
    if juiceConn:
        return juiceConn
    else:
        raise Exception(f'conn juicedb fail!')


def queryFromDb(conn, sql, params=None):
    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    header = [c[0] for c in cursor.description]
    datas = cursor.fetchall()
    cursor.close()
    result = []
    for row in datas:
        item = {}
        for idx in range(len(header)): item[header[idx]] = row[idx]
        result.append(item)
    return result


def executeForDb(conn, sql, params=None):
    cursor = conn.cursor()
    if params:
        affectedRows = cursor.execute(sql, params)
    else:
        affectedRows = cursor.execute(sql)
    conn.commit()
    return affectedRows


#####################################################################################################
#   API封装
#   sodaApi(apiPath, params, method='GET', debug=False): soda-api
#   autoapi(apiCode, params, method='GET', debug=False): soda-api 的autoapi模块
#   message(msgCode, content, debug=False): soda-api 的 钉钉消息模块
#   isSingleDataKey(dataService, dataKey, uuid=None): 数据是否已处理，用于去重，避免已处理的对象重复处理


def sodaApi(apiPath, params, method='GET', debug=False):
    # soda-api 的 autoapi
    sodaConf = loadConfigs()['soda-api']
    (appId, appKey, sodaHost) = (sodaConf['appId'], sodaConf['appKey'], sodaConf['host'])
    apiUrl = f'{sodaHost}{apiPath}'
    timestamp = int(time.time())
    if 'GET' == method:
        # get 方法
        queryStr = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        if debug: print(f'{nowTime()} queryStr: {queryStr}')
        headers = makeHeader(appId, appKey, timestamp, queryStr)
        if debug: print(f'{nowTime()} headers: {headers}')
        response = requests.request('GET', apiUrl, headers=headers, params=queryStr)
    else:
        # post方法
        payload = json.dumps(params)
        if debug: print(f'{nowTime()}, payload: {payload}')
        headers = makeHeader(appId, appKey, timestamp, payload)
        if debug: print(f'{nowTime()} headers: {headers}')
        response = requests.request('POST', apiUrl, headers=headers, json=params)
    if debug: print(f'{nowTime()} RESPONSE: ', response.ok, response.text)
    if response.ok:
        return json.loads(response.text)
    else:
        # api请求异常
        raise Exception(f'Request error: {response.status_code}, {response.text}')


def autoapi(apiCode, params, method='GET', debug=False):
    """ soda-api 的 autoapi

    示例:
    import soda_api as soda
    apiCode = 'hello'
    params = {'name': 'kc'}
    result = soda.autoapi(apiCode, params, method='GET')
    print(f'autoapi-hello: {result}')

    """
    apiPath = f'/api/autoapi/{apiCode}'
    return sodaApi(apiPath=apiPath, params=params, method=method.upper(), debug=debug)


def message(msgCode, content, imageUrl=None, debug=False):
    """ 发送钉钉消息
    示例:
    import soda_api as soda
    msgCode = "test"
    content = 'soda-api 消息测试'
    result = message(msgCode, content, debug=True)
    """
    apiPath = f'/api/msg/ding/send'
    params = {'classCode': msgCode, 'message': content}
    if imageUrl: params['image'] = imageUrl
    return sodaApi(apiPath=apiPath, params=params, method='POST', debug=debug)


#####################################################################################################
# 测试入口

if __name__ == "__main__":
    """ 测试：
        1、autoapi() : 测试验签和hello api的有效性
        2、message() : 测试钉钉消息
    """
    # 1. autoapi() : 测试验签和hello api的有效性
    apiCode = 'hello'
    params = {'name': 'tony'}
    result = autoapi(apiCode, params, method='GET', debug=True)
    print(f'autoapi-hello: {result}')
    if result and result['code'] == 200:
        print(f'\nautoapi-hello: PASSED\n\n')
    else:
        raise Exception(f"autoapi-hello: FAILD. {result['message']}")

    # 2.message() : 测试钉钉消息
    msgCode = "test"
    content = 'soda-api 消息测试'
    result = message(msgCode, content, debug=True)
    print(f'dingtalk-test: {result}')
    if result and result['code'] == 200:
        print(f'\ndingtalk-test: PASSED\n\n')
    else:
        raise Exception(f"dingtalk-test: FAILD. {result['message']}")

    exit(result)
