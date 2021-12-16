# 从 10.42.178.198 /data/videoprofile_kafka/logs 里获取一天的数据
# grep Kafka_producer:topic profile_info.log.2020-07-30 > oneday_test
# 快速解析一天数据
import json
import requests


def dup(jsonbody):
    # url = "http://10.19.21.255:9005/polls/dup_detector"
    # url = "http://10.19.6.41:9003/polls/dup_detector"
    # url = "http://127.0.0.1:8000/polls/dup_detector"
    url = "http://10.10.5.41:9003/polls/dup_detector"
    url = "http://10.10.25.182:9003/polls/dup_detector"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text


with open('oneday_test', 'r', encoding='utf-8') as f, open('oneday_res', 'w', encoding='utf-8') as w:
    for line in f:
        res = line.split(']value[')[-1][:-2]
        newres = json.loads(res)
        vid = newres['videoinfo']['vid']
        title = newres['videoinfo']['title']
        uname = newres['profileinfo']['uname']
        if 'tagname' not in newres['profileinfo']['content_teacher']:
            content_teacher = ''
        else:
            content_teacher = newres['profileinfo']['content_teacher']['tagid']
        if 'tagname' not in newres['profileinfo']['content_mp3']:
            content_mp3 = ''
        else:
            content_mp3 = newres['profileinfo']['content_mp3']['tagname']
        createtime = newres['videoinfo']['createtime']

        parms = {"vid": vid, "title": title, "uname": uname, "teacher_id": content_teacher, 'mp3_name': content_mp3,
                 'createtime': createtime}
        res1 = dup(parms)
        res = json.loads(res1)
        if res['reprint']['img_reprint_flag'] == 1:
            print("*" * 50)
            print(parms)
            print(res1)
            w.write(res1+'\n')

