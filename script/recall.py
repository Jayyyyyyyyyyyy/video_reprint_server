#coding:utf-8
import requests
import json
import sys

inputfile = sys.argv[1]
outputfile = sys.argv[2]
ip_num = int(sys.argv[3])

ip = ['10.10.5.41', '10.19.67.198', '10.19.75.189', '10.42.16.15'][ip_num]

import requests
import json
def dup(jsonbody):
    url = "http://{}:9003/polls/dup_detector".format(ip)
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

def getuname(vids):
    vids = [vids]
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}}, "_source": ['vid', 'title', 'uname', 'uid', 'content_teacher', 'content_raw', 'content_mp3', 'createtime', 'content_teach', 'talentstar', 'ctype']}
        payload = json.dumps(payload)
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        response = requests.request("POST", url, data=payload, headers=headers)
        json1 = response.json()['hits']['hits']
        for x in json1:
            # try:
            res = x['_source']
            content_teacher = res['content_teacher']
            content_mp3 = res['content_mp3']
            content_teach = res['content_teach']
            content_raw = res['content_raw']
            if content_mp3 !=[]:
                res['content_mp3_tagname'] = content_mp3['tagname']
            else:
                res['content_mp3_tagname'] = ''

            if content_teach !=[]:
                res['content_teach_tagname'] = content_teach['tagname']
            else:
                res['content_teach_tagname'] = ''

            if content_teacher !=[]:
                res['content_teacher_tagname'] = content_teacher['tagname']
                res['content_teacher_tagid'] = content_teacher['tagid']
                res['content_teacher_tagvalue'] = content_teacher['tagvalue']
            else:
                res['content_teacher_tagname'] = ''
                res['content_teacher_tagid'] = ''
                res['content_teacher_tagvalue'] = ''
            if content_raw !=[]:
                res['content_raw_tagname'] = content_raw['tagname']
            else:
                res['content_raw_tagname'] = ''

            del res['content_teacher']
            del res['content_teach']
            del res['content_mp3']
            del res['content_raw']
            return res
            # except:
            #     return None
    return None
import time

with open(inputfile, 'r') as f,  open(outputfile, 'w') as f2:
    for line in f:
        vids = int(line.strip())
        parms = getuname(vids)
        print(parms)
        res = dup(parms)
        print(res)
        f2.write(res+'\n')

# with open('/Users/jiangcx/Documents/vids', 'r', encoding='utf-8') as f, open('./reprint_0308', 'w') as f2:
#     for line in f:
#         line = line.split('Deep Response:')[1]
#         vid = json.loads(line)['vid']
#         print(vid)
#         parms = getuname(vid)
#         print(parms)
#         res = dup(parms)
#         print(res)


import json
with open('update_vids', 'r') as f, open('tmpfile', 'w') as f2:
    for line in f:
        vid = int(line)
        tmp = {}
        tmp['profileinfo'] = {}
        tmp['profileinfo']['video_reprint_flag'] = 0
        tmp ['id'] = vid
        myres = json.dumps(tmp)
        f2.write(myres+'\n')
