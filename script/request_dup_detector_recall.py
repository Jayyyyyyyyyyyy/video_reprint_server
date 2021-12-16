#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import json
def dup(jsonbody):
    #url = "http://10.19.21.255:9005/polls/dup_detector"
    url = "http://10.42.16.15:9003/polls/dup_detector"
    # url = "http://10.19.67.198:9003/polls/dup_detector"
    #url = "http://10.19.75.189:9003/polls/dup_detector"
    # url = "http://10.42.42.82:9003/polls/dup_detector"
    #url = "http://127.0.0.1:8000/polls/dup_detector"
    # url = "http://10.19.6.41:9003/polls/dup_detector"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

def getuname(vids):
    vids = [vids]
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}}, "_source": ['vid', 'title', 'uname', 'uid', 'content_teacher', 'content_raw',
                                                                                 'content_mp3', 'createtime', 'content_teach', 'talentstar',
                                                                                 'ctype', 'content_mp3_seg']}
        payload = json.dumps(payload)
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        response = requests.request("POST", url, data=payload, headers=headers)
        json1 = response.json()['hits']['hits']
        for x in json1:
            res = x['_source']
            if 'content_teacher' not in res:
                content_teacher = ''
            else:
                content_teacher = res['content_teacher']
            content_mp3 = res['content_mp3']
            content_teach = res['content_teach']
            content_raw = res['content_raw']
            if len(content_mp3) !=0:
                res['content_mp3_tagname'] = content_mp3['tagname']
            else:
                res['content_mp3_tagname'] = ''

            if len(content_teach) != 0:
                res['content_teach_tagname'] = content_teach['tagname']
            else:
                res['content_teach_tagname'] = ''

            if len(content_teacher) != 0:
                res['content_teacher_tagname'] = content_teacher['tagname']
                res['content_teacher_tagid'] = content_teacher['tagid']
                res['content_teacher_tagvalue'] = content_teacher['tagvalue']
            else:
                res['content_teacher_tagname'] = ''
                res['content_teacher_tagid'] = ''
                res['content_teacher_tagvalue'] = ''
            if len(content_raw) != 0:
                res['content_raw_tagname'] = content_raw['tagname']
            else:
                res['content_raw_tagname'] = ''


            del res['content_teacher']
            del res['content_teach']
            del res['content_mp3']
            del res['content_raw']
            return res
    return None
import time
print(time.ctime())


# vids = 1500679062672 # content_teacher 找不出来 # 1500678491373
# vids = 1500678136078
# vids = 1500679391389
#1500677914852
for x in [20000001079987 ]:# , 1500679179553,1500679272856,1500679307588]:1500678516571 #芳芳老师 忘川的河
    vids = x # 原创 1500678491373
    parms = getuname(vids)
    print(parms)
    res = dup(parms)
    print(res)
    # print(time.ctime())
    # res = json.loads(res)
    # print(res)
# 1500678324622 1500678056282 1500678008988
#
# with open('./reprint_vids', 'r') as f,  open('./img_flag', 'w') as f2:
#     for line in f:
#         vids = line.strip()
#         #vids = json.loads(vids)['vid']
#         parms = getuname(vids)
#         print(parms)
#         res = dup(parms)
#         print(res)
#         f2.write(res+'\n')
#

# content_mp3_seg = [{'name': '扛', 'weight': 0.160755}, {'name': '过', 'weight': 0.07057}, {'name': '枪', 'weight': 0.121366}, {'name': '放过羊',
#                                                                                                                    'weight': 0.219888},
#                    {'name': '放过', 'weight': 0.328682}, {'name': '羊', 'weight': 0.098737}, {'name': '扛过枪放过羊', 'weight': 1.0}]
# content_mp3_seg = " ".join(["{}/{}".format(x["name"],x["weight"]) for x in content_mp3_seg])
# profile = {"profileinfo": {"content_mp3_seg": content_mp3_seg}, "id": "20000000670265"}
# abc= json.dumps(profile, ensure_ascii=False)
# print(abc)
# # tmp_dict = set()
# with open('./img_flag_res5','r') as d:
#     for line in d:
#         new = json.loads(line.strip())
#         n_vid = new['vid']
#         tmp_dict.add(n_vid)
#
# with open('top_mp3', 'r') as f,  open('./img_flag_res6', 'w') as f2:
#     for line in f:
#         # line = line.split('Deep Response:')[1]
#         # if cnt == 1:
#         dict3 = json.loads(line)
#
#         for key in dict3.keys():
#             for vid in dict3[key]:
#                 if vid in tmp_dict:
#                     continue
#                 parms = getuname(vid)
#                 print("*"*100)
#                 print(parms)
#                 res = dup(parms)
#                 print(res)
#                 try:
#                     tmp = json.loads(res)
#                     if tmp['vid'] == vid:
#                         continue
#                 except:
#                     print('vid')
#                     continue
#
#                 f2.write(res + '\n')
        # cnt += 1
        # if cnt>=2:
        #     brea