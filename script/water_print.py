
import requests
import json
def dup(jsonbody):
    #url = "http://10.19.21.255:9005/polls/dup_detector"
    url = "http://10.42.16.15:9003/polls/dup_detector"
    #url = "http://10.19.67.198:9003/polls/dup_detector"
    #url = "http://10.19.75.189:9003/polls/dup_detector"
    #url = "http://10.10.5.41:9003/polls/dup_detector"
    #url = "http://127.0.0.1:8000/polls/dup_detector"
    #url = "http://10.10.25.182:9003/polls/dup_detector"
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
print(time.ctime())
# vids = 1500679062672 # content_teacher 找不出来 # 1500678491373
# vids = 1500678136078
# vids = 1500679391389
#1500677914852
for x in [1500679307588]:# , 1500679179553,1500679272856,1500679307588]:1500678516571
    vids = x # 原创 1500678491373
    parms = getuname(vids)
    print(parms)
    res = dup(parms)
    print(res)
    # print(time.ctime())
    # res = json.loads(res)
    # print(res)
# 1500678324622 1500678056282 1500678008988

# with open('/Users/jiangcx/Documents/pdate_2021-03-31_04', 'r') as f,  open('./img_flag_res', 'w') as f2:
#     for line in f:
#         vids = line.strip()
#         # vids = json.loads(vids)['id']
#         parms = getuname(vids)
#         print(parms)
#         res = dup(parms)
#         print(res)
#         f2.write(res+'\n')
# #
# with open('img_flag', 'r') as f,  open('./img_flag_res4', 'w') as f2:
#     cnt = 0
#     for line in f:
#         # line = line.split('Deep Response:')[1]
#         # if cnt == 1:
#         vid = json.loads(line)['vid']
#         parms = getuname(vid)
#
#         print("*"*100)
#         print(parms)
#         res = dup(parms)
#         print(res)
#
#         f2.write(res + '\n')
        # cnt += 1
        # if cnt>=2:
        #     break

