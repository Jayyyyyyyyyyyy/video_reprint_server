
import json
import requests
from requests.adapters import HTTPAdapter

def getuname(vids):
    vids = [vids]
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}}, "_source": ['vid', 'uname', 'uid', 'talentstar']}
        payload = json.dumps(payload)
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        response = requests.request("POST", url, data=payload, headers=headers)
        json1 = response.json()['hits']['hits']
        for x in json1:
            try:
                res = x['_source']
                uname = res['uname']
                uid = res['uid']
                talentstar = res['talentstar']
                return [int(uid), talentstar]
            except:
                return [None, None]
    return [None, None]

def get_firstcat(vids):
    vids = [vids]
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}}, "_source": ['vid', 'uname', 'uid', 'firstcat']}
        payload = json.dumps(payload)
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        response = requests.request("POST", url, data=payload, headers=headers)
        json1 = response.json()['hits']['hits']
        for x in json1:
            try:
                res = x['_source']
                uname = res['uname']
                uid = res['uid']
                firstcat = int(res['firstcat']['tagid'])
                return [int(uid), firstcat]
            except:
                return [None, None]
    return [None, None]

def get_content_mp3(vids):
    vids = [vids]
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}}, "_source": ['vid', 'content_mp3']}
        payload = json.dumps(payload)
        headers = {'Content-Type': "application/json", 'cache-control': "no-cache"}
        response = requests.request("POST", url, data=payload, headers=headers)
        json1 = response.json()['hits']['hits']
        for x in json1:
            try:
                res = x['_source']
                if 'tagname' in res['content_mp3']:
                    content_mp3 = res['content_mp3']['tagname']
                else:
                    content_mp3 = None
                return content_mp3
            except:
                return None
    return None

def getocr(vid, target_img_url):
    jsonbody = {'vid': vid, 'pic': target_img_url}
    url = "http://10.19.67.198:7070/ocrapp/get_ocr"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36', }
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))
    s.mount('https://', HTTPAdapter(max_retries=3))
    resp1 = s.post(url, data=jsonbody, timeout=10)
    have_orc = json.loads(resp1.text)['ocr_info']['have_ocr']
    return have_orc

# abc = {'vid': 1234, 'pic': 'http://aimg.tangdou.com/public/video/2020/1113/20201113000453_11944550.jpg'}
# print(getuname(20000001079987))