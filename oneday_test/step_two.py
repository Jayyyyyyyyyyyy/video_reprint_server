import pickle
import json
import requests
import redis

r = redis.Redis(host='10.42.158.47', port=6379)


def eschecker(vids):
    url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
    payload = {"size": 5000, "query": {"terms": {"id": vids}},
               "_source": ['vid', 'talentstar', 'content_teach']}

    payload = json.dumps(payload)
    headers = {
        'Content-Type': "application/json",
        'cache-control': "no-cache"
    }
    response = requests.request("POST", url, data=payload, headers=headers)
    json1 = response.json()['hits']['hits']
    all_list = []
    for x in json1:
        if 'tagid' not in x['_source']['content_teach']:
            content_teach = -1
        else:
            content_teach = int(x['_source']['content_teach']['tagid'])
        res = (x['_source']['vid'], x['_source']['talentstar'], content_teach)
        all_list.append(res)

    return all_list


def get_urls(vid):
    key = 'url_{}'.format(vid)
    if r.exists(key):
        #从redis中取到video url
        res = json.loads(r.get(key))
    else:
        return None
    if res['videourl'].startswith("/"):
        video = 'http://aqiniudl.tangdou.com'+res['videourl']+'-10.mp4'
    else:
        video = 'http://aqiniudl.tangdou.com/' + res['videourl'] + '-10.mp4'
    return [vid, video]


vids_list = pickle.load(open('step_one_res','rb'))
all_list = []
for ind, vids in enumerate(vids_list):
    res_from_es = eschecker(vids)
    targets = []
    candidates = []
    for vid, star, teach in res_from_es:
        if star>=3 and teach == 362:
            candidates.append(vid)
        else:
            targets.append(vid)
    final_res = (targets, candidates)
    all_list.append(final_res)

import pickle
pickle.dump(list(all_list), open('step_two_res', 'wb'), protocol=2)
