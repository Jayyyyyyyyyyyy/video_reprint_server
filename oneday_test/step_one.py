
import json

tmp = []
### 为了减少计算
with open('/Users/jiangcx/Downloads/video_reprint_20210125', 'r', encoding='utf-8') as f:
    for line in f:
        vid = int(json.loads(line)['vid'])
        tmp.append(vid)
with open('/Users/jiangcx/Downloads/to_update', 'r', encoding='utf-8') as f:
    for line in f:
        vid = int(json.loads(line)['vid'])
        tmp.append(vid)
done_dict = set(tmp)




dict = {}
mp3_count = '/Users/jiangcx/Downloads/mp3.all.json'
with open(mp3_count, 'r', encoding='utf-8') as f:
    for line in f:
        line = json.loads(line)
        if line['type'] in (6,8) and line['flag'] in (0,1):
            if line['qcmp3'] == line['mp3']:
                dict[line['qcmp3']] = [int(line['video_count']), line['rescount1']]


raw_path = '/Users/jiangcx/Downloads/search_result_20210125'
all_list = []
cnt = 0
with open(raw_path, 'r', encoding='utf-8') as f:  # open('/Users/jiangcx/Downloads/search_result_20210125_res', 'w', encoding='utf-8') as outf:
    for line in f:
        line = json.loads(line.split('\t')[5])
        if line['searchres'] == {}:
            continue
        search_query = line['searchres']['query']
        if search_query in dict:
            search_result = line['searchres']['result']
            cnt += 1 # 7695

            tmp_vid_list = []
            for ele in search_result:
                tmp_vid_list.append(ele['vid'])
            # print(tmp_vid_list)
            all_list.append(tmp_vid_list)

# get final result
new_all_list = []
for group in all_list:
    new_group = []
    for x in group:
        x = int(x)
        if x not in done_dict:
            new_group.append(x)
    new_all_list.append(new_group)

import pickle
pickle.dump(list(new_all_list), open('step_one_res','wb'),protocol=2)


