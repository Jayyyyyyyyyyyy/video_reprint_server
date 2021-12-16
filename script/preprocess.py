import json
path = '/Users/jiangcx/Documents/tmp_top_music'
tmp = []
with open(path, 'r', encoding='utf-8') as f, open('top_mp3', 'r')as f2, open('./img_flag_res5', 'w') as f3:
    tmp_dict = set()
    for f2_line in f2:
        dict3 = json.loads(f2_line)
        for key in dict3.keys():
            for vid in dict3[key]:
                tmp_dict.add(vid)


    for line in f:
        obj = json.loads(line)
        vid = obj['vid']
        if vid in tmp_dict:
            f3.write(line)
