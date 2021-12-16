import json

with open('img_flag','r') as f:
    for line in f:
        jsondata = json.loads(line.strip(), encoding='utf-8')
        jsondata['id'] = jsondata['vid']
        jsondata['profileinfo'] = jsondata['reprint']
        del jsondata['vid']
        del jsondata['reprint']
        print(jsondata)
        break