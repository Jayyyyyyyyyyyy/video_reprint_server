
# -*- coding: utf-8 -*-
# @Time    : 2020/6/10 18:55
# @Author  : Jiang Chenxi
# @File    : video_reprint.py
# @Project : 视频盗播
# @Software: PyCharm
# @Company : Xiao Tang

import numpy as np
from imagededup.methods import CNN
import requests
import json
import logging
from PIL import Image as pil_image
import urllib
import io
import socket
from urllib.error import HTTPError, URLError


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
cnn_encoder = CNN()

def load_img(path, target_size=None):
    try:
        response = urllib.request.urlopen(path, timeout=10).read()
        f = io.BytesIO(response)
        img = pil_image.open(f)
        img = img.convert('RGB')
    except HTTPError as error:
        logging.error('Data not retrieved because %s\nURL: %s', error, path)
        return None
    except URLError as error:
        if isinstance(error.reason, socket.timeout):
            logging.error('socket timed out - URL %s', path)
        else:
            logging.error('some other error happened')
        return None
    else:
        logging.info('Access successful.')

    if target_size is not None:
        width_height_tuple = (target_size[1], target_size[0])
        if img.size != width_height_tuple:
            try:
                img = img.resize(width_height_tuple)
            except:
                logging.error('the url cannot resize {}'.format(path))
                return None
    res = np.asarray(img)
    # if res.shape[-1]==4:
    #     return(res[:,:,:-1])
    # else:
    return res

def get_urls( vid):
    def request_url(vid):
        url = "http://10.10.95.75:8081/materiel_center/materiel/{}".format(vid)
        resp1 = requests.get(url)
        return resp1.text
    res = request_url(vid)
    logging.info("get url {}".format(vid))
    pic_url = json.loads(res)['data']['cover'].split("!")[0]

    if 'http' not in pic_url:
        pic = 'http://aimg.tangdou.com' + pic_url
        if '!' in pic:
            pic = pic.split('!')[0]
        return [vid, pic]
    else:
        logging.info("vid: {} and pic_url: {}".format(vid, pic_url))
        return [vid, pic_url]


def get_feature(vid, cover_url):
    if cover_url:
        res = load_img(cover_url, target_size=(224, 224))
        if res is not None:
            feat_vec = cnn_encoder.encode_image(image_array = res)
            tmp = {}
            tmp['vid'] = vid
            tmp['feature'] = feat_vec[0]
            res = json.dumps(tmp, cls=NumpyEncoder, ensure_ascii=False)
            return res
        else:
            return ''
    else:
        return ''

#print(get_feature(1500679314592))
#
# import json
# with open('my_star', 'r') as f:
#     tmp = []
#     for line in f:
#         line = json.loads(line)
#         vid = line['vid']
#         tmp.append(vid)
# dict = set(tmp)
#
# with open('star', 'r', encoding='utf-8') as f, open('star_feature3', 'w', encoding='utf-8') as f2:
#     for line in f:
#         # try:
#         vid = json.loads(line)['vid']
#         if vid in dict:
#             print('{} already processed'.format(vid))
#             continue
#         print("{} new feature".format(vid))
#         feature = get_feature(vid)
#         # tmp = {}
#         # tmp['vid'] = vid
#         # tmp['feature'] = feature
#         # res = json.dumps(tmp, cls=NumpyEncoder,  ensure_ascii=False)
#         f2.write(feature+'\n')
#         # except:
#         #     print(line)
# # nohup python video_cover_extract.py -u > nohup3.out 2>&1 &
#
#
#
#
#
