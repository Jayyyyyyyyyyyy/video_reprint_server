# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2021/10/27 10:33 AM
# @File      : filter.py
# @Software  : PyCharm
'''
根据条件，对某些视频不进行视频盗播检测处理
'''

import os, time, json, logging, requests, redis
from datetime import datetime
from .get_uname import getocr
from .video_cover_extract import get_feature
from .img_feature2hdfs import SaveFeature

hostname = '10.42.158.47'
port = 6379
r = redis.Redis(host=hostname, port=port)

save_feature = SaveFeature()

def time_format(start, end, form = 's'):
    if form == 's':
        string = str(round(end - start, 2))+'s'
    else:
        string = str(round((end - start)*1000, 1))+'ms'
    return string

def get_urls(vid):
    logging.info("getting urls {} ...".format(vid))
    def request_url(vid):
        url = "http://10.10.95.75:8081/materiel_center/materiel/{}".format(vid)
        resp1 = requests.get(url)
        return resp1.text
    res = request_url(vid)

    res = json.loads(res)
    pic_url = res['data']['cover']
    video_url = res['data']['storeUrlList']

    if pic_url and video_url:
        pic_url = pic_url.split("!")[0]
        video_url = 'http://aqiniudl.tangdou.com/' + video_url[0]
    else:
        # 请求结果若为None
        logging.warning("cant find cover url from http://10.10.95.75:8081/materiel_center/materiel/{}".format(vid))
        return [vid, None, None]
    if 'http' not in pic_url:
        pic = 'http://aimg.tangdou.com' + pic_url
        if '!' in pic:
            pic = pic.split('!')[0]
        return [vid, pic, video_url]
    else:
        logging.info("vid: {} and pic_url: {} and video_url: {}".format(vid, pic_url, video_url))
        return [vid, pic_url, video_url]

def get_urls2(vid):
    key = 'url_{}'.format(vid)
    if r.exists(key):
        #从redis中取到pic url
        res = json.loads(r.get(key))
        cover_url = res['pic']
        video_url = res['videourl']
        if 'http' not in cover_url:
            cover_url = 'http://aimg.tangdou.com' + cover_url
            if '!' in cover_url:
                cover_url = cover_url.split('!')[0]
            logging.info("vid: {} and pic_url: {}".format(vid, cover_url))
        else:
            logging.info("vid: {} and pic_url: {}".format(vid, cover_url))
        if 'mp4' in video_url:
            video_url = video_url[:-7]
        if video_url.startswith("/"):
            video = 'http://aqiniudl.tangdou.com' + video_url + '-10.mp4'
        else:
            video = 'http://aqiniudl.tangdou.com/' + video_url + '-10.mp4'
        return [vid, cover_url, video]

    else:
        logging.info('cannot find key: {} in redis'.format(key))
        return [vid, None, None]

def filter_videos(vid, title, uname, ctype, content_teacher, content_mp3, talentstar, firstcat):
    shortcut = True
    if ctype in (105, 106, 107, 501, 502, 503, -1) or (ctype == 103 and (content_teacher == '' or content_mp3 == '')) or firstcat!=264:
        result = {"code": 1, "reprint": {"img_reprint_vidlist": [], "video_reprint_flag": 0, "video_reprint_vidlist": [], "img_reprint_flag":
            0, "text_reprint_flag": 0, "text_reprint_vidlist": [], 'img_logo': -2}, "vid": vid}
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info("some ctype videos dont detect: {}".format(ctype))
        logging.info("Condition1 Response:" + responsejson)
        return (shortcut, responsejson, 0, '', '')
    vid, target_cover_url, target_video_url = get_urls(vid) # 获取封面url，请求翔龙接口
    if target_cover_url == None:
        vid, target_cover_url, target_video_url = get_urls2(vid)  # 获取封面url，从redis
    if target_cover_url == None:
        result = {"code": 1, "reprint": {"img_reprint_vidlist": [], "video_reprint_flag": 0, "video_reprint_vidlist": [], "img_reprint_flag":
            0, "text_reprint_flag": 0, "text_reprint_vidlist": [], 'img_logo': -2}, "vid": vid}
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info("No cover url")
        logging.info("Condition2 Response:" + responsejson)
        return (shortcut, responsejson, 0, '', '')

    logging.info("Cover pic url: {} {}".format(vid, target_cover_url))
    vid = int(vid)

    water_print_flag = getocr(vid, target_cover_url)
    if isinstance(water_print_flag, int):
        pass
    else:
        water_print_flag = -1

    if len(title.strip()) == 0:
        result = {"code": 1, "reprint": {"img_reprint_vidlist": [], "video_reprint_flag": 0, "video_reprint_vidlist": [], "img_reprint_flag":
            0, "text_reprint_flag": 0, "text_reprint_vidlist": [], 'img_logo': water_print_flag}, "vid": vid}
        logging.info("No title")
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info("Condition3 Response:" + responsejson)
        return (shortcut, responsejson, 0, target_cover_url, target_video_url)

    #### 抽针建立图片索引，先对封面进行建立索引
    if talentstar >= 4:
        start_cover = time.time()
        feature = get_feature(vid, target_cover_url)
        # cover feature写入本地 开始
        dir = datetime.now().strftime("%Y-%m-%d")
        dir = os.path.join('img_feature', dir)
        if not os.path.exists(dir):
            os.makedirs(dir)
        filename = os.path.join(dir, datetime.now().strftime("%H"))
        with open(filename, 'a+') as f:
            f.write(feature + '\n')
        # cover feature写入本地 结束
        end_cover = time.time()
        time_cover = time_format(start_cover, end_cover, 'ms')
        logging.info("vid {}, cover feature saved, and time: {}".format(vid, time_cover))
        # TODO 优化方案 传入duration参数，直接判断视频长短，小于60s的视频，直接跳过
        start_frames = time.time()
        save_feature.gen_img_feature(vid, target_video_url)
        end_frames = time.time()
        time_frames = time_format(start_frames, end_frames, 's')
        logging.info("vid {}, frames feature saved, and time: {}".format(vid, time_frames))

        result = {"reprint": {"img_reprint_vidlist": [], "video_reprint_flag": 0, "video_reprint_vidlist": [], "img_reprint_flag": 0,
                              "text_reprint_flag": 0, "text_reprint_vidlist": [], 'img_logo': water_print_flag}, "vid": vid}
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info("talent works save")
        logging.info("Condition4 Response:" + responsejson)
        return (shortcut, responsejson, 0, target_cover_url, target_video_url)

    if uname in ['32步学舞', '32步广场舞']:
        result = {"code": 1, "reprint": {"img_reprint_vidlist": [], "video_reprint_flag": 0, "video_reprint_vidlist": [], "img_reprint_flag": 0,
                                         "text_reprint_flag": 0, "text_reprint_vidlist": [], 'img_logo': water_print_flag}, "vid": vid}
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info("special uname: {}".format(uname))
        logging.info("Condition5 Response:" + responsejson)
        return (shortcut, responsejson, 0, target_cover_url, target_video_url)

    return (False, '', water_print_flag, target_cover_url, target_video_url)