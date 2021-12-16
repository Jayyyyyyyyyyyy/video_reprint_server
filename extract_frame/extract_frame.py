# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/7/7 10:33 AM
# @File      : extract_frame.py
# @Software  : PyCharm

'''
    1. 确定切帧策略：前提假设盗播视频与原创视频一致；或者截取原创视频的靠前部分，且时间线对齐误差不超过20秒 
        条件：1，只针对时常大于30秒的视频 
             2，如果两个视频比较，大于等于5帧，则判定此视频为盗播
             
        如何切帧：
            从视频的第10秒开始截取，截取每秒的第一帧，连续截取20秒，共有20张图片
'''
import cv2
from imagededup.methods import CNN
import redis
import json
import sys
import pandas as pd
import logging
logging.basicConfig(
   filename='script.log',
   level=logging.INFO,
   format="[%(asctime)s] %(name)s:%(levelname)s: %(message)s"

)
# logging.FileHandler('script.log', encoding='UTF-8')
# logging.basicConfig(format='%(levelname)s:%(funcName)s:%(message)s', level=logging.DEBUG)

class VideoReprint(object):
    def __init__(self):
        self.video_capture = cv2.VideoCapture()
        self.encoding_map = {}
        self.cnn_encoder = CNN()
        self.r = redis.Redis(host='10.42.158.47', port=6379)
    def valid_url(self, vid, url_string, start_sec=15, end_sec=578):
        # 视频抽帧+提取特征 大约需要11.9秒左右 （20张）
        key = 'video_feature_{}'.format(vid)
        # 如果key在redis中
        if not self.r.exists(key):
            logging.info("key {}, has written".format(key))
            pass
        else:
            self.video_capture.open(url_string)
            total_frames = self.video_capture.get(7)
            fps = self.video_capture.get(5)
            if fps > 0:
                duration = total_frames // fps
                end_sec = int(duration)
                if duration >= 60:
                    try:
                        feature_list = []
                        step = int((end_sec - start_sec) / 40)
                        for sec in range(start_sec, end_sec, step):
                            new_url_string = '{}\t{}'.format(url_string, sec)
                            frame_num = sec * fps
                            self.video_capture.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
                            _, image = self.video_capture.read()
                            cv2.imwrite('lena2_{}.png'.format(sec), image)

                            # image = cv2.resize(image, (224, 224), interpolation=cv2.INTER_CUBIC)
                            # feat_vec = self.cnn_encoder.encode_image(image_array=image)
                            # # save features to redis
                            # feature_list.append([new_url_string, feat_vec[0].tolist()])
                        # save to redis
                        #self.r.set(key, json.dumps(feature_list))
                        # logging.info('vid: {}'.format(vid))
                        # logging.info('key: {} has write into redis, and url: {}'.format(key, url_string))
                        # return url_string

                    except (Exception) as e:
                        logging.debug(1,e)
                        pass
                else:
                    logging.info('The duration of video is too short, vid: {}'.format(vid))
                    pass
            else:
                logging.info('the value of fps is smaller than or equal to 0')
                pass


                # return 'Error: Cannot open video url: {}'.format(url_string)

    def get_urls(self, vid):
        # vid = '8982780'
        key = 'url_{}'.format(vid)
        if self.r.exists(key):
            #从redis中取到video url
            res = json.loads(self.r.get(key))
        else:
            logging.info('cannot find key: {} in redis'.format(key))
            return None
        if res['videourl'].startswith("/"):
            video = 'http://aqiniudl.tangdou.com'+res['videourl']+'-10.mp4'
        else:
            video = 'http://aqiniudl.tangdou.com/' + res['videourl'] + '-10.mp4'
        return [vid, video]

    def is_open(self, tup):
        _, url = tup
        if self.video_capture.open(url):
            return url
        else:
            url = url.replace("-10", "-20")
            if self.video_capture.open(url):
                return url
            else:
                url = url.replace("-20.mp4", ".mp4")
                if self.video_capture.open(url):
                    return url
                else:
                    return None
videoprint = VideoReprint()

target_vid = 1500677914852
#target_url = 'http://aqiniudl.tangdou.com/201908/86A282E9-1D8D-704C-6B0E-412D9A31FB1B-10.mp4'
target_url = videoprint.get_urls(target_vid)
if target_url is not None:
    valid_url = videoprint.is_open(target_url)
    if valid_url is not None:
        videoprint.valid_url(target_vid, valid_url)
    else:
        logging.info('vid: {} has no valid url {}'.format(target_vid, target_url))
else:
    pass


# in_file = sys.argv[1]
# df = pd.read_csv(in_file)
# newdf = df.iloc[120000:140000]
# for index, row in newdf.iterrows():
#     target_vid = row[2]
#     target_vid = 1500677914852
#     #target_url = 'http://aqiniudl.tangdou.com/201908/86A282E9-1D8D-704C-6B0E-412D9A31FB1B-10.mp4'
#     target_url = videoprint.get_urls(target_vid)
#     if target_url is not None:
#         valid_url = videoprint.is_open(target_url)
#         if valid_url is not None:
#             videoprint.valid_url(target_vid, valid_url)
#         else:
#             logging.info('vid: {} has no valid url {}'.format(target_vid, target_url))
#     else:
#         pass
