
# -*- coding: utf-8 -*-
# @Time    : 2020/6/10 18:55
# @Author  : Jiang Chenxi
# @File    : video_reprint.py
# @Project : 视频盗播
# @Software: PyCharm
# @Company : Xiao Tang


import cv2
from imagededup.methods import CNN
import redis
import json
import logging
import os
from datetime import datetime


class SaveFeature(object):
    def __init__(self):
        self.video_capture = cv2.VideoCapture()
        self.encoding_map = {}
        self.cnn_encoder = CNN()
        self.r = redis.Redis(host='10.42.158.47', port=6379)


    def extrct_frame_and_feature(self, vid, url_string, key, start_sec, step = 1):

        dir = datetime.now().strftime("%Y-%m-%d")
        dir = os.path.join('img_feature', dir)
        if not os.path.exists(dir):
            os.makedirs(dir)
        filename = os.path.join(dir, datetime.now().strftime("%H"))
        with open(filename, 'a+') as f:
            self.video_capture.open(url_string)
            total_frames = self.video_capture.get(7)
            fps = self.video_capture.get(5)
            if fps > 0:
                duration = total_frames // fps
                end_sec = int(duration)
                if duration >= 60:
                    # try:
                    step = int((end_sec - start_sec)/40)
                    for sec in range(start_sec, end_sec, step):
                        frame_num = sec * fps
                        self.video_capture.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
                        _, image = self.video_capture.read()
                        image = cv2.resize(image, (224, 224), interpolation=cv2.INTER_CUBIC)
                        feat_vec = self.cnn_encoder.encode_image(image_array=image)
                        # save features to redis
                        tmp_dict = {}
                        tmp_dict['vid'] = '{}_{}'.format(vid,sec)
                        tmp_dict['feature'] = feat_vec[0].tolist()
                        res_dict = json.dumps(tmp_dict)
                        f.write(res_dict + '\n')
                        logging.info("{} write to file".format(tmp_dict['vid']))

                    self.r.set(key, '')
                    self.video_capture.release()
                    # except (Exception) as e:
                    #     logging.info(e)
                    #     pass
                else:
                    logging.info('The duration of video is too short, vid: {}'.format(vid))
                    pass
            else:
                logging.info('the value of fps is smaller than or equal to 0')
                pass

    def gen_img_feature(self, vid, url_string, talentstar=None, start_sec=20, end_sec=40):
        start_sec = 15
        step = 1
        logging.info("going to special route, vid:{} and url:{}".format(vid, url_string))
        key = 'spec_video_feature_{}'.format(vid)
        if self.r.exists(key):
            logging.info("{} is exist, True ".format(key))
            pass
        else:
            logging.info("{} is exist, False, then extracting ".format(key))
            self.extrct_frame_and_feature(vid, url_string, key, start_sec, step)



if __name__ == '__main__':
    pass
