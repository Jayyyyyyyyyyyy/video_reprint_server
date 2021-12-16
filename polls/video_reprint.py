
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
from collections import Counter
import requests
from .cassandra_ops import set_feature, get_feature

def eschecker(vids):
    vids = [vids]
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}},
                   "_source": ['vid', 'talentstar', 'content_teach']}

        payload = json.dumps(payload)
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        json1 = response.json()['hits']['hits']
        for x in json1:
            res = x['_source']
            if 'vid' in res:
                vid = res['vid']
            else:
                continue
            try:
                talentstar = res['talentstar']
                tmp_teach = res['content_teach']
                if 'tagid' in tmp_teach:
                    tagid = int(tmp_teach['tagid'])
                else:
                    tagid = 0
                if talentstar  > 3:# and tagid==362: #asd
                    return True
            except:
                return False
    return False


class VideoReprint(object):
    def __init__(self):
        self.video_capture = cv2.VideoCapture()
        self.encoding_map = {}
        self.cnn_encoder = CNN()
        self.r = redis.Redis(host='10.42.158.47', port=6379)
    def del_encoding_map(self):
        self.encoding_map.clear()

    def valid_url(self, vid, url_string, talentstar=None, start_sec=20, end_sec=40):
        # 视频抽帧+提取特征 大约需要11.9秒左右 （20张）
        logging.info("vid:{} and url:{}".format(vid, url_string))
        key = 'video_feature_{}'.format(vid)
        # 如果key在redis中
        feat_list = get_feature(key)
        if feat_list != None:
            logging.info('exist')
            feat_list = json.loads(feat_list)
            old_url_mark = feat_list[0][0].split('\t')[0][-13:-7]
            tmp_key = 'url_{}'.format(vid)
            res_obj = json.loads(self.r.get(tmp_key))
            if 'mp4' in res_obj['videourl']:
                cur_rul_mark = res_obj['videourl'][-13:-7]
            else:
                cur_rul_mark = res_obj['videourl'][-6:]
            logging.info('url_compare old {} new{}'.format(old_url_mark, cur_rul_mark))
            if old_url_mark == cur_rul_mark:
                for feat in feat_list:
                    self.encoding_map[feat[0]] = feat[1]
            else:
                self.video_capture.open(url_string)
                total_frames = self.video_capture.get(7)
                fps = self.video_capture.get(5)
                if fps > 0:
                    duration = total_frames // fps
                    if duration >= end_sec:
                        try:
                            feature_list = []
                            for sec in range(start_sec, end_sec):
                                new_url_string = '{}\t{}'.format(url_string, sec)
                                frame_num = sec * fps
                                self.video_capture.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
                                _, image = self.video_capture.read()
                                image = cv2.resize(image, (224, 224), interpolation=cv2.INTER_CUBIC)
                                logging.info("正在提取vid{}的第{}秒截图特征...".format(vid, sec))
                                feat_vec = self.cnn_encoder.encode_image(image_array=image)
                                logging.info("提取vid{}的第{}秒截图特征完成".format(vid, sec))
                                # save features to redis
                                feature_list.append([new_url_string, feat_vec[0].tolist()])
                                self.encoding_map[new_url_string] = feat_vec[0]
                            # save to redis
                            if talentstar != None:
                                if talentstar >= 4:
                                    # self.encoding_map = {}
                                    set_feature(key, json.dumps(feature_list))
                                    logging.info('vid: {}'.format(vid))
                                    logging.info('key: {} has write into redis, and url: {}'.format(key, url_string))
                            else:
                                if eschecker(vid):
                                    set_feature(key, json.dumps(feature_list))
                                    logging.info('vid: {}'.format(vid))
                                    logging.info('key: {} has write into redis, and url: {}'.format(key, url_string))
                            # return url_string
                        except (Exception) as e:
                            logging.debug(1, e)
                            pass
                    else:
                        logging.info('The duration of video is too short, vid: {}'.format(vid))
                        pass
                else:
                    logging.info('the value of fps is smaller than or equal to 0')
                    pass

        else:
            logging.info('not exist')
            self.video_capture.open(url_string)
            total_frames = self.video_capture.get(7)
            fps = self.video_capture.get(5)
            if fps > 0:
                duration = total_frames // fps
                if duration >= end_sec:
                    try:
                        feature_list = []
                        for sec in range(start_sec, end_sec):
                            new_url_string = '{}\t{}'.format(url_string, sec)
                            frame_num = sec * fps
                            self.video_capture.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
                            _, image = self.video_capture.read()
                            image = cv2.resize(image, (224, 224), interpolation=cv2.INTER_CUBIC)
                            feat_vec = self.cnn_encoder.encode_image(image_array=image)
                            # save features to redis
                            feature_list.append([new_url_string, feat_vec[0].tolist()])
                            self.encoding_map[new_url_string] = feat_vec[0]
                        # save to redis
                        if talentstar != None:
                            if talentstar >= 4:
                                # self.encoding_map = {}
                                set_feature(key, json.dumps(feature_list))
                                logging.info('vid: {}'.format(vid))
                                logging.info('key: {} has write into redis, and url: {}'.format(key, url_string))
                        else:
                            if eschecker(vid):
                                set_feature(key, json.dumps(feature_list))
                                logging.info('vid: {}'.format(vid))
                                logging.info('key: {} has write into redis, and url: {}'.format(key, url_string))
                        # return url_string
                    except (Exception) as e:
                        logging.debug(1, e)
                        pass
                else:
                    logging.info('The duration of video is too short, vid: {}'.format(vid))
                    pass
            else:
                logging.info('the value of fps is smaller than or equal to 0')
                pass

    def similarity(self, target_url, candidates):
        cand_dict = {candidates[1]:candidates[0]}

        def my_counter(list):
            mylist = Counter(list)
            dici = dict(mylist)
            tmp2 = []
            for key in dici.keys():
                tmp2.append((key, dici[key]))
            tmp2 = sorted(tmp2, key=lambda tup: tup[1], reverse=True)
            return tmp2
        # get simi results
        duplicates = self.cnn_encoder.find_duplicates(encoding_map=self.encoding_map, min_similarity_threshold=0.90, scores=True)
        #self.encoding_map = {}
        for key in list(self.encoding_map.keys()):
            first, second = key.split('\t')
            if candidates[1] == first:
                del self.encoding_map[key]
        all_dups = []
        for key in list(duplicates.keys()):
            pre_fix, suffix = key.split('\t')
            # only calculate the required vid so filter others out
            if target_url == pre_fix:
                # every frame only remain the unique one and remove required one itself
                duplicates[key] = list(set([x[0].split('\t')[0] for x in duplicates[key] if pre_fix not in x[0]]))
                all_dups.extend(duplicates[key])
        sort = my_counter(all_dups)
        final_res = []
        for ele in sort:
            url, cnt = ele
            # if cnt is large than 5, we determine this vid is reprint. For history we need to compare createtime.
            if cnt >= 5:
                final_res.append(cand_dict[url])
        return final_res

    def get_urls(self, vid):
        def request_url(vid):
            url = "http://10.10.95.75:8081/materiel_center/materiel/{}".format(vid)
            resp1 = requests.get(url)
            return resp1.text
        try:
            res = request_url(vid)
            logging.info("get url {}".format(vid))
            res = json.loads(res)['data']['storeUrlList'][0]
            video = 'http://aqiniudl.tangdou.com/' + res
            return [vid, video]
        except:
            return [vid, None]

    def get_urls2(self, vid):
        key = 'url_{}'.format(vid)
        if self.r.exists(key):
            #从redis中取到video url
            res = json.loads(self.r.get(key))
        else:
            logging.info('cannot find key: {} in redis'.format(key))
            return [vid, None]
        if 'mp4' in res['videourl']:
            res['videourl'] = res['videourl'][:-7]
        if res['videourl'].startswith("/"):
            video = 'http://aqiniudl.tangdou.com'+res['videourl']+'-10.mp4'
        else:
            video = 'http://aqiniudl.tangdou.com/' + res['videourl'] + '-10.mp4'
        return [vid, video]

    def is_open(self, url):
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

if __name__ == '__main__':
    pass
    # videoprint = VideoReprint()
    # for x in range(1):
    #     target_vid = 9155399
    #     target_url = 'http://aqiniudl.tangdou.com/C8A98ADD89F242519C33DC5901307461-10.mp4'
    #     videoprint.valid_url(target_vid, target_url)
    #     tmplist = [(9133218, 'http://aqiniudl.tangdou.com/C3BC129D208957BE9C33DC5901307461-10.mp4'),
    #                (9133206, 'http://aqiniudl.tangdou.com/9FFBCC8CD25437789C33DC5901307461-10.mp4'),
    #                (9399985, 'http://aqiniudl.tangdou.com/FD1A1BD5C7FC63F19C33DC5901307461-10.mp4')]
    #
    #     for x in tmplist:
    #         videoprint.valid_url(x[0], x[1])
    #     score =videoprint.similarity(target_url, tmplist)
    #     print(score)

    # import sys
    # import pandas as pd
    # videoprint = VideoReprint()
    # in_file = sys.argv[1]
    # df = pd.read_csv(in_file)
    # newdf = df.iloc[5:20000]
    # for index, row in newdf.iterrows():
    #     target_vid = row[0]
    #     #target_vid = 1500669492684
    #     #target_url = 'http://aqiniudl.tangdou.com/201908/86A282E9-1D8D-704C-6B0E-412D9A31FB1B-10.mp4'
    #     target_url = videoprint.get_urls(target_vid)
    #     if target_url is not None:
    #         valid_url = videoprint.is_open(target_url)
    #         if valid_url is not None:
    #             videoprint.valid_url(target_vid, valid_url)
    #         else:
    #             log ging.info('vid: {} has no valid url {}'.format(target_vid, target_url))
    #     else:
    #         pass
