# -*- coding: utf-8 -*-
# @Time    : 2020/6/10 18:55
# @Author  : LVP
# @File    : contentreacll.py
# @Project : 倒播排重
# @Software: PyCharm
# @Company : Xiao Tang
import redis
import json
import requests
import logging
import logging.handlers
import os
import time
import threading

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s]  %(levelname)s %(filename)s[line:%(lineno)d]: %(message)s')


class ContentRecall:
    def __init__(self, url="http://10.10.101.226:9221/portrait_video/video/_search", mdv_freq=15):
        self.__headers = {'Content-Type': 'application/json'}
        self.__url = url
        self.__logger = logging.getLogger('ContentRecall')
        self.__mdv_path = 'data/mdv_data/vid_mdv'
        self.__mdv_freq = mdv_freq
        self.__mdv_dict = self._load_mdv()

        self.__recall_num = 20
        self.__cos_threshold = 0.9
        self.__vec_url = "http://10.42.129.174:5004/vecrecall"

        self.__star = 4
        self.__balcklist_path = 'data/blacklist'
        self.__balcklist_uid = self._load_blacklist()
        # self.__vid_by_star_path = 'data/vid_data/star_vid'
        # self.__star_vid = self._load_vid_by_star()

        # 每天定时更新mdv_dict, star_vid 内存
        self.t1 = threading.Thread(target=self._update_mdv)
        self.t1.start()
        # self.t2 = threading.Thread(target=self._update_vid_by_star)
        # self.t2.start()
        self.__logger.info('ContentRecall initial finished')

    @staticmethod
    def _parse_recall(text):
        jsonbody = json.loads(text)
        hits_list = jsonbody['hits']['hits']
        ret = []
        for hits in hits_list:
            vid = hits['_source']['vid']
            createtime = hits['_source']['createtime']
            ret.append((vid, createtime))
        return ret


    # def _get_hdfs_file(self, input_path, output_path):
    #     cmd = "hdfs dfs -getmerge " + input_path + ' ' + output_path
    #     self.__logger.info('cmd: %s' % cmd)
    #     cmd_ret = subprocess.getstatusoutput(cmd)
    #     if cmd_ret[0] != 0:
    #         self.__logger.error('获取hdfs文件出错:{}'.format(cmd_ret[1]))



    def _load_blacklist(self, ):
        blacklist_set = set()
        hostname = '10.42.186.173'
        port = 6379
        r = redis.Redis(host=hostname, port=port)
        blacklist = r.smembers('reprint_uids')
        for uid in list(blacklist):
            blacklist_set.add(int(uid))
        self.__logger.info('blacklist uid num is: {}'.format(len(blacklist_set)))
        return blacklist_set

    def _update_blacklist(self, ):
        self.__logger.info("starting new thread for updating blacklist set from file")
        update_flag = False
        while True:
            # 每15分钟轮训一次
            time.sleep(3600)
            blacklist_chtime = os.stat(self.__balcklist_path).st_mtime
            blacklist_chtime = time.strftime('%Y-%m-%d', time.localtime(blacklist_chtime))
            now_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            # yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")


            if update_flag:
                # 进入新的一天（每天中午11点更新跑spark更新数据）
                if now_time[-2:] != blacklist_chtime[-2:]:
                    update_flag = False
            else:
                # 当天文件还没有更新，则不进行更新操作
                if now_time[-2:] != blacklist_chtime[-2:]:
                    # self._get_hdfs_file('/user/lvp/star_vid/star_vid_{}'.format(yesterday),'data/vid_data/star_vid')
                    continue
                # 当天文件更新，则更新内存，sleep五分钟为了确保文件更新完毕
                time.sleep(3600)
                try:
                    self.__balcklist_uid = self._load_blacklist()
                    self.__logger.info('updated blacklist set in memory')
                    update_flag = True
                except Exception as e:
                    self.__logger.error("updating blacklist set failed! %s" % str(e))
                    update_flag = False

    def _load_mdv(self, ):
        _dict = {}
        with open(self.__mdv_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = json.loads(line)
                if line['m_dv'] > self.__mdv_freq:
                    _dict[line['vid']] = line['m_dv']
        self.__logger.info('vid mdv nums are: {}'.format(len(_dict)))
        return _dict

    def _update_mdv(self, ):
        self.__logger.info("starting new thread for updating vid-mdv dict from file")
        update_flag = False
        while True:
            # 每15分钟轮训一次
            time.sleep(3600)
            mdv_chtime = os.stat(self.__mdv_path).st_mtime
            mdv_chtime = time.strftime('%Y-%m-%d', time.localtime(mdv_chtime))
            now_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            # yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")


            if update_flag:
                # 进入新的一天（因为文件再凌晨3点之后才更新的）
                if now_time[-2:] != mdv_chtime[-2:]:
                    update_flag = False
            else:
                # 当天文件还没有更新，则不进行更新操作
                if now_time[-2:] != mdv_chtime[-2:]:
                    # self._get_hdfs_file('/user/lvp/vid_mdv/vid_mdv_{}'.format(yesterday),'data/mdv_data/vid_mdv')
                    continue
                # 当天文件更新，则更新内存，sleep五分钟为了确保文件更新完毕
                time.sleep(3600)
                try:
                    self.__mdv_dict = self._load_mdv()
                    self.__logger.info('updated vid-mdv dict in memory')
                    update_flag = True
                except Exception as e:
                    self.__logger.error("updating vid-mdv dict failed! %s" % str(e))
                    update_flag = False

    def recall(self, content_teacher_id, content_mp3, size=20):
        ret = []
        try:
            payload = '{"size": %d, "query":{ "bool": {"must":[{"term": {"content_teacher.tagid": {"value": "%s"}}}, \
            {"term": {"content_mp3.tagname": {"value": "%s"} }}, {"range": {"talentstar": {"from": %d, "to": null, "include_lower": true, "include_upper": true, "boost": 1.0}}}, {"range": {"duration": {"from": 40, "to": null, "include_lower": true, "include_upper": true, "boost": 1.0}}}]}},"sort": [{"talentstar": {"order": "desc"}},{"dy15_clickNum": {"order": "desc"}}], \
            "_source": ["vid", "createtime"]}' % (size, str(content_teacher_id), content_mp3, self.__star)
            response = requests.request("GET", self.__url, headers=self.__headers, data=payload.encode('utf-8'))
            ret = self._parse_recall(response.text)
        except Exception as e:
            self.__logger.error('recall request error, ' + str(e))
        return ret

    def recall_teacher(self, content_teacher_id, size=20):
        ret = []
        try:
            payload = '{"size": %d, "query":{ "bool": {"must":[{"term": {"content_teacher.tagid": {"value": "%s"}}}, {"range": {"talentstar": {"from": %d, "to": null, "include_lower": true, "include_upper": true, "boost": 1.0}}}, {"range": {"duration": {"from": 40, "to": null, "include_lower": true, "include_upper": true, "boost": 1.0}}}]}},"sort": [{"talentstar": {"order": "desc"}},{"dy15_clickNum": {"order": "desc"}}], \
            "_source": ["vid", "createtime"]}' % (size, str(content_teacher_id), self.__star)

            response = requests.request("GET", self.__url, headers=self.__headers, data=payload.encode('utf-8'))
            ret = self._parse_recall(response.text)
        except Exception as e:
            self.__logger.error('recall_teacher request error, ' + str(e))
        return ret

    def recall_mp3(self, content_mp3, size=20):
        ret = []
        try:
            payload = '{"size": %d, "query":{ "bool": {"must":[{"term": {"content_mp3.tagname": {"value": "%s"} }}, {"range": {"talentstar": {"from": %d, "to": null, "include_lower": true, "include_upper": true, "boost": 1.0}}}, {"range": {"duration": {"from": 40,"to": null, "include_lower": true, "include_upper": true, "boost": 1.0}}}]}},"sort": [{"talentstar": {"order": "desc"}},{"feed_clicknum": {"order": "desc"}}], "_source": ["vid", "createtime"]}' % (size, content_mp3, self.__star)

            response = requests.request("GET", self.__url, headers=self.__headers, data=payload.encode('utf-8'))
            ret = self._parse_recall(response.text)
        except Exception as e:
            self.__logger.error('recall_mp3 request error, ' + str(e))
        return ret

    def _filter_mdv(self, recall_list):
        if not recall_list:
            return []
        new_list = list(filter(lambda x: self.__mdv_dict.get(x[0], 0) > 0 and x[1].startswith('2'), recall_list))
        return new_list

    def _recall_vec(self, vid, title, uname, createtime):
        ret = requests.post(
            self.__vec_url,
            json={
                "vid": vid,
                "title": title,
                "uname": uname,
                "createtime": createtime,
                "recall_num": self.__recall_num,
                "cos_threshold": self.__cos_threshold,
            }
        )
        return ret.text

    def get_group_video(self, vid, title, uname, createtime, content_teacher_id=0, content_mp3_tagnmae=''):
        try:
            # 有content_teacher、或者content_mp3 通过结构化召回
            ret = {'video': [], 'vec': []}
            if int(content_teacher_id) != 0 or content_mp3_tagnmae != '':
                # print('hello1')
                if int(content_teacher_id) != 0 and content_mp3_tagnmae != '':
                    recall_ret = self.recall(content_teacher_id, content_mp3_tagnmae)
                if int(content_teacher_id) != 0 and content_mp3_tagnmae == '':
                    recall_ret = self.recall_teacher(content_teacher_id)
                if int(content_teacher_id) == 0 and content_mp3_tagnmae != '':
                    recall_ret = self.recall_mp3(content_mp3_tagnmae)
                    print(recall_ret)

                # teacher 或者 mp3 只有一个的情况下，进行曝光量过滤
                if int(content_teacher_id) == 0 or content_mp3_tagnmae == '':
                    # print('hello2')
                    self.__logger.debug('filter by m_dv')
                    recall_ret = self._filter_mdv(recall_ret)

                # 删除召回与原视频重复的列
                img_vid = list(filter(lambda x: x[0] != int(vid) and createtime.startswith('2'), recall_ret))
                # 视频处理的需要过滤掉三星级以下的vid，保留老师的教程
                #video_vid = list(filter(lambda x: x[0] in self.__star_vid, img_vid))
                # video_vid = list(filter(lambda x: x[0] in self.__teach_vid, video_vid))
                # if int(vid) in self.__star_vid and int(vid) in self.__teach_vid:
                # if int(vid) in self.__star_vid and createtime.startswith('2'):
                # if createtime.startswith('2'):
                #     video_vid.append((int(vid), createtime))
                # ret['img'] = img_vid
                ret['video'] = img_vid
            else:
                # 向量化召回, 如果结果有自己，会过滤掉自身
                vec_vid = json.loads(self._recall_vec(str(vid), title, uname, createtime))
                ret['vec'] = vec_vid
            return ret
        except Exception as e:
            self.__logger.error('get recall list error, ' + str(e))
            return []


if __name__ == '__main__':
    pass
    # myrecall = ContentRecall()
    # # time.sleep(10*60)
    #
    # print(myrecall.recall_mp3("后海酒吧"))


