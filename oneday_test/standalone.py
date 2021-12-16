
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
                if talentstar >=3 and tagid==362:
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
    def valid_url(self, vid, url_string, start_sec=20, end_sec=40):
        # 视频抽帧+提取特征 大约需要11.9秒左右 （20张）
        logging.info("vid:{} and url:{}".format(vid, url_string))
        key = 'video_feature_{}'.format(vid)
        # 如果key在redis中
        if self.r.exists(key):
            feat_list = json.loads(self.r.get(key))
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
                            feat_vec = self.cnn_encoder.encode_image(image_array=image)
                            # save features to redis
                            feature_list.append([new_url_string, feat_vec[0].tolist()])
                            self.encoding_map[new_url_string] = feat_vec[0]
                        # save to redis
                        if eschecker(vid):
                            self.r.set(key, json.dumps(feature_list))
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
        # create candidate url dict for finding vid in last
        cand_dict = {}
        for cand_vid, cand_url in candidates:
            cand_dict[cand_url] = cand_vid
        # count how many pics are duplicated
        def my_counter(list):
            mylist = Counter(list)
            dici = dict(mylist)
            tmp2 = []
            for key in dici.keys():
                tmp2.append((key, dici[key]))
            tmp2 = sorted(tmp2, key=lambda tup: tup[1], reverse=True)
            return tmp2
        # get simi results
        duplicates = self.cnn_encoder.find_duplicates(encoding_map=self.encoding_map, min_similarity_threshold=0.92, scores=True)
        self.encoding_map = {}
        all_dups = []
        for key in duplicates.keys():
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

if __name__ == '__main__':
    videoprint = VideoReprint()
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




    r = redis.Redis(host='10.42.158.47', port=6379)


    def get_urls(vid):
        key = 'url_{}'.format(vid)
        if r.exists(key):
            # 从redis中取到video url
            res = json.loads(r.get(key))
        else:
            return None
        if 'mp4' in res['videourl']:
            if res['videourl'].startswith("/"):
                return 'http://aqiniudl.tangdou.com' + res['videourl']
            else:
                return 'http://aqiniudl.tangdou.com/' + res['videourl']
        else:
            if res['videourl'].startswith("/"):
                video = 'http://aqiniudl.tangdou.com' + res['videourl'] + '-10.mp4'
            else:
                video = 'http://aqiniudl.tangdou.com/' + res['videourl'] + '-10.mp4'
            return  video

    import pickle
    vids_list = pickle.load(open('step_two_res', 'rb'))
    result = {}
    result['profileinfo'] = {}
    count = 0
    index = 0
    with open('./will_update','w' ) as write_file:   # index 7 is wrong, need to review      85开始  25有一正例
        for targets, candidates in vids_list:
            index += 1
            if index > 84:
                break
            candidates = [ (vid, get_urls(vid)) for vid in candidates]
            print("***********")
            print(index)
            print(len(targets))
            print(len(candidates))
            print("-----------")
            for ind, vid in enumerate(targets):
                target_vid = vid
                target_url = get_urls(vid)
                videoprint.valid_url(target_vid, target_url)

                for x in candidates:
                    videoprint.valid_url(x[0], x[1])
                    try:
                        video_reprint_list = videoprint.similarity(target_url, candidates)
                    except:
                        print('error {}'.format(index))
                    if video_reprint_list:
                        result['profileinfo']['video_reprint_flag'] = 1
                        result['profileinfo']['video_reprint_vidlist'] = video_reprint_list
                        result['id'] = vid
                        print(result)
                        write_file.write(json.dumps(result)+'\n')
                        count += 1
                        print('got one ~ {}'.format(count))



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
