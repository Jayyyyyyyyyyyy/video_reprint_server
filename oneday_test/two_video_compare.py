#%%time
import cv2
from imagededup.methods import CNN
import redis
import json
import logging


class VideoReprint(object):
    def __init__(self):
        self.video_capture = cv2.VideoCapture()
        self.encoding_map = {}
        self.cnn_encoder = CNN()
        self.r = redis.Redis(host='10.42.158.47', port=6379)
    def valid_url(self, vid, url_string, start_sec=20, end_sec=40):
        # 视频抽帧+提取特征 大约需要11.9秒左右 （20张）
        key = 'video_feature_{}'.format(vid)
        # 如果key在redis中
        print(key)
        if self.r.exists(key):
            feat_list = json.loads(self.r.get(key))
            for feat in feat_list:
                self.encoding_map[feat[0]] = feat[1]
        else:
            self.video_capture.open(url_string)
            total_frames = self.video_capture.get(7)
            fps = self.video_capture.get(5)
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
                # return 'Error: Cannot open video url: {}'.format(url_string)
    def similarity(self, target_url):
        duplicates = self.cnn_encoder.find_duplicates(encoding_map=self.encoding_map,
                                                 min_similarity_threshold=0.92,
                                                 scores=True)
        print(duplicates)
        self.encoding_map = {}
        cnt = 0
        for key in duplicates.keys():
            pre_fix, suffix = key.split('\t')
            if target_url == pre_fix:
                self_filter = [ele for ele in duplicates[key] if pre_fix not in ele[0]]
                if self_filter:
                    print(key)
                    cnt += 1
        return int(cnt)/20

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
if __name__ == '__main__':
    videoprint = VideoReprint()
    for x in range(1):
        target_vid = 9155399
        target_url = 'http://aqiniudl.tangdou.com/C8A98ADD89F242519C33DC5901307461-10.mp4'
        videoprint.valid_url(target_vid, target_url)
        tmplist = [(9133218, 'http://aqiniudl.tangdou.com/C3BC129D208957BE9C33DC5901307461-10.mp4'),
                   (9133206, 'http://aqiniudl.tangdou.com/9FFBCC8CD25437789C33DC5901307461-10.mp4')
                   ]

        for x in tmplist:
            videoprint.valid_url(x[0], x[1])

        score =videoprint.similarity(target_url)
        print(score)


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
#             logging.info('vid: {} has no valid url {}'.format(target_vid, target_url))
#     else:
#         pass
