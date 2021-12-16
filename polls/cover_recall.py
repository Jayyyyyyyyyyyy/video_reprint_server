# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2021/10/27 10:33 AM
# @File      : cover_recall.py
# @Software  : PyCharm

'''
图片召回服务处理单元
'''
import json, time
import logging
import requests
from collections import Counter
from requests.adapters import HTTPAdapter
from .filter import time_format, get_urls, get_urls2
from .color_similarity import ColourDistance
from .get_uname import get_content_mp3
from .get_uname import getuname



def clother_color(jsonbody):
    s = requests.Session()
    s.mount('http://', HTTPAdapter(max_retries=3))
    url = "http://10.19.75.189:7788/daobaopose/pose_color"
    resp1 = s.post(url, data=jsonbody, timeout=(30,30))
    return resp1.text


def mp3_compare(content_mp3, vids, flag = 'only_vids'):
    if flag == 'only_vids':
        t_vids = []
        for ori_vid in vids:
            if "_" in str(ori_vid):
                tmp_vid = int(ori_vid.split("_")[0])
            else:
                tmp_vid = int(ori_vid)
            t_content_mp3 = get_content_mp3(tmp_vid)
            if content_mp3 == t_content_mp3:
                t_vids.append(ori_vid)
        return t_vids
    else:
        t_vids = []
        for ori_vid in vids:
            if "_" in ori_vid[0]:
                tmp_vid = int(ori_vid[0].split("_")[0])
            else:
                tmp_vid = int(ori_vid[0])
            t_content_mp3 = get_content_mp3(tmp_vid)
            if content_mp3 == t_content_mp3:
                t_vids.append(ori_vid)
        return t_vids

def CoverRecall(para, vid, content_mp3, target_cover_url, water_print_flag):
    cover_recall = requests.Session()
    cover_recall.mount('http://', HTTPAdapter(max_retries=3))
    cover_recall_res = cover_recall.post(
        "http://10.42.183.53:5012/recall",
        json=para)
    img_sim_res = json.loads(cover_recall_res.text)
    logging.info('img_sim_res: {}'.format(img_sim_res))
    # 对图片召回结果进行content_mp3比对，过滤非同歌曲名的图片
    if len(content_mp3)!= 0:
        recall_img_sim_res = mp3_compare(content_mp3, img_sim_res['recall'],'with_simi')
        # recall_img_sim_res = img_sim_res['recall']
    else: # 没有mp3名，检查更严格
        recall_img_sim_res = img_sim_res['recall']
        recall_img_sim_res = [x for x in recall_img_sim_res if x[1] >= 0.82]
    logging.info('After filter mp3name img_sim_res: {}'.format(recall_img_sim_res))

    have_face = 0
    topK = []
    # 如果图片召回有结果
    if 'recall' in img_sim_res and recall_img_sim_res and water_print_flag == 1:
        filtered = [x for x in recall_img_sim_res if x[1] >= 0.88]
        for _vid, _sim in filtered:
            _vid = int(str(_vid).split('_')[0]) # 召回结果中，视频截图带有帧数，用"_"分隔
            if _vid > vid: # 盗播视频的vid一定大于原创视频的_vid
                continue
            else:
                topK.append(_vid)
        topK = list(set(topK))  # topK 相似度大于0.88 直接保存
        if topK: # 如果topK有值，则直接判定此视频为盗播视频
            logging.info('cover recall find similarity greater than 0.88: {}'.format(topK))
            return (True, topK)
        else:
            # 策略2：如果没找到，说明相似度都小于0.88， 取召回结果的前7个
            logging.info('cover recall doesnt find similarity greater than 0.88')
            new_old_vid = {} #
            most_count = []   # >= 0.77的列表
            most_reprint = [] # >= 0.80的列表
            # 策略4: 如果召回不同vid，同一老师，大概率是盗播

            vid_transform = []
            filtered2 = [x for x in recall_img_sim_res if x[1] >= 0.75]
            for old_vid, sim in filtered2:
                if '_' in old_vid:
                    new_vid, sec = old_vid.split('_')
                    new_vid = int(new_vid)
                else:
                    new_vid = int(old_vid)
                if sim >= 0.70:
                    vid_transform.append(new_vid)
            if len(vid_transform)>=4 and water_print_flag == 1:
                all_diff_vids = list(set(vid_transform))
                logging.info("all_diff_vids is: {}".format(all_diff_vids))
                num_vids = len(all_diff_vids)
                all_uids = [ getuname(x)[0] for x in all_diff_vids if getuname(x)[0] != None]
                count_uids = Counter(all_uids)
                most_common_uid, num_ = count_uids.most_common(1)[0]
                ratio = num_/num_vids
                logging.info("ratio is: {}/{}={}".format(num_, num_vids, ratio))
                if ratio >= 0.6:
                    return (True, all_diff_vids)

            for old_vid, sim in recall_img_sim_res[:7]:
                if '_' in old_vid:
                    new_vid, sec = old_vid.split('_')
                    new_vid = int(new_vid)
                else:
                    new_vid = int(old_vid)
                if sim >= 0.80 and new_vid < vid:
                    most_reprint.append(new_vid)
                if sim >= 0.77 and new_vid < vid:
                    if str(new_vid) not in new_old_vid:
                        new_old_vid[str(new_vid)]=[old_vid]
                    else:
                        new_old_vid[str(new_vid)].append(old_vid)
                    most_count.append(str(new_vid))


            logging.info("count_reprint {}".format(most_reprint))
            # 策略2：如何前7个图片召回结果，相似度大于0.80并且至少有三个相同的vid，则判定为盗播。
            # 若相同，则说明盗播封面图片大概率是从视频中截取的
            count_reprint = Counter(most_reprint)
            ensure_img_reprint = [key for key, count in count_reprint.most_common() if count >= 3]
            logging.info("ensure_img_reprint {}".format(ensure_img_reprint))
            if ensure_img_reprint and water_print_flag == 1:
                logging.info("Quick Response:" + 'responsejson')
                return (True, ensure_img_reprint)

            # 策略3：继续放松条件，如何前7个图片召回结果，相似度大于0.77并且至少有两个相同的vid，则需要进行衣服颜色比对

            cc = Counter(most_count)
            most_left = [key for key, count in cc.most_common() if count >= 2]
            # topK.extend(most_left)  # 加入候选集，用于深度检测
            logging.info("need deep check: strategy 3: {}".format(most_left))
            tmp_filter = []
            for mod_vid in most_left:
                old_vids = new_old_vid[mod_vid]
                tmp_filter.extend(old_vids)# 获取图片贞数
            if tmp_filter:
                logging.info('need clothes color check: {}'.format(tmp_filter))
                parm = {'vid': vid, 'pic': target_cover_url, 'video': '', 'sec': 0, 'flag': 1} # 盗播封面衣服颜色提取
                logging.info('parm: {}'.format(parm))
                start_clother = time.time()
                json_info = clother_color(parm)
                end_clother = time.time()
                time_clother = time_format(start_clother, end_clother, 's')
                logging.info("vid {}, cover get clother color, and time: {}".format(vid, time_clother))
                logging.info('json_info: {}'.format(json_info))
                # try:
                obj = json.loads(json_info)
                have_face = obj['pose_info']['have_pose'] # 获取封面人数
                if int(have_face) > 0: #人数太多则不进行衣服颜色对比
                    clothes = obj['pose_info']['video_clotheres_color']
                    if clothes['tagname'] != '-1':
                        top_color, top_r, top_g, top_b, down_color, down_r, down_g, down_b = [int(x) for x in clothes['tagname'].split('_')]
                        start_cands = time.time()
                        for _vid in tmp_filter:
                            if '_' in _vid: # 视频抽针来检测
                                _vid, sec = _vid.split('_')
                                _vid = int(_vid)

                                _, _, video_url = get_urls(_vid)  # 获取封面url，请求翔龙接口
                                if video_url == None:
                                    _, _, video_url = get_urls2(_vid)  # 获取封面url，从redis
                                parm2 = {'vid': vid, 'pic': '', 'video': video_url, 'sec': sec,
                                         'flag': 2}
                            else: # 直接用图片来检测
                                sec = 0
                                _vid = int(_vid)
                                _, my_img_url, _ = get_urls(_vid)  # 获取封面url，请求翔龙接口
                                if my_img_url == None:
                                    _, my_img_url, _ = get_urls2(_vid)  # 获取封面url，从redis
                                parm2 = {'vid': vid, 'pic': my_img_url, 'video': '', 'sec': sec,
                                         'flag': 1}

                            logging.info('parm2: {}'.format(parm2))
                            json_info2 = clother_color(parm2)
                            logging.info('json_info2: {}'.format(json_info2))
                            # try:
                            obj2 = json.loads(json_info2)
                            have_face2 = obj2['pose_info']['have_pose']

                            if int(have_face2) > 0 and (have_face == have_face2 or have_face > 10): # 人头相同，或者人头数大于11
                                clothes2 = obj2['pose_info']['video_clotheres_color']
                                if clothes2['tagname'] != '-1':
                                    top_color_2, top_r_2, top_g_2, top_b_2, down_color_2, down_r_2, down_g_2, down_b_2 =\
                                        [int(x) for x in clothes2['tagname'].split('_')]
                                    # if top_color == top_color_2 and down_color== down_color_2:
                                    top = ColourDistance(top_r, top_g, top_b, top_r_2, top_g_2, top_b_2)
                                    down = ColourDistance(down_r, down_g, down_b, down_r_2, down_g_2, down_b_2)
                                    logging.info('top: {}, down: {}'.format(top, down))
                                    if (top <= 13 and down <= 13):  # or (top_color == top_color_2 and # 16 maybe
                                        # down_color== down_color_2):
                                        logging.info('added')
                                        topK.append(_vid)
                            else:
                                continue
                        end_cands = time.time()
                        time_cands = time_format(start_cands, end_cands, 's')
                        logging.info("vid {}, multi candicates get clother color, and time: {}".format(vid, time_cands))
                        topK = list(set(topK))
                    else:
                        pass
                else:
                    pass
            else:
                pass
            result = {}
            result['reprint'] = {}
            result['vid'] = vid
            if topK and have_face <= 5:
                return (True, topK)
            else:
                return (False, topK)
    else:
        return (False, topK)