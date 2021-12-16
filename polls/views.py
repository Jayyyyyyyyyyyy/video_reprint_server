# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/4/7 10:33 AM
# @File      : views.py
# @Software  : PyCharm

import json
import logging
import re
import time
from datetime import datetime
import redis
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from .content_recall import ContentRecall
from .get_uname import getuname, get_firstcat
from .img_feature2hdfs import SaveFeature
from .video_cover_extract import get_feature
from .video_reprint import VideoReprint
from .filter import filter_videos, time_format
from .cover_recall import CoverRecall, mp3_compare

hostname = '10.42.158.47'
port = 6379
r = redis.Redis(host=hostname, port=port)

videoprint = VideoReprint()
myrecall = ContentRecall()
save_feature = SaveFeature()
balcklist_uid_set = myrecall._load_blacklist()



whitelist = ["习舞", "合屏", "练习", "演绎", "制作"]



def get_unametitle_flag(uname, title):
    title = title.replace("\n", "")
    title = title.replace("\r", "")
    title = title.replace("\t", " ")
    uname2 = re.sub("[^\u4e00-\u9fa5]", " ", uname)
    uname2 = uname2.strip()
    flaguanme = 0
    uname2list = uname2.strip().split(" ")
    for i, utk in enumerate(uname2list):
        if title.find(utk) >= 0 and len(utk) >= 3 and i == 0:
            flaguanme += 1
        if title.find(utk) >= 0 and len(utk) >= 3 and i == len(uname2list) - 1:
            flaguanme += 1
    if title.find(uname) >= 0 and len(uname) >= 2:
        flaguanme += 1
    if title.find(uname2) >= 0 and len(uname2) >= 2:
        flaguanme += 1
    if title.find(uname[:4]) >= 0 and len(uname) >= 4:
        flaguanme += 1
    if title.find(uname[-4:]) >= 0 and len(uname) >= 4:
        flaguanme += 1
    if title.find(uname2[:4]) >= 0 and len(uname2) >= 4:
        flaguanme += 1
    if title.find(uname2[-4:]) >= 0 and len(uname2) >= 4:
        flaguanme += 1
    return flaguanme

def reprint_strategy(title, uname, content_mp3, content_teacher, content_teach, uid, content_teacher_tagvalue, ctype, water_print_flag):
    title = title.replace("\n", "")
    title = title.replace("\r", "")
    title = title.replace("\t", " ")
    # title 是否包括uname
    flaguanme = get_unametitle_flag(uname, title)
    #pair = content_mp3 + "\t" + content_teacher

    # 文本习舞标记: 需要判断
    xiwu_flag = 0
    for word in whitelist:
        if title.find(word) >= 0:
            xiwu_flag += 1

    # 黑名单 black uid: 需要判断
    uid_flag = 0
    if uid in balcklist_uid_set:
        uid_flag = 1

    # pair 是否在首发原创,可能是盗播或者习舞
    rawpair_flag = 0
    if uid == content_teacher_tagvalue and len(content_teacher) > 0 and len(content_mp3) > 0:
        rawpair_flag = 1

    # 提取的content_teacher 是否是本身uid 发出的，是的情况为正常
    self_flag = 0
    if uid == content_teacher_tagvalue:
        self_flag += 1

    if len(content_teacher)>0 and content_teacher!="unknown":
        if water_print_flag >= 1 and uid_flag == 1 and rawpair_flag == 1:
            return 3
        # 盗播
        elif (water_print_flag >= 1 and uid_flag == 1) or (water_print_flag >= 1 and rawpair_flag == 1) or (
                uid_flag == 1 and rawpair_flag == 1):
            # 如果是表演或者非原创豁免
            if uid_flag == 1 and rawpair_flag == 1 and ((flaguanme > 0 or xiwu_flag > 0 or ctype == 103 or content_teach != "教学")):
               return 0
            else:
                return 3
        else:
            return 0
    else:
        return 0



def time2timestamp(createtime):
    if len(createtime) == 19:
        dateobj = datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S')
        t = dateobj.timetuple()
        timeStamp = int(time.mktime(t))
    else:
        dateobj = datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S.%f')
        t = dateobj.timetuple()
        timeStamp = int(time.mktime(t))
    return timeStamp


def simi_process(vid, video_cands):
    vid, target_url = videoprint.get_urls(vid)
    if target_url == None:
        vid, target_url = videoprint.get_urls2(vid)
    if target_url is not None:
        valid_url = videoprint.is_open(target_url)
        if valid_url is not None:
            videoprint.valid_url(vid, valid_url)
            candidate_info = []
            ### 对哪些视频进行盗播检查，修改这里.后面策略调整，有可能把图片的vid也加上
            for item in video_cands:
                try:
                    item, item_url = videoprint.get_urls(item)
                    if item_url == None:
                        item, item_url = videoprint.get_urls2(item)
                    if item_url is not None:
                        item_url = videoprint.is_open(item_url)
                        if item_url is not None:
                            videoprint.valid_url(item,item_url)
                            video_reprint_list = videoprint.similarity(valid_url, (item, item_url))
                            if video_reprint_list:
                                videoprint.del_encoding_map()
                                return video_reprint_list
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
                except:
                    continue
            videoprint.del_encoding_map()
            return False
        else:
            return False
    else:
        return False


@csrf_exempt
def dup_detector(request):
    if request.method == "POST":
        vid = request.POST.get('vid', '')
        title = request.POST.get('title', '')
        uname = request.POST.get('uname', '')
        uid = request.POST.get('uid', '')
        content_teacher = request.POST.get('content_teacher_tagname', '')
        content_teacher_tagid = request.POST.get('content_teacher_tagid', '')
        content_teacher_tagvalue = request.POST.get('content_teacher_tagvalue', '')
        content_raw = request.POST.get('content_raw_tagname', '')
        content_mp3 = request.POST.get('content_mp3_tagname', '')
        createtime = request.POST.get('createtime', '')
        content_teach = request.POST.get('content_teach_tagname', '')
        talentstar = request.POST.get('talentstar', '')
        ctype = request.POST.get('ctype', '')

    elif request.method == "GET":
        vid = request.GET.get('vid', '')
        title = request.GET.get('title', '')
        uname = request.GET.get('uname', '')
        uid = request.GET.get('uid', '')
        content_teacher = request.GET.get('content_teacher_tagname', '')
        content_teacher_tagid = request.GET.get('content_teacher_tagid', '')
        content_teacher_tagvalue = request.GET.get('content_teacher_tagvalue', '')
        content_raw = request.GET.get('content_raw_tagname', '')
        content_mp3 = request.GET.get('content_mp3_tagname', '')
        createtime = request.GET.get('createtime', '')
        content_teach = request.GET.get('content_teach_tagname', '')
        talentstar = request.GET.get('talentstar', '')
        ctype = request.GET.get('ctype', '')

    try:
        logging.info(
            "Request:vid:{}, title:{}, uname:{}, uid:{}, content_teacher:{}, content_teacher_tagid:{}, content_teacher_tagvalue:{}, content_raw:{}, content_mp3:{}, createtime:{}, content_teach:{}, talentstar:{}, ctype:{}".format(
                vid, title, uname, uid, content_teacher, content_teacher_tagid, content_teacher_tagvalue, content_raw,
                content_mp3, createtime, content_teach, talentstar, ctype))

        # parameters pre-process
        content_teacher = content_teacher.strip()
        content_mp3 = content_mp3.strip()
        start = time.time()
        vid = int(vid)
        try:
            ctype = int(ctype)
        except:
            ctype = -1
        try:
            talentstar = int(talentstar)
        except:
            talentstar = 0
        try:
            uid = int(uid)
        except:
            uid = 0
        try:
            content_teacher_tagvalue = int(content_teacher_tagvalue)
        except:
            content_teacher_tagvalue = 0
        try:
            content_teacher_tagid = int(content_teacher_tagid)
        except:
            content_teacher_tagid = 0

        # 信息补充firstcat
        _, firstcat = get_firstcat(vid)
        if firstcat == None:
            firstcat = 0
        # 过滤不需要检测的视频 module1
        shortcut, responsejson, water_print_flag, target_cover_url, target_video_url = filter_videos(vid, title, uname, ctype, content_teacher,
                                                                                                     content_mp3, talentstar, firstcat)
        if shortcut:
            return HttpResponse(responsejson)

        # 文本召回
        logging.info('content_recall paras: {} {} {}'.format(vid, content_teacher_tagid, content_mp3))
        recall_res_dict = myrecall.get_group_video(vid, title, uname, createtime, content_teacher_tagid, content_mp3)
        logging.info("content_recall results: {}".format(recall_res_dict))
        if recall_res_dict['video']:
            tmp_vids_createtimes = recall_res_dict['video']
        else:
            tmp_vids_createtimes = recall_res_dict['vec']
        # 只保留时间戳小的vid，多数情况用户回溯历史数据
        tmp_vids_createtimes = [x for x in tmp_vids_createtimes if time2timestamp(x[1]) < time2timestamp(createtime)]
        text_reprint_vidlist = [x[0] for x in tmp_vids_createtimes]

        start_feature = time.time()
        feature = get_feature(vid, target_cover_url)
        end_feature = time.time()
        time_feature = time_format(start_feature, end_feature, 'ms')
        logging.info("vid {}, cover feature extracted then search similar video cover or frames, and time: {}".format(vid, time_feature))
        para = json.loads(feature)
        para['embed'] = para['feature']
        del para['feature']
        para['recall_k'] = 10

        flag, topK = CoverRecall(para, vid, content_mp3, target_cover_url, water_print_flag)
        result = {}
        result['reprint'] = {}
        result['vid'] = vid
        result['reprint']['img_logo'] = water_print_flag
        if topK:
            if flag:
                top5 = topK[:5]
                newtop = set([int(str(x).split('_')[0]) for x in top5])
                if text_reprint_vidlist:
                    result['reprint']['text_reprint_flag'] = 1
                    result['reprint']['text_reprint_vidlist'] = text_reprint_vidlist
                else:
                    result['reprint']['text_reprint_flag'] = 0
                    result['reprint']['text_reprint_vidlist'] = text_reprint_vidlist
                if vid in newtop: # 有时候会出现自己盗播自己的情况
                    result['reprint']['img_reprint_flag'] = 0
                    result['reprint']['img_reprint_vidlist'] = []
                    result['reprint']['video_reprint_flag'] = 0
                    result['reprint']['video_reprint_vidlist'] = []
                else:
                    result['reprint']['img_reprint_flag'] = 1
                    result['reprint']['img_reprint_vidlist'] = list(newtop)
                    result['reprint']['video_reprint_flag'] = -2
                    result['reprint']['video_reprint_vidlist'] = []
                end = time.time()
                logging.info("Query spent total time: " + str(end - start) + " seconds")
                responsejson = json.dumps(result, ensure_ascii=False)
                logging.info("Quick Response:" + responsejson)
                return HttpResponse(responsejson)
            else:
                text_reprint_vidlist.extend([int(str(x).split('_')[0]) for x in topK[:5]])

        # deep check 视频内容检测
        result['reprint']['img_reprint_flag'] = 0
        result['reprint']['img_reprint_vidlist'] = []
        result['reprint']['video_reprint_flag'] = 0
        result['reprint']['video_reprint_vidlist'] = []
        if text_reprint_vidlist:
            result['reprint']['text_reprint_flag'] = 1
            result['reprint']['text_reprint_vidlist'] = text_reprint_vidlist
        else:
            result['reprint']['text_reprint_flag'] = 0
            result['reprint']['text_reprint_vidlist'] = text_reprint_vidlist
        video_cands = []
        for vid_ in text_reprint_vidlist:
            try:
                uid_, talentstar_ = getuname(vid_)
                if uid_ != uid and talentstar_>= 4:
                    video_cands.append(vid_)
            except:
                continue
        deep_check = False
        if len(content_mp3)!=0:
            video_cands = mp3_compare(content_mp3, video_cands)
        else:
            pass
        logging.info("video_cands {}".format(video_cands))

        if video_cands:
            logging.info("video_cands for video deep check: {}".format(video_cands))
            res_sim = simi_process(vid, video_cands)
            deep_check = True
            logging.info('res_sim {}'.format(res_sim))
            if res_sim:
                result['reprint']['video_reprint_flag'] = 1
                result['reprint']['video_reprint_vidlist'] = res_sim
        else:
            logging.info('vid: {}, not recall any 3 stars and teach videos'.format(vid))
            pass
        if deep_check and result['reprint']['video_reprint_flag']==0:
            tmp_flag = reprint_strategy(title, uname, content_mp3, content_teacher, content_teach, uid,
                                        content_teacher_tagvalue, ctype, water_print_flag)
            logging.info("get video_reprint_flag from strategy")
            result['reprint']['video_reprint_flag'] = tmp_flag
        end = time.time()
        logging.info("Query spent total time: " + str(end - start) + " seconds")
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info("Deep Response:" + responsejson)
        return HttpResponse(responsejson)
    except Exception as e:
        logging.error("querytag exception: " + e)

