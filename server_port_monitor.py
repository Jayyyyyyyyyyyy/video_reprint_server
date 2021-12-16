#-*- encoding:utf-8 -*-
#!/usr/bin/env python

import os
import socket
import time
import smtplib
from email.mime.text import MIMEText

mailto_list=["jiangcx@tangdou.com"] #接收信息的邮箱地址，可以多多个 “，”区别
mail_host="smtp.exmail.qq.com"
mail_user="jiangcx"
mail_postfix="tangdou.com"

def send_mail(to_list, sub, content):
    me="业务平台系统异常"+"<"+mail_user+"@"+mail_postfix+">"
    msg = MIMEText(content)
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    try:
        s = smtplib.SMTP()
        s.connect('smtp.exmail.qq.com')
        s.starttls()
        s.login("jiangcx@tangdou.com", "Ainiaiwo123")
        s.sendmail(me, to_list, msg.as_string())
        s.quit()
        return True
    except:
        return False

def PortCheck(ip,port):
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        s.connect((ip,int(port)))
        s.shutdown(2)
        print('%d is open' % port)
        return True
    except:
        print ('%d is down' % port)
        return False

if __name__ == '__main__':
    while(1>0):
        flag=1
        flag=PortCheck('10.19.67.198',7070)
        #flag = PortCheck('10.42.16.15', 9003)
        print (flag)
        time.sleep(10)
        if flag==False:
            send_mail(mailto_list,"10.19.75.189：7788","error：请求失败")
            time.sleep(3600*6)
        else:
            print('正常')
            time.sleep(600)