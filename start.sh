#!/bin/sh
rm ./nohupout.log
nohup python  manage.py runserver 0.0.0.0:9003  --noreload  >>nohupout.log 2>&1  &

# this update from server
#this update from test branch
# add more from test branch
