#!/bin/sh
ps -ef | grep python | grep runserver | grep 9003 |  awk -F' ' '{print $2}' | xargs kill -9
# add from master