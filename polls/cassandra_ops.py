from cassandra.cluster import Cluster
# from cassandra.cqlengine.management import  create_keyspace_simple
import redis
import json



cluster = Cluster(["10.42.113.183", "10.42.180.245", "10.42.67.246"])
session = cluster.connect()
keyspacename = "reprint_project"
session.set_keyspace(keyspacename)
s = session

def set_feature(key, value):
    if isinstance(value, str):
        value = value.encode('utf-8')
    if isinstance(key, bytes):
        key = key.decode('utf-8')
    s.execute("INSERT INTO video_reprint (key, value) VALUES (%s, %s)", [key, value])

def get_feature(key):
    if isinstance(key, bytes):
        key = key.decode('utf-8')
    rows = s.execute("SELECT * FROM video_reprint where key='%s'" % key)
    if not rows:
        return None
    else:
        return rows[0].value


if __name__ == '__main__':
    hostname = '10.42.158.47'
    port = 6379
    r = redis.Redis(host=hostname, port=port)
    # s.execute("drop table video_reprint")
    # s.execute("CREATE TABLE video_reprint (key text PRIMARY KEY, value blob)")


    keys = r.keys("video_feature_*")
    print(type(keys[0]))
    cc = 0
    fw = open('./keys.txt', "w")
    all = len(keys)
    for key in keys:

        # write key value to cassandra
        value = r.get(key)
        try:
            set_feature(key, value)
        except:
            fw.write(str(key, encoding="utf-8") + "\n")
        cc += 1
        print("{}/{}".format(cc, all))
    fw.close()

# create_keyspace_simple(keyspacename, 3)
# first run open this line, to create keyspace
# session.execute("create keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor': 3};" % keyspacename)
# use keyspace; create a sample table
#
# try:
#     s.execute("drop table video_reprint")
#     s.execute("CREATE TABLE video_reprint (key text PRIMARY KEY, value blob)")
# except:
#     pass
# key = 'video_feature_1500677391276'
# value = r.get(key)
# data = [key, value]
#
# s.execute("INSERT INTO video_reprint (key, value) VALUES (%s, %s)", data)
# results = s.execute("SELECT * FROM video_reprint where key='%s'" % key)
#
# # s.execute("UPDATE list_test set b = b + %s WHERE a = %s", params2)
# # results = s.execute("SELECT * FROM video_reprint")
# print ("********************")
# for x in results:
#     print (x.key, x.value)
#
