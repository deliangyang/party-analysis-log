# -*-- coding:utf-8 -*--
from pyspark import SparkConf, SparkContext
import re
import json
from collections import Counter


conf = SparkConf().setMaster("local").setAppName("My app").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

log_file = '/mnt/hgfs/learnC/request-2019052911'
data_set = sc.textFile(log_file, 2).cache()

clean_pattern = re.compile(r'^[^{]+')
last_pattern = re.compile(r'\s+[{\[].+$')

content = re.compile(r'((\d+\.\d+\.\d+\.\d+)\s+(\d+)\s+(\w+)\s+([\w/]+)\s+{)')


def clean_data(s):
    item = content.findall(s)
    if len(item) <= 0:
        return [('', 0, '', '', '', '')]
    _, ip, user_id, method, url = item[0]

    s = clean_pattern.sub('', s)
    s = last_pattern.sub('', s)
    #print(s)
    worlds = json.loads(s)
    #print(worlds)
    lang = None,
    client_os = None
    if 'cli-app-lang' in worlds:
        lang = worlds['cli-app-lang']
    if 'cli-os' in worlds:
        client_os = worlds['cli-os']
    return [(ip, user_id, method, url, client_os, lang)]


def _filter(x):
    if x is False:
        return False
    return True


nums = data_set.flatMap(clean_data) \
    .filter(_filter) \
    .map(lambda key: (key, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collect()
print(nums)

print(sum(map(lambda x: x[1], nums)))
print(len(nums))

ips = Counter()

for item in nums:
    key, value = item
    ip, user_id, method, url, client_os, lang = key
    if ip in ips:
        ips[ip] = value
    else:
        ips[ip] += value

ips = sorted(ips.items(), key=lambda x: x[0], reverse=False)
print(ips)
