# -*-- coding:utf-8 -*--
# -*-- coding:utf-8 -*--
from pyspark import SparkConf, SparkContext
import re
import json
from numpy import *


conf = SparkConf()\
    .setMaster("local")\
    .setAppName("My app")\
    .set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

log_file = '/mnt/hgfs/learnC/consumer-2019052911'
data_set = sc.textFile(log_file, 2).cache()

clean_pattern = re.compile(r'^[^{]+')
consumer_name = re.compile(r'Consumer\(([^)]+)\)')


def clean_data(line):
    if len(line) <= 0:
        return [(0, 0, 0)]
    _name = consumer_name.findall(line)
    if len(_name) <= 0:
        return [(0, 0, 0)]

    s = clean_pattern.sub('', line)
    s = s.replace('} []', '}')
    try:
        worlds = json.loads(s)
        process_time = 0
        if 'processTime' in worlds:
            process_time = worlds['processTime']
        return [(_name.pop(), worlds['RoutingKey'], process_time)]
    except Exception as _e_:
        print(_e_)
        return [(0, 0, 0)]


def _map(item):
    queue_name, routing_key, process_time = item

    item = (queue_name, routing_key), {
        'count': 1,
        'process_time': [process_time]
    }
    return item


def reduce_key(a, b):
    if a['process_time'] is None:
        a['process_time'] = list()
    return {
        'count': a['count'] + b['count'],
        'process_time': a['process_time'] + b['process_time'],
    }


def _filter(item):
    a, b = item
    if a == 0:
        return False
    return True


def mid(arr: [int]) -> float:
    n = len(arr)
    if n % 2 == 1:
        return arr[n - 1] / 2.0
    return (arr[n // 2] + arr[(n - 1) // 2]) / 2.0


nums = data_set.flatMap(clean_data) \
    .map(_map)\
    .filter(_filter) \
    .reduceByKey(reduce_key) \
    .collect()

for (key, value) in nums:
    queue, routing = key
    process_times = value['process_time']
    times = msort(process_times)
    print('queue: %s, routing: %s, mid: %f, var: %f, avg: %f, count: %d, (%f, %f)' % (
        queue, routing, mid(times), var(process_times), average(process_times), value['count'],
        times[0], times[-1]
    ))
