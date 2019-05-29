# -*-- coding:utf-8 -*--
from pyspark import SparkConf, SparkContext
import re
import json

conf = SparkConf().setMaster("local").setAppName("My app").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

log_file = '/mnt/hgfs/learnC/producer-2019052911'
data_set = sc.textFile(log_file, 2).cache()

clean_pattern = re.compile(r'^[^{]+')


def clean_data(s):
    s = clean_pattern.sub('', s)
    s = s.replace('} []', '}')
    worlds = json.loads(s)
    return [worlds['messageQueue']['producer']['routeKey']]


nums = data_set.flatMap(clean_data) \
    .map(lambda key: (key, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collect()
print(nums)
