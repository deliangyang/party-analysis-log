# -*-- coding:utf-8 -*--
from pyspark import SparkConf, SparkContext
import re
import json

conf = SparkConf().setMaster("local").setAppName("My app").set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)

log_file = '/mnt/hgfs/learnC/exception-2019052911'
data_set = sc.textFile(log_file, 2).cache()

clean_pattern = re.compile(r'^[^{]+')


def clean_data(s):
    s = clean_pattern.sub('', s)
    s = s.replace('} []', '}')
    worlds = json.loads(s)
    return [worlds['error']['message']]


nums = data_set.flatMap(clean_data) \
    .map(lambda key: (key, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collect()
print(nums)

print(sum(map(lambda x: x[1], nums)))


"""
[('model_exception_UserNotInRoomException', 14), ('点歌总数已达上限90', 10), ('Not Found', 13), ('cURL error 28: Operation timed out after 3001 milliseconds with 0 bytes received (see http://curl.haxx.se/libcurl/c/libcurl-errors.html)', 1), ('roomId参数错误', 3), ('model_exception_DuplicateApplicantException', 1), ('model_exception_BeatNotFoundException', 1), ('model_exception_CreatePartyMinUserLevelException', 6), ('roomId參數錯誤', 27), ('api_users_not_exist_exception', 17), ('model_exception_TreasureSnappedException', 2), ('校驗失敗', 62), ('model_exception_PasswordErrorException', 64), ('model_exception_CaptchaCodeException', 1), ('api_InvalidVisitForGuestException', 62), ('model_exception_EmailNeedVerifyException', 5), ('model_exception_TreasureOverdueException', 128), ('该邮箱已注册，是否登录？', 1), ('model_exception_UserNotFoundException', 3), ('长度不能少于2个字符', 1), ('model_exception_HasJoinedException', 3), ('多首模式每人最多点10首歌，请先移除再点歌', 2), ('model_exception_UserHasInMicQueueException', 64), ('model_exception_TreasureHaveException', 50), ('校验失败', 35), ('model_exception_AlreadyOnSeatsException', 1), ('model_exception_InvalidTaskException', 36), ('該郵箱已註冊，是否登入？', 1), ('model_exception_EmailPasswordMismatchException', 4), ('model_exception_EmptyException', 1948), ('model_exception_TreasureCoolingException', 1048)]
"""