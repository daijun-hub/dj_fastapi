# !/usr/bin/python
# coding: utf8

import base64
import copy
import hashlib
import hmac
import itertools
import json
import random
import time
import datetime
import os
from json import JSONEncoder, JSONDecoder
from json.decoder import WHITESPACE
from bson import ObjectId, _millis_to_datetime
from bson.codec_options import DEFAULT_CODEC_OPTIONS
from encoder import XML2Dict
from basedoc import ModelBase
from commons.Helpers.Helper_validate import Validate, RegType

import settings

# 解析JSON特殊数据类型
from caches.redis_utils import AsyncRedisCache as RedisCache


class FrontendJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.strftime(settings.TIME_FORMAT)
        elif isinstance(obj, ModelBase):
            return obj.to_dict()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')

        return JSONEncoder.default(self, obj)

    def encode(self, obj):
        obj = {
            k.decode('utf-8') if isinstance(k, (bytes, bytearray)) else k:
                json.loads(v, cls=ComplexJsonDecoder) if isinstance(
                    v, (bytes, bytearray)) else v
            for k, v in obj.items()
        }
        return super(FrontendJsonEncoder, self).encode(obj)


class ComplexJsonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(obj, ModelBase):
            return obj.to_dict()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')

        return JSONEncoder.default(self, obj)

    def encode(self, obj):
        obj = {
            k.decode('utf-8') if isinstance(k, (bytes, bytearray)) else k:
                json.loads(v, cls=ComplexJsonDecoder) if isinstance(
                    v, (bytes, bytearray)) else v
            for k, v in obj.items()
        }
        return super(ComplexJsonEncoder, self).encode(obj)


class ComplexJsonDecoder(JSONDecoder):
    def decode(self, s, _w=WHITESPACE.match):
        try:
            if isinstance(s, (str, bytes, bytearray)):
                s = s.replace('\'', '"').replace('None', 'null')
            return super(ComplexJsonDecoder, self).decode(s)
        except ValueError:
            pass
        return s


def string(b_str: bytes):
    if isinstance(b_str, bytes):
        return b_str.decode('utf-8')
    return str(b_str)


def strip_tags(html):
    """
    删除文本中所有的HTML标签
    :param html: 文本
    :return:
    """
    return Validate.clear(html, reg_type=RegType.ALL_HTML_ATG)


def md5(a_string):
    """
    生成MD5
    :param a_string: 对照
    :return:
    """
    if not isinstance(a_string, (bytes, str)):
        pass
    if isinstance(a_string, str):
        a_string = a_string.encode('utf-8')
    return hashlib.md5(a_string).hexdigest()


def h_mac(key, msg=None, digestmod=None):
    if isinstance(key, str):
        key = key.encode('utf-8')
    if isinstance(msg, str):
        msg = msg.encode('utf-8')
    if isinstance(digestmod, str):
        digestmod = digestmod.encode('utf-8')

    return hmac.new(key, msg, digestmod).hexdigest()


def sha1(a_string):
    if isinstance(a_string, str):
        a_string = a_string.encode('utf-8')
    return hashlib.sha1(a_string).hexdigest()


def sha256(a_string):
    if isinstance(a_string, str):
        a_string = a_string.encode('utf-8')
    return hashlib.sha256(a_string).hexdigest()


def base64encode(value):
    """
    给予Base64对值进行加密
    :param value: 传入值
    :return: 加密结果
    """
    if value:
        if isinstance(value, str):
            value = value.encode('utf-8')
        return base64.b64encode(value).decode('utf-8')
    return None


def base64decode(value):
    """
    给予Base64对值进行解密
    :param value: 传入已加密值
    :return: 解密结果
    """
    if value:
        if isinstance(value, str):
            value = value.encode('utf-8')
        return base64.b64decode(value).decode('utf-8')
    return None


def str2datetime(datetime_str, date_format=settings.TIME_FORMAT):
    """
    日期字符串转换成日期
    :param datetime_str: 日期字符串
    :param date_format: 日期格式
    :return: 日期
    """
    if datetime_str:
        return datetime.datetime.strptime(datetime_str, date_format)
    return None


def datetime2str(date_time, date_format=settings.TIME_FORMAT):
    """
    日期转换成字符串
    :param date_time: 日期
    :param date_format: 日期格式
    :return: 日期字符串
    """
    if date_time:
        return date_time.strftime(date_format)
    return None


def datetime2timestamp(date_time):
    """
    日期转换成时间戳
    :param date_time: 日期
    :return: 时间戳
    """
    if date_time:
        return time.mktime(time.strptime(date_time.strftime(settings.TIME_FORMAT), settings.TIME_FORMAT))
    return None


def timestamp2datetime(timestamp):
    """
    时间戳转换成日期
    :param timestamp: 时间戳
    :return: 日期
    """
    if timestamp:
        return datetime.datetime.strptime(time.strftime(settings.TIME_FORMAT, time.localtime(timestamp)),
                                          settings.TIME_FORMAT)
    return None


def timestamp2str(timestamp, date_format=settings.TIME_FORMAT):
    """
    时间戳转换成字符串
    :param timestamp:
    :param date_format:
    :return:
    """
    if timestamp:
        return datetime2str(timestamp2datetime(timestamp), date_format)
    return None


def str2timestamp(datetime_str, date_format=settings.TIME_FORMAT):
    """
    字符串转换成时间戳
    :param datetime_str:
    :param date_format:
    :return:
    """
    if datetime_str:
        return datetime2timestamp(str2datetime(datetime_str, date_format))
    return None


def read_json_resource(json_file):
    """
    读取资源文件JSON
    :param json_file: 文件名
    :return:
    """
    if not isinstance(json_file, (bytes, str)):
        raise ValueError(
            'Parameter json_file must be not None and type of str or unicode.')
    json_path = os.path.join(settings.SITE_ROOT, 'res', json_file)
    if os.path.exists(json_path):
        with open(json_path, 'rt', encoding='utf-8') as jf:
            json_s = jf.read()
            menu_dict = json.loads(json_s)
            if menu_dict:
                return json.loads(json_s)
    return None


def write_file_by_meta(file_meta, file_path, rename=True, extension_list=None):
    """

    :param file_meta: 上传的元文件信息
    :param file_path: 需要保存的文件位置
    :param rename: 是否重命名
    :param extension_list: 后缀名检查
    :return:
    """
    res = dict(code=0)
    buffers = []
    full_file_name = file_meta['filename']
    extension = os.path.splitext(full_file_name)[-1]
    file_name = ''.join(os.path.splitext(full_file_name)[:-1])
    if extension_list is not None:
        # 校验文件扩展名
        if extension not in extension_list:
            res['code'] = -1
    else:
        # 重命名文件名
        if rename:
            file_name = md5('%s%s' % (
                file_name, datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))).upper()

        current_file_path = os.path.join(
            file_path, '%s%s' % (file_name, extension))
        buffers.append(file_meta['body'])
        if not os.path.exists(file_path):
            os.mkdir(file_path)

        with open(current_file_path, 'wb') as f:
            f.write(b''.join(buffers))

        res['code'] = 1
        res['file_name'] = '%s%s' % (file_name, extension)
    return res


def get_random_str(size=32):
    """
    获取定长随机字符串
    :param size: 结果长度
    :return: 随机字符串
    """
    seed_str = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ=_-+<>!@#$%^&*()[]{}|\/?'
    return ''.join(random.choices(seed_str, k=size))


def get_file_extension_name(file_name=''):
    """
    获取文件扩展名
    :param file_name: 文件名或路径
    :return:
    """
    return os.path.splitext(file_name)[1]


async def drop_disk_file(path):
    if os.path.exists(path):
        os.remove(path)


def xml2dict(xml):
    """
    将xml转换成dict
    :param xml: xml格式字符串
    :return:
    """
    return XML2Dict().parse(xml).get('xml')


async def get_increase_code(key, begin=100000000):
    """
    获取增长的code键值
    :param key:
    :param begin:
    :return:
    """
    value = await RedisCache.get(key)
    if value:
        value = int(value) + 1
    else:
        value = begin
    await RedisCache.set(key, value)
    return str(value)


def float2str(value):
    if isinstance(value, float):
        return str(int(value))
    return value


def combination(*elements) -> list:
    """
    交叉组合所有数组
    :param elements:
    :return:
    """
    result_list = []
    # 一次组合
    permuted_list = []
    for element in elements:
        permuted_list.append(
            [list(result) for result in itertools.permutations(element, len(element))])
    # 二次组合
    twice_permuted_list = list(itertools.permutations(
        permuted_list, len(permuted_list)))
    for permuted_list in twice_permuted_list:
        tmp_list = []
        length = len(permuted_list)
        if length > 1:
            index = 1
            while True:
                if index == 1:
                    tmp_list = combined_two_list(
                        permuted_list[index - 1], permuted_list[index])
                else:
                    tmp_list = combined_two_list(
                        tmp_list, permuted_list[index])
                index += 1
                if index >= length:
                    break
        elif length == 1:
            tmp_list = (permuted_list[0],)

        if tmp_list:
            result_list.extend(tmp_list)

    return result_list


def combined_two_list(first_list: list, second_list: list):
    """
    交叉两个数组
    :param first_list: 数组一
    :param second_list: 数组二
    :return:
    """
    result_list = []
    if first_list and second_list:
        for first in first_list:
            for second in second_list:
                if isinstance(first, tuple):
                    # print(first, second)
                    tmp = list(copy.deepcopy(first))
                    tmp.append(second)
                    result_list.append(tmp)
                else:
                    result_list.append((first, second))
    # print(result_list)
    return result_list


def get_dir_path(path, *paths):
    dir_path = os.path.join(path, *paths)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def get_download_path(path, request_info):
    if settings.DOWNLOAD_ENV == 'SERVER':
        return path.replace(os.path.join(settings.SITE_ROOT, "static"),
                            f"{request_info.protocol}://{request_info.host}/{settings._STATIC_PATH}")
    else:
        return path


def to_str(obj, encode='utf8'):
    if isinstance(obj, bytes):
        try:
            return obj.decode(encode)
        except:
            return obj
    elif isinstance(obj, (tuple, list)):
        return obj.__class__([to_str(o) for o in obj])
    elif isinstance(obj, dict):
        return obj.__class__([(to_str(k), to_str(v)) for k, v in obj.items()])
    return obj


def int_or_none(maybe_number):
    if maybe_number is None:
        return None
    else:
        return int(maybe_number)


def model_object_hook(dic):
    """
    外键的格式化，统一解析成string
    :param dic:
    :return:
    """
    if dic.get('$oid'):
        return dic['$oid']
    elif dic.get('$date'):
        return _millis_to_datetime(dic['$date'], DEFAULT_CODEC_OPTIONS).strftime(settings.TIME_FORMAT)
    return dic
