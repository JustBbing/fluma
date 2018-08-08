import json
import traceback
import time
import sys
from datetime import datetime


def try_three_times(call_func):
    def wrapper(*args, **kwargs):
        count_times = 0
        while count_times < 3:
            try:
                res = call_func(*args, **kwargs)
                return res
            except:
                time.sleep(1.5)
                print(traceback.format_exc())
                count_times += 1
                if count_times == 3:
                    print("连续三次出现网络异常")
                    sys.exit(1)
    return wrapper


def turn_to_datetime_obj(time_stamp):
    # 时间戳转换为时间对象
    create_time_num = int(str(time_stamp)[:-3])
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time_num))
    return create_time


def turn_to_timestamp(time_str):
    # 时间对象转为时间戳
    datetime_obj = time.strptime(time_str, '%Y-%m-%d %H:%M:%S')
    time_stamp = time.mktime(datetime_obj)
    return time_stamp


def load_setting_file():
    with open('./setting.json')as file:
        setting_data = json.load(file)
    return setting_data


# print(datetime.strptime('2018-03-11-18-40-59', "%Y-%m-%d-%H-%M-%S"))  # 字符串转换成时间对象

if __name__ == '__main__':
    load_setting_file()
