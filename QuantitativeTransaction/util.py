import sys
import platform
import time
import os


def get_ts_code(code):
    if code[0] == '0' or code[0] == '3':
        return code + '.SZ'
    elif code[0] == '6':
        return code + '.SH'
    else:
        print("[File: %s line:%d] Invalid code: %s!" % (
        sys._getframe().f_code.co_filename, sys._getframe().f_lineno, code))
        sys.exit()


def transfer_code_as_ts_code(code_list):
    list_length = len(code_list)
    index = 0
    while index < list_length:
        raw_code = code_list[index]
        code_list[index] = get_ts_code(raw_code)
        index = index + 1


def get_delimiter():
    if 'Windows' in platform.system():
        return "\\"
    else:
        return '/'


def get_dir_name_of_report():
    time_format = time.strftime("_%Y_%m_%d", time.localtime())
    dir_name = 'report' + time_format
    return dir_name


def create_report_dir():
    path = get_dir_name_of_report()
    isExists = os.path.exists(path)
    if not isExists:
        os.makedirs(path)