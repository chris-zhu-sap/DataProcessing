import sys
import platform
import time
import os
import data_process as dp

SIGNAL_FILES_DIR = 'trade-signal-file'

dic_period = {
    'M': 'month',
    'D': 'day',
    'W': 'week'
}


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


def get_signal_file_path(period, sig_name):
    time_format = time.strftime("_%Y_%m_%d", time.localtime())
    file_path = SIGNAL_FILES_DIR + get_delimiter() + sig_name + '_' + dic_period[period] + time_format + '.csv'
    return file_path


def write_signal_into_csv(df, file_path):
    if os.path.exists(file_path):
        df.to_csv(file_path, mode='a', index=False, float_format=dp.FLOAT_FORMAT2, encoding="utf-8")
    else:
        df.to_csv(file_path, index=False, float_format=dp.FLOAT_FORMAT2, encoding="utf-8")


def create_report_dir():
    isExists = os.path.exists(SIGNAL_FILES_DIR)
    if not isExists:
        os.makedirs(SIGNAL_FILES_DIR)