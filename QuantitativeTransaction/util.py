import sys
import platform
import time
import os
import data_process as dp
import zipfile as zf
import smtplib
import mimetypes
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

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
    time_format = time.strftime("%Y_%m_%d_", time.localtime())
    if isinstance(period, str):
        return SIGNAL_FILES_DIR + get_delimiter() + time_format + sig_name + '_' + dic_period[period] + '.csv'
    return SIGNAL_FILES_DIR + get_delimiter() + time_format + sig_name + '_' + str(period) + '.csv'


def write_signal_into_csv(df, file_path):
    if os.path.exists(file_path):
        df.to_csv(file_path, mode='a', index=False, float_format=dp.FLOAT_FORMAT2, encoding="utf-8")
    else:
        df.to_csv(file_path, index=False, float_format=dp.FLOAT_FORMAT2, encoding="utf-8")


def create_report_dir():
    isExists = os.path.exists(SIGNAL_FILES_DIR)
    if not isExists:
        os.makedirs(SIGNAL_FILES_DIR)


def save_signal_into_csv(df_data, period, sig_name):
    if len(df_data) > 0:
        file_path = get_signal_file_path(period, sig_name)
        write_signal_into_csv(df_data, file_path)


def save_data_into_data_frame(current_period, PERIOD, df_all, df_current, concerned_stock_code_list):
    if current_period == PERIOD:
        df_current = df_current.reset_index(drop=True)
        if len(df_all) > 0:
            code_current = df_current.at[0, 'code']
            if code_current in concerned_stock_code_list:
                df_all = df_current.append(df_all)
            else:
                df_all = df_all.append(df_current)
            df_all = df_all.reset_index(drop=True)
        else:
            df_all = df_current
    return df_all


def zip_signal_files():
    time_format = time.strftime("%Y_%m_%d_", time.localtime())
    zip_file_path = SIGNAL_FILES_DIR + get_delimiter() + time_format + 'signal.zip'
    current_zip_file_name = time_format + 'signal.zip'
    zp = zf.ZipFile(zip_file_path, 'w', zf.ZIP_DEFLATED)
    for path, dir_names, file_names in os.walk(SIGNAL_FILES_DIR):
        for file_name in file_names:
            if file_name.find(time_format) >= 0 and file_name != current_zip_file_name:
                print("Start to zip file: %s" % file_name)
                file_path = os.path.join(SIGNAL_FILES_DIR, file_name)
                zp.write(file_path)
    zp.close()


def get_signal_zip_file_path():
    time_format = time.strftime("%Y_%m_%d_", time.localtime())
    zip_file_path = SIGNAL_FILES_DIR + get_delimiter() + time_format + 'signal.zip'
    return zip_file_path


def get_signal_zip_file_name():
    time_format = time.strftime("%Y_%m_%d_", time.localtime())
    zip_file_name = time_format + 'signal.zip'
    return zip_file_name


def send_mail(user_name, user_pwd, receiver, title, content, file=None, email_host='smtp.163.com', port=25):
    msg = MIMEMultipart()
    if file:
        data = open(file, 'rb')
        ctype, encoding = mimetypes.guess_type(file)
        if ctype is None or encoding is not None:
            ctype = 'application/octet-stream'
        main_type, sub_type = ctype.split('/', 1)
        attach_zip = MIMEBase(main_type, sub_type)
        attach_zip.set_payload(data.read())
        data.close()
        encoders.encode_base64(attach_zip)
        # att = MIMEText(open(file).read())
        # att["Content-Type"] = 'application/octet-stream'
        # att["Content-Disposition"] = 'attachment; filename="%s"' % file
        file_name = get_signal_zip_file_name()
        attach_zip.add_header('Content-Disposition', 'attachment', filename=file_name)
        msg.attach(attach_zip)
        msg.attach(MIMEText(content, 'plain', 'utf-8'))
        msg['Subject'] = title
        msg['From'] = user_name
        msg['to'] = receiver
        smtp = smtplib.SMTP(email_host, 25)
        smtp.login(user_name, user_pwd)
        try:
            smtp.sendmail(user_name, receiver, msg.as_string())
        except Exception as e:
            print('Send failed with info: ', e)
        else:
            print('Send successful!')

# def adjust_concerned_stock_on_top(df, concerned_stock_code_list):
#     if len(df) > 0 and len(concerned_stock_code_list) > 0:
#         concerned_ts_stock_code_list = get_ts_code(concerned_stock_code_list)
#         for code in concerned_ts_stock_code_list:
#             df_concerned_stock_data = df[(df['code'] == code)]
#             if len(df_concerned_stock_data) > 0:
#                 index_list = df_concerned_stock_data.index.tolist()