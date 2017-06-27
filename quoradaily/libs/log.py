#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import sys
import time
import logging
import traceback
from logging.handlers import TimedRotatingFileHandler


__inited_loggers = {}
__all__ = ['get_logger']

# 输出的日志编码
CODE = 'utf-8'
# 存放日志文件的相对路径
RELATIVE_PATH = '../../log'


class Logger(logging.Logger):

    def __init__(self, name, level=logging.NOTSET):
        # 同名函数
        self.warn = self.warning
        super(Logger, self).__init__(name, level)

    def _compose_msg(self, *args, **kwargs):
        """组成日志输出的信息, 格式为 key=value

        Args:
            args: 输出在靠前位置的字符串
            kwargs: 输出在靠后位置的 key=value 对

        Returns:
            返回组成完毕的日志信息
        """
        global CODE

        def _parse_value(value):
            """递归处理层级关系
            """
            if isinstance(value, unicode):
                return value.encode(CODE)
            elif isinstance(value, str):
                return value
            elif type(value) in (list, tuple, set):
                items = []
                for item in value:
                    items.append(_parse_value(item))
                return "[{}]".format(",".join(items))
            elif type(value) is dict:
                items = []
                for k, v in value.items():
                    items.append('{}:{}'.format(
                        _parse_value(k),
                        _parse_value(v),
                    ))
                return "{{{}}}".format(",".join(items))
            else:
                return repr(value)

        msg_list = []
        msg_list.extend(map(lambda x: _parse_value(x), args))

        for k, v in kwargs.items():
            # key 只能是字符串
            k = k.encode(CODE) if isinstance(k, unicode) else str(k)
            v = _parse_value(v)
            msg_list.append('{0}={1}'.format(k, v))

        return ', '.join(msg_list)

    def debug(self, *args, **kwargs):
        if self.isEnabledFor(logging.DEBUG):
            msg = self._compose_msg(*args, **kwargs)
            self._log(logging.DEBUG, msg, [])

    def info(self, *args, **kwargs):
        if self.isEnabledFor(logging.INFO):
            msg = self._compose_msg(*args, **kwargs)
            self._log(logging.INFO, msg, [])

    def warning(self, *args, **kwargs):
        if self.isEnabledFor(logging.WARNING):
            msg = self._compose_msg(*args, **kwargs)
            self._log(logging.WARNING, msg, [])

    def error(self, *args, **kwargs):
        if self.isEnabledFor(logging.ERROR):
            msg = self._compose_msg(*args, **kwargs)
            self._log(logging.ERROR, msg, [])

    def critical(self, *args, **kwargs):
        if self.isEnabledFor(logging.CRITICAL):
            msg = self._compose_msg(*args, **kwargs)
            self._log(logging.CRITICAL, msg, [])

    def exc_info(self):
        """获取 excpetion 信息

        Returns:
            返回 exception 信息:
            `file name`, `line number`, `function`, `message`, `exception`
        """
        info = sys.exc_info()
        exc_list = list(traceback.extract_tb(info[2]))
        file, lineno, func, msg = exc_list[-1]
        # get exception
        exc = traceback.format_exc().rstrip()
        exc_index = exc.rfind('\n')
        exc = exc[exc_index + 1:]
        return {'file': file, 'lineno': lineno, 'func': func,
                'msg': msg, 'exc': exc, }


class MultiProcessingTimedRotatingFileHandler(TimedRotatingFileHandler):
    """多进程的时序切分 Handler
    """

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
        dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
        if not os.path.exists(dfn):
            os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            # find the oldest log file and delete it
            for s in self.getFilesToDelete():
                os.remove(s)
        self.mode = 'a'
        self.stream = self._open()
        currentTime = int(time.time())
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and \
                not self.utc:
            dstNow = time.localtime(currentTime)[-1]
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                # DST kicks in before next rollover,
                # so we need to deduct an hour
                if not dstNow:
                    newRolloverAt = newRolloverAt - 3600
                # DST bows out before next rollover, so we need to add an hour
                else:
                    newRolloverAt = newRolloverAt + 3600
        self.rolloverAt = newRolloverAt


def __init_logger(logger_name, log_path, logger_level, **kwargs):
    """初始化

    Args:
        logger_name: 名称. 一个项目中使用一个比较方便
        log_path: 日志文件的路径
        logger_level: 日志级别, 和 logging 一致
        file_level: 文件存储的日志级别
    """
    global __inited_loggers

    # formatter
    fmt = '%(levelname)s %(asctime)s %(filename)s|%(lineno)d\t%(message)s'
    formatter = logging.Formatter(fmt)

    # logger handler
    handler = MultiProcessingTimedRotatingFileHandler(
        log_path,
        when='MIDNIGHT',  # 当地时间零点切分
        interval=1,
        backupCount=0,
    )

    file_level = kwargs.get('file_level')
    if not file_level:
        file_level = logger_level
    logger = Logger(logger_name, logger_level)

    # handler level
    handler.setLevel(file_level)
    # handler format
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logger_level)

    if logger_name in __inited_loggers:
        del __inited_loggers[logger_name]
    __inited_loggers[logger_name] = logger


def get_logger(name='quoradaily', **kwargs):
    """获取指定文件名的 logger

    Args:
        name: logger 名称
        file_level: 日志级别 (可选)
    """
    global __inited_loggers, RELATIVE_PATH

    # 不存在, 初始化一个
    if name not in __inited_loggers:
        # 日志文件的存储根目录
        log_dir = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            RELATIVE_PATH,
        )
        # 路径不存在则创建
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        log_path = os.path.join(
            log_dir,
            '{0}_log'.format(name),  # 文件名格式: {logger 名称}_log
        )
        __init_logger(name, log_path, logging.DEBUG)

    return __inited_loggers[name]
