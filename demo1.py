#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""TODO
#引入线程池
#配置热加载
#脚本平滑重启

"""
import MySQLdb
import threading
import time
import logging
import sys

log = {
        "path"  :   "test.log",
        "once_read_lines"   : 1000,
        "write_thread_nums" : 10,
    }

db = {
        'host'    :     "127.0.0.1",
        'user'    :     "root",
        'passwd'  :     "",
        'db'      :     "dpi",
        'port'    :     3306,
        'charset' :     "utf8",
        'connect_num' : 100,
    }


class DB(object):
    """docstring for DB"""
    __INIT_DB_CHARSET   =   'latin1'
    __INIT_DB_USER      =   'root'
    __INIT_DB_HOST      =   'localhost'
    __INIT_DB_PORT      =   3306

    def __init__(self, config):
        super(DB, self).__init__()
        for key, value in config.items():
            self.__dict__[key] = value
        self.unix_socket = self.__dict__.get("unix_socket", None)
        self.connect_num = int(self.__dict__.get("connect_num", 1))
        self.table_name = 'dpi_test'
        self.connects = []
        self.__create_connect_pool(self.__get_connect_config())
        

    def __get_attr(self, attr, default=""):
        self.__dict__[attr] = self.__dict__.get(attr, default)
        return self.__dict__[attr]

    def __get_connect_config(self):
        connect_config = {
            "user"      :   self.__get_attr("user", self.__INIT_DB_USER),
            "passwd"    :   self.__get_attr("passwd"),
            "charset"   :   self.__get_attr("charset", self.__INIT_DB_CHARSET),
        }
        if self.unix_socket:
            connect_config['unix_socket'] = self.unix_socket
        else:
            connect_config['host'] = self.__get_attr('host', self.__INIT_DB_HOST)
            connect_config['port'] = int(self.__get_attr('port', self.__INIT_DB_PORT))

        if self.__get_attr("db", None):
            connect_config['db'] = self.db

        return connect_config


    def __create_connect_pool(self, config):
        try:
            for i in xrange(self.connect_num):
                self.connects.append(self.__create_connect(config))
        except MySQLdb.Error,e:
            logging.critical("create connect pool error %s" , e.args) 

    def __create_connect(self, connect_config):
        return MySQLdb.connect(**connect_config)


    def insert(self, lines):
        try:
            values = []
            for one in lines:
                values.append(tuple(one.strip().split("|")))

            if len(self.connects) > 0:
                conn = self.connects.pop()
            else:
                logging.warning("not enough connect, now create a new. max is %d", self.connect_num)
                conn = self.__create_connect(self.__get_connect_config())

            self.__realy__insert(conn, values)
        except MySQLdb.Error,e:
            logging.critical("mysql error %s, values %s" , e.args, values)


    def __realy__insert(self, conn, values):
        sql = "INSERT INTO %s values(%s%s)" % (self.table_name, "%s,"*5, "%s")
        cur = conn.cursor()
        cur.executemany(sql, values)
        conn.commit()
        cur.close()
        if len(self.connects) >= self.connect_num:
            conn.close()
        else:
            self.connects.append(conn)        
 
def  work(cls, arg):
    cls.insert(arg)


LOG_FORMAT = '%(levelname)s %(asctime)s [line:%(lineno)d] %(message)s'
LOG_FILE = 'demo.log'  


if __name__ == '__main__': 
    logging.basicConfig(
        level = logging.DEBUG,
        format = LOG_FORMAT,
        filename = LOG_FILE,
        filemode='w'
        )
    logging.info("starting...")
    fp = open(log.get('path'),'r')
    db = DB(db)
    
    '''
    old_hour = time.strftime("%Y%m%d%H",time.localtime())
    if os.path.isfile("offset"):
        @fp1 = open("offset", 'r+')
        offset = fp1.readline().strip() or 0
    else:
        offset = 0
        os.system("touch offset")
    fp.seek(int(offset))
    '''


    while True:
        while threading.activeCount() < log.get('write_thread_nums', 1) + 1:
            lines=[]
            is_write = False
            i = 0
            while True:
                line = fp.readline()
                if len(line) == 0 and i < 10:
                    i = i+1
                    logging.debug("%d [%d] no enough data to read, waiting 1s", i, len(lines))
                    time.sleep(1)
                    continue
                if len(lines) == 0 and i >= 10:
                    logging.info("no enough data reading to exec, yet waited 10s")
                    i = 0
                    continue 
                if len(lines) and i >= 10:
                    is_write = True

                if len(line) != 0:
                    lines.append(line)

                if len(lines) == log.get('once_read_lines',1) or is_write:
                    t = threading.Thread(target=work, args=(db,lines))
                    '''
                    
                    offset = fp.tell()
                    fp1.seek(0)
                    fp1.write(offset + "\n")
                    '''
                    t.start()
                    logging.debug("current thread num is :%d" , threading.activeCount())
                    break
                #自动备份及更新文件
                '''
                hour = time.strftime("%Y%m%d%H",time.localtime())
                if hour > old_hour :
                    fp.close()
                    fp1.seek(0)
                    fp1.write(0)
                    os.rename(log.get('path'), log.get('path') + '.' + str(i))
                    os.system("touch " + log.get('path'))
                    fp = open(log.get('path'),'r')
                    old_hour = hour
                '''

        logging.warning("now thread nums not enough, max is :%d" , threading.activeCount())
        time.sleep(1)


                
            
               
        



