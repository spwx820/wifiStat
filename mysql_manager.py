# -*- coding: UTF-8 -*-
'''
mysql连接管理类
test
@author: zh
'''
import MySQLdb,traceback,logging

class mmysql:
    '''
    classdocs
    '''

    db = None
    cur = None

    def __init__(self, _options):
        '''
        Constructor
        '''
        if self.db == None:
            self.db = MySQLdb.Connect(host=_options["db_host"] ,user=_options["db_uname"] ,
                         passwd=_options["db_upass"] , port=int(_options["db_port"]) ,
                         db=_options["db_name"] , charset="utf8")
            self.db.autocommit(True)
            self.cur=self.db.cursor()
#             self.cur.execute("set names 'utf8'")
    #filter  
    def F(self,sql):
        try:
            sql = MySQLdb.escape_string(sql)
            return sql
        except :
            print sql
    
    def Q(self,sql):
#         print sql
        return self.query(sql)
    
    
    #事物需要TQ支持 而不是Q
    def TQ(self,sql):
        rs = self.cur.execute(sql)
        return rs
    
    def query(self,sql):
        
        try:
            rs = self.cur.execute(sql)
            return rs
        except MySQLdb.Error,e:
            print sql
            traceback.print_exc()
            logging.error("error: %s"%sql)
            
    def fetchone(self):
        return self.cur.fetchone()
    
    def fetchall(self):
        return self.cur.fetchall()
    
    def close(self):
        try:
            self.cur.close()
            self.db.close()
        except:
            pass
        self.db = None
        
    def __del__(self):
        try:
            if self.db != None:
                self.close()
        except:
            pass
        
    def __exit__(self):
        # 添加对with语句的支持
        try:
            if self.db != None:
                self.close()
        except:
            pass
