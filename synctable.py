#coding=utf-8
import pyodbc
import sys

class SyncTable(object):
	l=[]

	@staticmethod
	def info():
		# db = raw_input("please input sync database name:")
		# table = raw_input("please input sync table name:")
		print("--------------此程序是把20上的表同步到172中----------------".decode('utf-8').encode(sys.getfilesystemencoding()))
		db = raw_input("请输入要同步的数据库:".decode('utf-8').encode(sys.getfilesystemencoding()))
		table = raw_input("请输入要同步的表:".decode('utf-8').encode(sys.getfilesystemencoding()))
		SyncTable.l.append(db)
		SyncTable.l.append(table)

	def __init__(self,db,table):
		try:
			self.conn = pyodbc.connect('DRIVER={SQL Server Native Client 10.0};SERVER=10.7.1.172;DATABASE=%s;UID=sa;PWD=cargosa'%db)
			self.cursor = self.conn.cursor()
			self.db = db
			self.table = table
			# self.dsql = "delete from %s"%table
			# self.sql = "select c.name from sys.columns c,sys.tables t where t.name='%s' and t.object_id=c.object_id and c.is_identity<>1"%table
			# self.isql = "insert into %s(%s) select %s from [10.7.1.172].%s.dbo.%s"%(table,c,c,db,
		except Exception, e:
			self.close()
			print("同步出错了".decode('utf-8').encode(sys.getfilesystemencoding()))
			raise(e)

	def run(self,sql):
		 self.cursor.execute(sql)
	
	def close(self):
		self.cursor.close()
		self.conn.close()

	def commit(self):
		self.cursor.commit()

	def rollback(self):
		self.cursor.rollback()

	def start(self):
		col = ''
		csql = "select c.name from sys.columns c,sys.tables t where t.name='%s' and t.object_id=c.object_id and c.is_identity<>1"%self.table
		self.run(csql)
		row = self.cursor.fetchall()
		for i in row:
			col+=i[0]+','
		col=col[:-1] 
		isql = "insert into %s(%s) select %s from [10.7.1.20].%s.dbo.%s"%(self.table,col,col,self.db,self.table)
		dsql = "delete from %s"%self.table
		self.run(dsql)
		self.run(isql)

	def  sync(self):
		try:
			self.start()
			self.commit()
			print("sync successful")
		except Exception, e:
			self.rollback()
			print("sync failure")
		finally:
			self.close()
def main():
	SyncTable.info()
	sync = SyncTable(SyncTable.l[0],SyncTable.l[1])
	sync.sync()
		
if __name__ == '__main__':
	main()



