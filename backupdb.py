import os
import pyodbc

# cmd = 'sqlcmd -S 127.0.0.1 -U sa -P cargosa -d test -b -Q "backup database test to disk=\'D:\\java\\test3.bak\'"'

# os.system(cmd)

# cmd = 'copy G:\\test3.bak  D:\\java\\'

# os.system(cmd)

# cmd = 'sqlcmd -S 127.0.0.1 -U sa -P cargosa -d test -b -Q "restore database testcp from disk=\'D:\\java\\test.bak\' with move \'test\' to \'D:\\sqlserver\\sqlserver2008\\MSSQL10_50.MSSQLSERVER\MSSQL\\DATA\\testcp.mdf\', move \'test_log\' to \'D:\\sqlserver\\sqlserver2008\\MSSQL10_50.MSSQLSERVER\MSSQL\\DATA\\testcp_log.ldf\'"'

# os.system(cmd)

class DbBr(object):
	l = []

	@staticmethod
	def fun():
		db = raw_input('please input backup dbname:')
		DbBr.l.append(db)

	def __init__(self,db):
		try:
			self.db = db
			self.conn = pyodbc.connect('DRIVER={SQL Server Native Client 10.0};SERVER=127.0.0.1;DATABASE=%s;UID=sa;PWD=cargosa'%db)
			self.cursor = self.conn.cursor()
			# print self.conn
			# print self.cursor
		except Exception, e:
			print('conn error')
			raise e
		
	def backup(self):
		try:
			cmd = 'sqlcmd -S 127.0.0.1 -U sa -P cargosa -d %s -b -Q "backup database %s to disk=\'D:\\java\\test3.bak\'"'%(self.db,self.db)
			os.system(cmd)
			# cmd = 'copy G:\\test3.bak  D:\\java\\'
			# os.system(cmd)
		except Exception, e:
			print('backup error')
			raise e
		
	def recovery(self):
		try:
			self.cursor.execute('select name from sysfiles')
			row =  self.cursor.fetchall()
			cmd = 'sqlcmd -S 127.0.0.1 -U sa -P cargosa -d %s -b -Q "restore database %s from disk=\'D:\\java\\test3.bak\' with move \'%s\' to \'D:\\sqlserver\\sqlserver2008\\MSSQL10_50.MSSQLSERVER\MSSQL\\DATA\\testcp.mdf\', move \'%s\' to \'D:\\sqlserver\\sqlserver2008\\MSSQL10_50.MSSQLSERVER\MSSQL\\DATA\\testcp_log.ldf\'"'%(self.db,self.db+'cp',row[0][0],row[1][0])
			os.system(cmd)
		except Exception, e:
			print('recovery error')
			raise e

def main():
	DbBr.fun()
	dbbr = DbBr(DbBr.l[0])
	dbbr.backup()
	dbbr.recovery()

if __name__ == '__main__':
	main()
















