#!/usr/bin/env python3
# coding: utf-8
# packages
import pyodbc
import config_reader

def sql_connet(user,password,host):
	cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+host+';UID='+user+';PWD='+password)
	cursor=cnxn.cursor()
	return cnxn,cursor


config = config_reader.get_config()
SqlConnection = config.get("SqlConnection", None)
SqlConnectionPath= config.get("SqlConnection_path", None)
output_DB_prop = config.get("outputDBprop", None)
#DBtablename=config.get("DBtablename", None)
dbname=output_DB_prop.get("dbName")

cnxn,cursor= sql_connet(SqlConnection.get('dbUserName'),SqlConnection.get('dbPassword'),SqlConnection.get('hostName'))

#with open(SqlConnectionPath, "rt") as f:
#	count=0
#	for line in f:
#		l = line.strip()
#		query = l.split(';')
#		query=query[0]
#		if count <=1:
#			query=query.replace("?",dbname)
#		if count ==2:
#			query=query.replace("?",tableName)
#		count = count+1
#		try:
#			cur.execute(query)
#			cnx.commit()
#		except:pass