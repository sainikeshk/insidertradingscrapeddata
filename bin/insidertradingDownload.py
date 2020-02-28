#!/usr/bin/env python3
# coding: utf-8
# packages
import pandas as pd
import time
import mysql.connector
import re
import datetime as dt
import os
import logging
# config_reader File
import config_reader
import requests
import platform
from datetime import date
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from collections import defaultdict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as chrome_options
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import pyodbc
import csv

loggin_user=os.getlogin()
# get the Logger Created
def create_logger():
    logger = logging.getLogger(__name__)
    if not os.path.exists('../logs'):
        os.makedirs('../logs')
    dt_str = str(dt.datetime.now()).replace(' ', '_' ).replace('-','').replace(':', '').split('.')[0]
    logging.basicConfig(filename='../logs/insidertradinglog'+ dt_str+'.log', filemode='a', format='%(process)d  %(asctime)s %(levelname)s %(funcName)s %(lineno)d ::: %(message)s', level=logging.INFO)
    return logger

# importing from config.properties file
def config_imports(logger):
    try:
        config = config_reader.get_config()
        return config
    except Exception as e:
        logger.exception('ERROR:: Some issue in reading the Config...check config_reader.py script in bin Folder....')
        raise e
def sql_connect(dbName,dbUserName,dbPassword,hostName):
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+hostName+';DATABASE='+dbName+';UID='+dbUserName+';PWD='+dbPassword)
    cursor = cnxn.cursor()
    return cnxn,cursor
def passing_xml_to_beautifulSoup(url,headers):
    res = requests.get(url, headers=headers)
    xml_data = res.text
    soup = BeautifulSoup(xml_data,'xml')
    return soup
def get_filename_from_cd(cd):
    """
    Get filename from content-disposition
    """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]
# WebScraping Code
def parse_xbrl_links(xbrl_links):
    xml_data_list=[]
    for link in xbrl_links[1:]:
        pagedata=defaultdict(list)
        url = link
        pagedata['xbrl']=url
        headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}
        try:
            soup = passing_xml_to_beautifulSoup(url,headers)
            value_list = soup.find_all()
            xml_data_list.append(pagedata)
        except:
            continue
        for tag in value_list:
            if '<in-capmkt:' in str(tag):
                tag_name=tag.name
                key=tag_name
                key = re.sub('([A-Z]{1})', r' \1',key).strip()
                value = tag.get_text()
                
                changed_key={"B S E Scrip Code":"scripcode","N S E Symbol":"nsesymbol","M S E I Symbol":"mseisymbol", "Name Of The Company":"companyname","I S I N":"isin","Regulation Of Insider Trading":"regulationofinsider","Category Of Person":"personcategory","Name Of The Person":"personname","Type Of Instrument":"instrumenttype","Securities Held Prior To Acquisition Or Disposal Number Of Security":"securitynumberheldprior","Securities Held Prior To Acquisition Or Disposal Percentage Of Shareholding":"securitiespercentageheldprior","Securities Acquired Or Disposed Number Of Security":"noofsecurities","Securities Acquired Or Disposed Value Of Security":"securityvalue","Securities Acquired Or Disposed Transaction Type":"securitytype","Securities Held Post Acquistion Or Disposal Number Of Security":"postsecurities","Securities Held Post Acquistion Or Disposal Percentage Of Shareholding":"postsecuritypercentage","Date Of Allotment Advice Or Acquisition Of Shares Or Sale Of Shares Specify From Date":"allotmentadvicedate","Date Of Allotment Advice Or Acquisition Of Shares Or Sale Of Shares Specify To Date":"saleofsharesspecifytodate","Date Of Intimation To Company":"dateofintimation","Mode Of Acquisition Or Disposal":"modeofacquisionordisposal","Exchange On Which The Trade Was Executed":"exchangeonwhichthetradewasexecuted","Value In Aggregate":"valueinaggregate"}
                try:
                    pagedata[str(changed_key[key])] = value
                except:
                    pass    
    df=pd.DataFrame(xml_data_list)
    return df
def Historical_Scraped_data(st_yr,end_yr,cursor,cnxn):
    for yr in range(st_yr,end_yr):
        st_mnth=1
        end_mnth=3
        #c is quarter counter of an year
        c=1
        while st_mnth<=10 and end_mnth<=12:
            if st_mnth<=9 and end_mnth<=9: 
                if end_mnth==3:
                    if yr== int(date.today().year) and end_mnth> int(date.today().month):
                        r = requests.get("https://www.nseindia.com/api/corporates-pit?index=equities&from_date=01-0"+str(st_mnth)+"-"+str(yr)+"&to_date="+str(datetime.now().strftime('%d'))+"-0"+str(date.today().month)+"-"+str(yr)+"&csv=true", headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}, allow_redirects=True)
                        filename= get_filename_from_cd(r.headers.get('content-disposition'))                    
                        if os.path.isfile("../docs/"+filename):
                            print ("File exist")
                        else:
                            os.path.join('../docs', filename)
                            open('../docs/'+filename, 'wb').write(r.content)
                            with open("../docs/"+filename) as csvfile:
                                csvreader = csv.reader(csvfile)
                                xbrl_links=[]
                                for row in csvreader: 
                                    xbrl_links.append(row[-1])
                            print("created original csv file of"+str(yr)+"-q"+str(c))
                    else:
                        r = requests.get("https://www.nseindia.com/api/corporates-pit?index=equities&from_date=01-0"+str(st_mnth)+"-"+str(yr)+"&to_date=31-0"+str(end_mnth)+"-"+str(yr)+"&csv=true", headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}, allow_redirects=True)
                        filename = get_filename_from_cd(r.headers.get('content-disposition'))
                        if os.path.isfile("../docs/"+filename):
                            print ("File exist")
                        else:
                            os.path.join('../docs', filename)
                            open('../docs/'+filename, 'wb').write(r.content)                                    
                            with open("../docs/CF-Insider-Trading-equities-01-0"+str(st_mnth)+"-"+str(yr)+"-to-31-0"+str(end_mnth)+"-"+str(yr)+".csv") as csvfile:
                                csvreader = csv.reader(csvfile)
                                xbrl_links=[]
                                for row in csvreader: 
                                    xbrl_links.append(row[-1])
                            print("created original csv file of"+str(yr)+"-q"+str(c))
                else:
                    if yr== int(date.today().year) and end_mnth> int(date.today().month):
                        r = requests.get("https://www.nseindia.com/api/corporates-pit?index=equities&from_date=01-0"+str(st_mnth)+"-"+str(yr)+"&to_date="+str(datetime.now().strftime('%d'))+"-0"+str(date.today().month)+"-"+str(yr)+"&csv=true", headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}, allow_redirects=True)
                        filename = get_filename_from_cd(r.headers.get('content-disposition'))
                        if os.path.isfile("../docs/"+filename):
                            print ("File exist")
                        else:
                            os.path.join('../docs', filename)
                            open('../docs/'+filename, 'wb').write(r.content)                    
                            with open("../docs/CF-Insider-Trading-equities-01-0"+str(st_mnth)+"-"+str(yr)+"-to-"+str(datetime.now().strftime('%d'))+"-0"+str(date.today().month)+"-"+str(yr)+".csv") as csvfile:
                                csvreader = csv.reader(csvfile)
                                xbrl_links=[]
                                for row in csvreader: 
                                    xbrl_links.append(row[-1])
                            print("created original csv file of"+str(yr)+"-q"+str(c))
                    else:
                        r = requests.get("https://www.nseindia.com/api/corporates-pit?index=equities&from_date=01-0"+str(st_mnth)+"-"+str(yr)+"&to_date=30-0"+str(end_mnth)+"-"+str(yr)+"&csv=true", headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}, allow_redirects=True)
                        filename = get_filename_from_cd(r.headers.get('content-disposition'))
                        if os.path.isfile("../docs/"+filename):
                            print ("File exist")
                        else:
                            os.path.join('../docs', filename)
                            open('../docs/'+filename, 'wb').write(r.content)                    
                            with open("../docs/CF-Insider-Trading-equities-01-0"+str(st_mnth)+"-"+str(yr)+"-to-30-0"+str(end_mnth)+"-"+str(yr)+".csv") as csvfile:
                                csvreader = csv.reader(csvfile)
                                xbrl_links=[]
                                for row in csvreader: 
                                    xbrl_links.append(row[-1])
                            print("created original csv file of"+str(yr)+"-q"+str(c))
            else:
                if yr== int(date.today().year) and end_mnth> int(date.today().month):
                    r = requests.get("https://www.nseindia.com/api/corporates-pit?index=equities&from_date=01-"+str(st_mnth)+"-"+str(yr)+"&to_date="+str(datetime.now().strftime('%d'))+"-0"+str(date.today().month)+"-"+str(yr)+"&csv=true", headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}, allow_redirects=True)
                    filename = get_filename_from_cd(r.headers.get('content-disposition'))
                    if os.path.isfile("../docs/"+filename):
                        print ("File exist")
                    else:
                        os.path.join('../docs', filename)
                        open('../docs/'+filename, 'wb').write(r.content)
                        with open("../docs/CF-Insider-Trading-equities-01-"+str(st_mnth)+"-"+str(yr)+"-to-"+str(datetime.now().strftime('%d'))+"-"+str(date.today().month)+"-"+str(yr)+".csv") as csvfile:
                            csvreader = csv.reader(csvfile)
                            xbrl_links=[]
                            for row in csvreader: 
                                xbrl_links.append(row[-1])
                            print("created original csv file of"+str(yr)+"-q"+str(c))
                else:
                    r = requests.get("https://www.nseindia.com/api/corporates-pit?index=equities&from_date=01-"+str(st_mnth)+"-"+str(yr)+"&to_date=31-"+str(end_mnth)+"-"+str(yr)+"&csv=true", headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}, allow_redirects=True)
                    filename = get_filename_from_cd(r.headers.get('content-disposition'))
                    if os.path.isfile("../docs/"+filename):
                        print ("File exist")
                    else:
                        os.path.join('../docs', filename)
                        open('../docs/'+filename, 'wb').write(r.content)                
                        with open("../docs/CF-Insider-Trading-equities-01-"+str(st_mnth)+"-"+str(yr)+"-to-31-"+str(end_mnth)+"-"+str(yr)+".csv") as csvfile:
                            csvreader = csv.reader(csvfile)
                            xbrl_links=[]
                            for row in csvreader: 
                                xbrl_links.append(row[-1])
                            print("created original csv file of"+str(yr)+"-q"+str(c))
            time.sleep(10)
            if (st_mnth<=9 and end_mnth<=9): 
                if end_mnth==3:
                    if yr== int(date.today().year) and end_mnth> int(date.today().month):
                        table1 = pd.read_csv("../docs/CF-Insider-Trading-equities-01-0"+str(st_mnth)+"-"+str(yr)+"-to-"+str(datetime.now().strftime('%d'))+"-0"+str(date.today().month)+"-"+str(yr)+".csv")
                    else:
                        table1 = pd.read_csv("../docs/CF-Insider-Trading-equities-01-0"+str(st_mnth)+"-"+str(yr)+"-to-31-0"+str(end_mnth)+"-"+str(yr)+".csv")
                else:
                    if yr== int(date.today().year) and end_mnth> int(date.today().month):
                        table1 = pd.read_csv("../docs/CF-Insider-Trading-equities-01-0"+str(st_mnth)+"-"+str(yr)+"-to-"+str(datetime.now().strftime('%d'))+"-0"+str(date.today().month)+"-"+str(yr)+".csv")
                    else:
                        table1 = pd.read_csv("../docs/CF-Insider-Trading-equities-01-0"+str(st_mnth)+"-"+str(yr)+"-to-30-0"+str(end_mnth)+"-"+str(yr)+".csv")
            else:
                if yr== int(date.today().year) and end_mnth> int(date.today().month):                            
                    table1 = pd.read_csv("../docs/CF-Insider-Trading-equities-01-"+str(st_mnth)+"-"+str(yr)+"-to-"+str(datetime.now().strftime('%d'))+"-0"+str(date.today().month)+"-"+str(yr)+".csv")
                else:
                    table1 = pd.read_csv("../docs/CF-Insider-Trading-equities-01-"+str(st_mnth)+"-"+str(yr)+"-to-31-"+str(end_mnth)+"-"+str(yr)+".csv")
            #table1.columns = ['SYMBOL', 'COMPANY', 'REGULATION', 'NAMEOFTHEACQUIRERDISPOSER', 'CATEGORYOFPERSON', 'TYPEOFSECURITYPRIOR', 'NOOFSECURITYPRIOR', 'SHAREHOLDINGPRIOR', 'TYPEOFSECURITYACQUIREDDISPLOSED', 'NOOFSECURITIESACQUIREDDISPLOSED', 'VALUEOFSECURITYACQUIREDDISPLOSED', 'ACQUISITIONDISPOSALTRANSACTIONTYPE', 'TYPEOFSECURITYPOST', 'NOOFSECURITYPOST', 'POST', 'DATEOFALLOTMENTACQUISITIONFROM', 'DATEOFALLOTMENTACQUISITIONTO', 'DATEOFINITMATIONTOCOMPANY', 'MODEOFACQUISITION', 'DERIVATIVETYPESECURITY', 'DERIVATIVECONTRACTSPECIFICATION', 'NOTIONALVALUEBUY', 'NUMBEROFUNITSCONTRACTLOTSIZEBUY', 'NOTIONALVALUESELL', 'NUMBEROFUNITSCONTRACTLOTSIZESELL', 'EXCHANGE', 'REMARK', 'BROADCASTEDATEANDTIME', 'XBRL']
            table1.columns = ['symbol', 'company', 'regulation', 'nameoftheacquirerdisposer', 'categoryofperson', 'typeofsecurityprior', 'noofsecurityprior', 'shareholdingprior', 'typeofsecurityacquireddisplosed', 'noofsecuritiesacquireddisplosed', 'valueofsecurityacquireddisplosed', 'acquisitiondisposaltransactiontype', 'typeofsecuritypost', 'noofsecuritypost', 'post', 'dateofallotmentacquisitionfrom', 'dateofallotmentacquisitionto', 'dateofinitmationtocompany', 'modeofacquisition', 'derivativetypesecurity', 'derivativecontractspecification', 'notionalvaluebuy', 'numberofunitscontractlotsizebuy', 'notionalvaluesell', 'numberofunitscontractlotsizesell', 'exchange', 'remark', 'broadcastedateandtime', 'xbrl']
            #table1 is the original csv table which we have extracted for each quarter
            #link_field is variable which holds XBRL LINKS field
            if os.path.isfile("../docs/xml_file-"+str(yr)+"-q"+str(c)+".csv"):
                print ("File exist")
            else:
                try:        
                    link_field=table1.columns[-1]
                    xml_data_list=[]
                    df=parse_xbrl_links(xbrl_links)
                    df = df.drop([0], axis=0)
                    df.to_csv("../docs/xml_file-"+str(yr)+"-q"+str(c)+".csv",index=0)
                    print("created xml file of"+str(yr)+"-q"+str(c))
                    #table2 is xml file extracted for each quarter
                    table2 = pd.read_csv("../docs/xml_file-"+str(yr)+"-q"+str(c)+".csv")
                except:
                    pass
                try:
                    cmbd=pd.merge(table1,table2,on=link_field).drop(['nsesymbol','regulationofinsider'],axis=1)
                    cmbd.drop_duplicates(keep=False,inplace=True)
                    cmbd.to_csv('../docs/'+str(yr)+'-q'+str(c)+'.csv', index=0)
                    print("created combined file of"+str(yr)+"-q"+str(c))
                except:
                    pass
                #server = 'sainikeshk.database.windows.net'
                #database = 'Insidertrading'
                #username = 'sainikeshk'
                #password = 'maveric1@123'
                #cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
                #cursor = cnxn.cursor()
                #creating table
                try:
                    query="CREATE TABLE combi_table"+str(yr)+"q"+str(c)+"(symbol varchar(255),company varchar(255),regulation varchar(255),nameoftheacquirerdisposer varchar(255),categoryofperson varchar(255),typeofsecurityprior varchar(255),noofsecurityprior varchar(255),shareholdingprior varchar(255),typeofsecurityacquireddisplosed varchar(255),noofsecuritiesacquireddisplosed varchar(255),valueofsecurityacquireddisplosed varchar(255),acquisitiondisposaltransactiontype varchar(255),typeofsecuritypost varchar(255),noofsecuritypost varchar(255),post varchar(255),dateofallotmentacquisitionfrom varchar(255),dateofallotmentacquisitionto varchar(255),dateofinitmationtocompany varchar(255),modeofacquisition varchar(255),derivativetypesecurity varchar(255),derivativecontractspecification varchar(255),notionalvaluebuy varchar(255),numberofunitscontractlotsizebuy varchar(255),notionalvaluesell varchar(255),numberofunitscontractlotsizesell varchar(255),exchange varchar(255),remark varchar(255),broadcastedateandtime varchar(255),xbrl varchar(255),scripcode varchar(255),mseisymbol varchar(255),companyname varchar(255),isin varchar(255),personcategory varchar(255),personname varchar(255),instrumenttype varchar(255),securitynumberheldprior varchar(255),securitiespercentageheldprior varchar(255),noofsecurities varchar(255),securityvalue varchar(255),securitytype varchar(255),postsecurities varchar(255),postsecuritypercentage varchar(255),allotmentadvicedate varchar(255),saleofsharesspecifytodate varchar(255),dateofintimation varchar(255),modeofacquisionordisposal varchar(255),exchangeonwhichthetradewasexecuted varchar(255),valueinaggregate varchar(255))"
                    cursor.execute(query)
                    cnxn.commit()
                except:
                    pass
                try:
                    with open ('../docs/'+str(yr)+'-q'+str(c)+'.csv', 'r') as f:
                        reader = csv.reader(f)
                        columns = next(reader)
                        query = "insert into combi_table"+str(yr)+"q"+str(c)+"({0}) values ({1})"
                        query = query.format(','.join(columns), ','.join('?' * len(columns)))
                        #cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
                        #cursor = cnxn.cursor()
                        for data in reader:
                            cursor.execute(query, data)
                        cnxn.commit()
                        print("sucessfully created combi_table"+str(yr)+"q"+str(c))
                except:
                    pass
            time.sleep(3)
            if(yr==int(date.today().year) and end_mnth>int(date.today().month)):
                end_mnth=15
            c+=1
            st_mnth+=3
            end_mnth+=3
def Current_Scraped_data(cursor,cnxn):
    r = requests.get("https://www.nseindia.com/api/corporates-pit?index=equities&csv=true", headers ={'user-agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"}, allow_redirects=True)
    filename = get_filename_from_cd(r.headers.get('content-disposition'))
    print(filename)
    os.path.join('../docs', filename)
    open('../docs/'+filename, 'wb').write(r.content)
    with open("../docs/"+filename) as csvfile:
        csvreader = csv.reader(csvfile)
        xbrl_links=[]
        for row in csvreader:
            xbrl_links.append(row[-1])
    df=parse_xbrl_links(xbrl_links)
    df = df.drop([0], axis=0)
    df.to_csv("../docs/xmlfile.csv",index=0)
    data1 = pd.read_csv("../docs/"+filename)
    data1.columns= ['symbol', 'company', 'regulation', 'nameoftheacquirerdisposer', 'categoryofperson', 'typeofsecurityprior', 'noofsecurityprior', 'shareholdingprior', 'typeofsecurityacquireddisplosed', 'noofsecuritiesacquireddisplosed', 'valueofsecurityacquireddisplosed', 'acquisitiondisposaltransactiontype', 'typeofsecuritypost', 'noofsecuritypost', 'post', 'dateofallotmentacquisitionfrom', 'dateofallotmentacquisitionto', 'dateofinitmationtocompany', 'modeofacquisition', 'derivativetypesecurity', 'derivativecontractspecification', 'notionalvaluebuy', 'numberofunitscontractlotsizebuy', 'notionalvaluesell', 'numberofunitscontractlotsizesell', 'exchange', 'remark', 'broadcastedateandtime', 'xbrl']
    data2 = pd.read_csv("../docs/xmlfile.csv")
    cmbd=pd.merge(data1, data2,on="xbrl")
    cmbd.to_csv('../docs/combin.csv', index=0)
    query="CREATE TABLE today_table(symbol varchar(255),company varchar(255),regulation varchar(255),nameofacquision varchar(255),typeofsecurity varchar(255),noofsecurity varchar(255),acquision varchar(255),broadcastdatetime varchar(255),xbrl varchar(255),scripcode varchar(255),nsesymbol varchar(255),mseisymbol varchar(255),companyname varchar(255),isin varchar(255),regulationofinsider varchar(255),personcategory varchar(255),personname varchar(255),instrumenttype varchar(255),securitynumberheldprior varchar(255),securitiespercentageheldprior varchar(255),noofsecurities varchar(255),securityvalue varchar(255),securitytype varchar(255),postsecurities varchar(255),postsecuritypercentage varchar(255),allotmentadvicedate varchar(255),saleofsharesspecifytodate varchar(255),dateofintimation varchar(255),modeofacquisionordisposal varchar(255),exchangeonwhichthetradewasexecuted varchar(255),valueinaggregate varchar(255))"
    cursor.execute(query)
    cnxn.commit()
    with open ('../docs/combin.csv', 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)
        query = 'insert into today_table({0}) values ({1})'
        query = query.format(','.join(columns), ','.join('?' * len(columns)))
            #cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            #cursor = cnxn.cursor()
        for data in reader:
            cursor.execute(query, data)
        cnxn.commit()
    
def main():
    logger = create_logger()
    config = config_imports(logger)
    logger.info('Config == %s', config)
    output_DB_prop = config.get("outputDBprop", None)
    Input_path= config.get("Input_path", None)
    whoami=platform.system()
    st_yr=2014
    end_yr=int(date.today().year)+1
    cnxn,cursor = sql_connect(output_DB_prop.get('dbName'),output_DB_prop.get('dbUserName'),output_DB_prop.get('dbPassword'),output_DB_prop.get('hostName'))
    Historical_Scraped_data(st_yr,end_yr,cursor,cnxn)
    time.sleep(2)
    Current_Scraped_data(cursor,cnxn)
if __name__ == "__main__":
        main()

              




    















