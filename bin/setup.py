#!/usr/bin/env python3
# coding: utf-8
# packages
import warnings
warnings.filterwarnings("ignore")
import subprocess
import sys
import os
import config_reader
from pkg_resources import WorkingSet , DistributionNotFound
from setuptools.command.easy_install import main as install

# collecting all the installed packages from system
try:
	reqs = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'])	
except:
	try:
		reqs = subprocess.check_output([sys.executable, '-m', 'pip3', 'freeze'])
	except:print("Please ensure that pip or pip3 is installed on your laptop and redo the setup")
installed_packages = [r.decode().split('==')[0] for r in reqs.split()]

config = config_reader.get_config()
requirements = config.get("requirements", None)
# required packages for executing the program is collecting from ../lib/requirement.txt then installing the package
# requirements=["selenium","pandas","mysql-connector-python","times","datetime","beautifulsoup4"]
packages=[]
with open(requirements, "rt") as f:
	for line in f:
		l = line.strip()
		package = l.split(',')
		package=package[0]
		packages.append(package)

for i in packages:
	if i not in installed_packages:
		working_set = WorkingSet()
		try:
			dep = working_set.require('paramiko>=1.0')
		except DistributionNotFound:
			pass
		whoami=os.getlogin()
		if whoami =='root':
			install([i])
		if whoami !='root':
			try:
				subprocess.check_call(["pip", "install","--user", i])
			except:
				try:
					subprocess.check_call(["pip3", "install","--user", i])
				except:
					print("Check whether this user has admin privileges for installing package")
	
