#!/usr/bin/env python3
# coding: utf-8
# packages
import os
import re 

# We need to use below listed elements from config_properties(there might be extra parameters defined in that file)
used_properties_keys = ['outputDBprop','Input_path','SqlConnection','SqlConnection_path','Firefox_win','Firefox_mac','Firefox_Linux','geckodriver_win_path','geckodriver_mac_path','geckodriver_Linux_path','log_path','logfilename_path','binfolder_path','requirements']
def get_config():
    """ Function to read the Config_properties file and have them in form key_value pair in dictionary config_dict """
    try:
        config_dict=dict()     
        ## properties file name cannot be parameterized
        with open('../cfg/conf.txt', "rt") as f:
            for line in f:
                if not line.startswith('#'):
                    l = line.strip()
                    key_value = l.split('=')
                    key = key_value[0].strip()
                    if key != "Firefox_win":
                        key_value = l.replace(' ','').split(key+'=')
                    if key == "Firefox_win":
                        key_value=l.split(key+'=')
                    config_dict[key] = ' '.join(key_value[1:]).strip(' "')
                
            # Only select the keys present in used_properties_keys
            config_dict = {k:(int(v) if v.isnumeric() else v ) for k,v in config_dict.items() if k in used_properties_keys}
            if 'outputDBprop' in config_dict and config_dict['outputDBprop']:
                config_dict['outputDBprop'] = eval(config_dict['outputDBprop'])
            if 'SqlConnection' in config_dict and config_dict['SqlConnection']:
                config_dict['SqlConnection'] = eval(config_dict['SqlConnection'])
            
            f.close()
            return config_dict
    except Exception as e:
        print('Exception in readConfig as:: ', e)


if __name__ == '__main__':
    config = get_config() 
    print('config==', config)