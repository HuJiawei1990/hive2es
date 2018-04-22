# -*- coding:utf-8 -*-
"""
@file       filter_data.py
@project    hive2es
--------------------------------------
@author     hjw
@date       2018-04-04 15:19
@version    0.0.1.20180404
--------------------------------------
<enter description here>
"""

import sys
import time


def run():
    file_path = "../data/tczf.csv"
    output_path = '../output/tczf'
    data = []
    
    ip_list = []
    with open(file_path, 'r', encoding='utf-8') as f:
        col_names = []
        for idx, line in enumerate(f):
            
            ## read schemas
            if idx == 0:
                col_names = list(line.strip("\n\r").split('\t'))
                continue
            
            ## read content
            line_list = list(line.strip("\n\r").split('\t'))
            
            line_dict = {col_name: line_list[i] for i, col_name in enumerate(col_names)}
            
            ## create timestamp column
            if 'timestamp' not in col_names:
                datex_time = line_dict['datex'] + ' ' + line_dict['time_str']
                line_dict['timestamp'] = int(time.mktime(time.strptime(datex_time, '%Y-%m-%d %H:%M:%S')))
            else:
                line_dict['timestamp'] = int(line_dict['timestamp'])
            
            if 'host_ip' in col_names:
                if line_dict['host_ip'] not in ip_list:
                    ip_list.append(line_dict['host_ip'])
            
            data.append(line_dict)
    
    data = sorted(data, key=lambda x: x['timestamp'])
    
    if ip_list:
        file_list = {ip: open(output_path + ip, 'w') for ip in ip_list}
        for line in data:
            file_list[line['host_ip']].write(
                '%s,%.2f,%s,%s,%s\n' % (line['timestamp'], float(line['disk']), line['disk_max'], line['disk_min'],
                                        line['host_ip']))
        
        for file in file_list.values():
            file.close()
    
    else:
        file = open(output_path, 'w')
        for line in data:
            file.write('%s,%s,%s,%s,%s\n' % (line['timestamp'], line['ct_trans'], line['ct_fail'], line['rate_success'],
                                             line['avg_cost']))


if __name__ == "__main__":
    run()
