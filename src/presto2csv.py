# C:\lib\Python\Python36 python.exe
# -*- coding:utf-8 -*-
"""
@file       presto2csv.py
@project    hive2es
--------------------------------------
@author     hjw
@date       2018-04-22 20:41
@version    0.0.1.20180422
--------------------------------------
<enter description here>
"""

import sys
import datetime
from pyhive import presto
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import configparser


def read_from_presto(cursor, sql):
    """
    Read data from hive/presto connector
    :param cursor: PyHive hive/presto connector
    :param sql: database sql to read data
    :return: [{cols:values}] list of dictionaries which contains the set of (Column Name: field value)
    """
    results = []
    cursor.execute(sql)
    
    ## get col
    col_schema = [col[0] for col in cursor.description]
    
    for line in cursor.fetchall():
        line_content = {}
        for idx in range(len(col_schema)):
            line_content[col_schema[idx]] = line[idx]
        
        results.append(line_content)
    
    return results



if __name__ == "__main__":
    run()
