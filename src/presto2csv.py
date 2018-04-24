# C:\lib\Python\Python36 python.exe
# -*- coding:utf-8 -*-
"""
@file       presto2csv.py
@project    datageek
--------------------------------------
@author     hjw
@date       2018-04-22 20:41
@version    0.0.1.20180422
--------------------------------------
Executing sql in presto engine and store result into a csv file.
"""

import sys
import os
import datetime
from pyhive import presto
import configparser
import logging
import argparse

## log configuration
logging.basicConfig(filename='../logs/run.log',
                    format='%(asctime)s\t%(filename)s[%(lineno)d]\t%(levelname)s\t%(message)s',
                    datefmt='[%Y-%m-%d %H:%M:%S]',
                    level=logging.INFO)

logger = logging.getLogger("presto2csv")


def presto_connector(host='localhost', port=8089, catalog='elasticsearch', schema='default', *args, **kwargs):
    """
    presto connector API
    :param host:
    :param port:
    :param catalog:
    :param schema:
    :param args:
    :param kwargs:
    :return:
    """
    logger.info("Connecting to PRESTO server [{}:{}/{}/{}]".format(host, port, catalog, schema))
    return presto.connect(host=host, port=port, catalog=catalog, schema=schema, *args, **kwargs).cursor()


def presto_export(cursor, sql, output='csv', **kwargs):
    """
    Read data from hive/presto connector
    :param cursor: PyHive hive/presto connector
    :param sql: database sql to read data
    :param output: output mode, possible values
                'csv':  save result into a csv file, using file_name
                'json': save result into a json file, using file_name
                'dictionary':   return python list[dictionary]
    :param kwargs:
    :return: [{cols:values}] list of dictionaries which contains the set of (Column Name: field value)
    """
    results = []
    logging.info("Executing SQL: ")
    # logging.info("{}".format(sql))
    
    try:
        cursor.execute(sql)
    except:
        logger.error("Error while querying sql. PLEASE CHECK sql...")
        return -1
    
    ## get columns list
    col_schema = [col[0] for col in cursor.description]
    
    if output == 'csv':
        try:
            output_f = open(kwargs['file_name'], 'w', encoding='utf-8')
        except:
            logger.error("Cannot write into file [{}], Please check your csv file...".format(kwargs['file_name']))
            return -1
        
        header = True if 'header' not in kwargs else (kwargs['header'].lower()=='true')
        splitter = ',' if 'splitter' not in kwargs else kwargs['splitter']
        logger.info("Writing result into file [{}], using splitter='{}'...".format(kwargs['file_name'], splitter))
        
        if header:
            ## write the header of csv file
            output_f.write(splitter.join(col_schema) + '\n')
        
        for idx, line in enumerate(cursor.fetchall()):
            line1 = [str(field) if field else 'NULL' for field in line]
            logger.debug("Line content: {}".format(str(line)))
            output_f.write(splitter.join(line1) + '\n')
        
        logger.info('{} lines exported successfully.'.format(str(idx + 1)))
        
        output_f.close()
        
        return 0
    
    elif output == 'list':
        for line in cursor.fetchall():
            line_content = {}
            for idx in range(len(col_schema)):
                line_content[col_schema[idx]] = line[idx]
            
            results.append(line_content)
        
        return results
    
    elif output.lower() == 'json':
        ##TODO: complete json output
        pass


def set_config(config_file="../config/presto.conf"):
    """
    Read configuration from config files
    :param config_file: path of configuration
    :return: configuration settings
    """
    conf = configparser.ConfigParser()
    if not os.path.isfile(config_file):
        logger.warning("Failed to find the configuration file {}.".format(config_file))
        return -1
    
    conf.read(config_file)
    
    presto_conf = {key: conf.get('PRESTO', key) for key in conf.options('PRESTO')}
    sql_query = conf.get('QUERY', 'sql')
    csv_conf = {key: conf.get('CSV', key) for key in conf.options('CSV')}
    
    csv_conf['file_name'] = csv_conf['directory'] + csv_conf['file']
    
    return presto_conf, sql_query, csv_conf


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "-config", type=str, help="配置文件")
    args = parser.parse_args()
    
    config_file = args.c
    
    if not config_file:
        config_file = "../config/presto.conf"
    
    ## read configuration
    presto_conf, sql, output_conf = set_config(config_file)
    
    ## create presto server
    cursor = presto_connector(**presto_conf, username='hive')
    
    ## executing sql
    start_time = datetime.datetime.now()
    presto_export(cursor, sql, output='csv', **output_conf)
    end_time1 = datetime.datetime.now()
    
    logger.info('It takes %.3f seconds to export data into csv.' % ((end_time1 - start_time).microseconds / 1e6))
