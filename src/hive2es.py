# -*- coding:utf-8 -*-
"""
@file       hive2es.py
@project    hive2es
--------------------------------------
@author     hjw
@date       2018-03-15 10:27
@version    0.0.1.20180315
--------------------------------------
<enter description here>
"""

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


def create_index(data_schema, es_connector, index_name="default", doc_type_name="system", delete_mode=0):
    """
    Create a new index in Elastic Search Server
    :param data_schema: index mapping setting
    :param es_connector: ES server configuration
    :param index_name: new index name
    :param doc_type_name: doc_type name
    :param delete_mode: if delete_name
    :return:    0:  status success;
                -1: status failed
    """
    if es_connector.indices.exists(index_name) and delete_mode:
        es_connector.indices.delete(index_name)
    
    ## setting mapping structure
    my_mapping = {
        "mappings": {
            doc_type_name: {
                "properties": {col: {"type": "keyword"} for col in data_schema}
            }
        }
    }
    
    es_connector.search(index='1')
    
    if "@timestamp" not in data_schema:
        my_mapping["mappings"][doc_type_name]["properties"]["@timestamp"] = {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss.SSS"
        }
    
    try:
        es_connector.indices.create(index=index_name, body=my_mapping)  # {u'acknowledged': True}
        es_connector.indices.put_mapping(index=index_name, doc_type=doc_type_name, body=my_mapping)
    except:
        print("Index creation failed...")
        return -1
    
    print('Successfully created new index: [%s]' + index_name)
    return 0


def import_into_es(results, data_schema=None, es_connector=Elasticsearch(hosts=["localhost:9200"], timeout=5000)
                   , index_name='default', doc_type_name='system'):
    num_lines = len(results)
    
    if data_schema is None:
        data_schema = results[0].keys()
    
    create_index(data_schema, es_connector, index_name, doc_type_name)
    
    for result in results:
        ## add create timestamp into ES
        if '@timestamp' not in result:
            result['@timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[0:-3]
    
    ACTIONS = [
        {
            "_index": index_name,
            "_type": doc_type_name,
            "_source": result
        } for result in results
    ]
    
    ## import data
    success, _ = bulk(es_connector, ACTIONS, index=index_name, raise_on_error=True)
    print('Performed %d/%d actions.' % (success, num_lines))
    
    if success == num_lines:
        ## all line is imported
        return 0
    else:
        return -1


def set_config(config_file="../config/config.ini"):
    conf = configparser.ConfigParser()
    
    conf.read(config_file)
    
    presto_host = conf.get('PRESTO', 'host')
    presto_port = conf.get('PRESTO', 'port')
    presto_catalog = conf.get('PRESTO', 'catalog')
    presto_schema = conf.get('PRESTO', 'schema')
    presto_sql = conf.get('PRESTO', 'sql')
    
    es_host = conf.get("ElasticSearch", 'host')
    es_port = conf.get("ElasticSearch", 'port')
    es_index = conf.get("ElasticSearch", 'index')
    
    return presto_host, presto_port, presto_catalog, presto_schema, presto_sql, es_host, es_port, es_index


def run():
    start_time = datetime.datetime.now()
    
    ## presto connector
    presto_host, presto_port, presto_catalog, hive_db, sql, es_host, es_port, es_index = set_config()
    
    print("Read data from presto %s:%s/%s in schema %s..." % (presto_host, presto_port, presto_catalog, hive_db))
    print("Using sql = [%s]" % sql)
    
    cursor = presto.connect(host=presto_host, port=presto_port, catalog=presto_catalog,
                            schema=hive_db, username='hive').cursor()
    presto_results = read_from_presto(cursor, sql)
    print("Get %i lines from presto - hive." % len(presto_results))
    
    end_time1 = datetime.datetime.now()
    
    ## Part II: import into es
    print('It takes %.3f seconds to read data from presto on HIVE.' % ((end_time1 - start_time).microseconds / 1e6))
    
    print("Writing data into ES server %s:%s" % (es_host, es_port))
    
    es_server = Elasticsearch(hosts=["%s:%s" % (es_host, es_port)], timeout=5000)
    
    import_into_es(presto_results, es_connector=es_server, index_name=es_index)
    end_time2 = datetime.datetime.now()
    print("It takes %.3f seconds to write data into ES." % ((end_time2 - end_time1).microseconds / 1e6))


if __name__ == "__main__":
    # run()
    
    '''
    ###################################################3
    results = []
    for ts in range(1522108800, 1522281600 + 60 * 5, 60):
        content = {}
        content["clock"] = str(ts)
        content['value'] = "%.3f" % (1 + random.random())
        content['hostid'] = '10000'
        content['ns'] = '100'
        results.append(content)

    ## Part II: import into es
    end_time1 = datetime.datetime.now()
    es_host = '172.31.18.75'
    es_port = 9200
    es_index = 'zabbix_test'
    
    print("Writing data into ES server %s:%s" % (es_host, es_port))

    es_server = Elasticsearch(hosts=["%s:%s" % (es_host, es_port)], timeout=5000)

    import_into_es(results, es_connector=es_server, index_name=es_index)
    end_time2 = datetime.datetime.now()
    print("It takes %.3f seconds to write data into ES." %
        ((end_time2 - end_time1).seconds + (end_time2 - end_time1).microseconds / 1e6))
    
    '''
