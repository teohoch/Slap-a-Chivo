__author__ = 'teohoch'
import elasticsearch
import pprint


primary_host = "http://otto.csrg.inf.utfsm.cl:9200/"
primary_index = "SL-Repository"
primary_mapping = "Spectral-Lines"
support_mapping = "support"


class ElasticRetriever():
    '''
    Class designed to retrieve easily SLAP data from Elasticsearch
    '''

    def __init__(self,host, primary_index, primary_mapping, support_mapping):
        self.__host = host
        self.__primary_index = primary_index.lower()
        self.__primary_mapping = primary_mapping
        self.__support_mapping = support_mapping
        self.__connection = elasticsearch.Elasticsearch(hosts=self.__host)

        self.__max_result_size = self.__connection.count()["count"]

    def query_by_frequency(self,minimum, maximum):
        query = {
            "query": {
                "range": {
                    "Frequency": {
                        "gte": minimum,
                        "lte": maximum
                    }
                }
            },
            "size": self.__max_result_size
        }
        return self.__connection.search(self.__primary_index,self.__primary_mapping,query,size=self.__max_result_size)

    def query_by_wavelenght(self,minimum, maximum):
        query = {
            "query": {
                "range": {
                    "Wavelenght": {
                        "gte": minimum,
                        "lte": maximum
                    }
                }
            },
            "size": self.__max_result_size
        }
        return self.__connection.search(self.__primary_index,self.__primary_mapping,query,size=self.__max_result_size)

pp = pprint.PrettyPrinter(indent=4)
el = ElasticRetriever(primary_host,primary_index,primary_mapping,support_mapping)
r = el.query_by_wavelenght(1,3)
pp.pprint(r["hits"]["total"])
pp.pprint(r)