import requests
from astropy.io import votable
import re
from astropy.io.votable.tree import VOTableFile, Resource, Table, Field, Info

import StringIO

__author__ = "teohoch"


class SlapClient(object):
    def __init__(self, slap_service, slap_version=1.0):
        """

        :type slap_version: Double
        :type slap_service: String

        """
        self.__slap_service = slap_service
        self.__slap_version = slap_version

    @classmethod
    def query(cls, service, slap_version=1.0, **kwargs):
        query_params = SlapClient.__parser(slap_version, kwargs)
        if query_params:
            request = requests.get(service, params=query_params)
            return request.text
        else:
            raise ValueError("The query is not valid according to the SLA Protocol")

    @classmethod
    def __parser(cls, slap_version, parameters):

        query_params = {"VERSION": slap_version,
                        "REQUEST": "queryData"}

        for constrain_name, constrain in parameters.iteritems():

            if constrain_name.lower() == "wavelength":
                valid = True

            if isinstance(constrain, dict):  # range
                pass
            elif isinstance(constrain, list):  # list of constrains
                pass
            else:  # equality
                query_params[constrain_name.upper()] = cls.__equality(constrain)
        return query_params if valid else None

    def query_service(self,**kwargs):
        return self.query(self.__slap_service, self.__slap_version, kwargs)

    def query_votable(self, **kwargs):
        result = self.query(kwargs)
        temp = StringIO.StringIO()
        temp.write(result.text)
        return votable.parse(temp)

    def query_fields(self):
        '''
        Returns the fields (columns) returned by the SLAP Service for an standard query
        :return: A list of dictionaries, each one containing the information of one field.
        Common keys of these dictionaries are "name", description, "ID"
        '''
        basic = SlapClient.query(self.__slap_service,self.__slap_version, wavelength=1.0)  # Splatalogue can't deal with wavelenght == 0 XD!
        # Use Regex <FIELD (.*?)>.*?<DESCRIPTION>(.*?)<\/DESCRIPTION>.*?<\/FIELD>
        # https://regex101.com/r/pL7pY1/1
        p = re.compile(ur'<FIELD (.*?)>.*?<DESCRIPTION>(.*?)<\/DESCRIPTION>.*?<\/FIELD>', re.UNICODE | re.DOTALL)

        # Use Regex (.*?)="(.*?)".*?
        # https://regex101.com/r/qW8iX8/1
        p2 = re.compile(ur'(.*?)="(.*?)".*?', re.UNICODE | re.DOTALL)


        result = re.findall(p, basic)
        fields = map(lambda s: [re.findall(p2,s[0]), s[1]],result)
        a = list()
        for i in fields:
            dict_field =(dict((x[0],x[1]) for x in i[0]))
            dict_field.update({"description" : i[1]})
            a.append(dict_field)
            print dict_field
        return a


    @staticmethod
    def __range(params):
        pass

    @staticmethod
    def __equality(params):
        return params

    @staticmethod
    def __list(params):
        pass


if __name__ == "__main__":
    service = "https://find.nrao.edu/splata-slap/slap"
    client = SlapClient(service)
    data = client.query_fields()
    print data
