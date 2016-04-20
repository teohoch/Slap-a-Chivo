import requests
from astropy.io import votable
from astropy.io.votable.tree import VOTableFile, Resource, Table, Field, Info

import StringIO

__author__ = "teohoch"


class SlapClient():
	def __init__(self, slap_service, slap_version=1.0):
		"""

		:type slap_version: Double
		:type slap_service: String

		"""
		self.__slap_service = slap_service
		self.__slap_version = slap_version

	def query(self, **kwargs):
		query_params = self.__parser(kwargs)

		if query_params:
			request = requests.get(self.__slap_service, params=query_params)
			return request.text

		else:
			raise ValueError("The query is not valid according to the SLA Protocol")

	def query_votable(self, **kwargs):
		result = self.query(kwargs)
		temp = StringIO.StringIO()
		temp.write(result.text)
		return votable.parse(temp)

	def query_fields(self):
		basic = self.query(WAVELENGTH=1.0)  # Splatalogue can't deal with wavelenght == 0 XD!
		temp = StringIO.StringIO()
		temp.write(basic)
		data = votable.parse(temp)
		print data.params
		result = []
		for field in data.params:
			result.append(field)
		return result



	def __parser(self, parameters):

		query_params = {"VERSION": self.__slap_version,
						"REQUEST": "queryData"}

		for constrain_name, constrain in parameters.iteritems():

			if constrain_name.lower() == "wavelength":
				valid = True

			if isinstance(constrain, dict):  # range
				pass
			elif isinstance(constrain, list):  # list of constrains
				pass
			else:  # equality
				query_params[constrain_name.upper()] = self.__equality(constrain)
		return query_params if valid else None

	def __range(self, params):
		pass

	def __equality(self, params):
		return params

	def __list(self):
		pass


if __name__ == "__main__":
	service = "https://find.nrao.edu/splata-slap/slap"
	client = SlapClient(service)
	data = client.query_fields()
	print data
