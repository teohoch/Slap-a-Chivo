import requests
from astropy.io import votable
from astropy.io.votable.tree import VOTableFile, Resource, Table, Field, Info

import StringIO
__author__ = "teohoch"


class SlapClient():

	def __init__(self, slap_service, slap_version = 1.0):
		"""

		:type slap_version: Double
		:type slap_service: String

		"""
		self.__slap_service = slap_service
		self.__slap_version = slap_version

	def query(self, **kwargs):
		query_params = {#"VERSION"	:	self.__slap_version,
						"REQUEST"	:	"queryData"}
		#if "find.nrao.edu" in self.__slap_service:
		#	query_params["VERB"] = 3
		valid = False

		for constrain_name, constrain in kwargs.iteritems():
			if constrain_name.lower() == "wavelength":
				valid = True

			if isinstance(constrain, dict): #range
				pass
			elif isinstance(constrain, list): #list of constrains
				pass
			else: #equality
				query_params[constrain_name.upper()] = self.__equality(constrain)
		if valid:
			request = requests.get(self.__slap_service, params=query_params)
			temp = StringIO.StringIO()
			temp.write(request.text)
			return votable.parse(temp)
		else:
			raise ValueError("The query is not valid according to the SLA Protocol")

	def __range(self, params):
		pass

	def __equality(self, params):
		return params

	def __list(self):
		pass

if __name__ == "__main__":
	service = "https://find.nrao.edu/splata-slap/slap"
	client = SlapClient(service)
	data = client.query(WAVELENGTH=0.010)
	data.to_xml("test.xml")


