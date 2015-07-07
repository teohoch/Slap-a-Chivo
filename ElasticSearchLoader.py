__author__ = 'teohoch'
import csv
import elasticsearch
import json
import os
from time import sleep
from datetime import datetime


class ElasticSearchLoader():
    """
    Class designed to load CSV Files containing to a ElasticSearch Server
    """

    def __init__(self, fail_directory="", elastic_server=None):
        """

        :param elastic_server: The Hostname for the ElasticSearch Server. Can be the hostname to a single Node or an
        array of Hostnames.
        :type elastic_server: String or Array of Strings
        :param fail_file: Filename where to save the DataLines that couldn't be Uploaded
        :type fail_file: String
        """
        self.server = elastic_server
        self.__server_connection = elasticsearch.Elasticsearch(hosts=elastic_server, sniff_on_start=True)
        self.__default_index = None
        self.__default_mapping = None
        self.fail_directory = fail_directory
        self.index_json = '{' \
                          '    "settings" :' \
                          '    {' \
                          '        "number_of_shards" : 10,' \
                          '        "number_of_replicas"  : 2' \
                          '    },' \
                          '    "mappings" :' \
                          '    {' \
                          '        "line" :' \
                          '        {' \
                          '            "properties" :' \
                          '            {' \
                          '                "Species"               :   {"type" : "string"},' \
                          '                "Chemical Name"         :   {"type" : "string"},' \
                          '                "Freq-MHz"              :   {"type" : "double"},' \
                          '                "Freq Err"              :   {"type" : "double"},' \
                          '                "Meas Freq-MHz"         :   {"type" : "double"},' \
                          '                "Meas Freq Err"         :   {"type" : "double"},' \
                          '                "Resolved QNs"          :   {"type" : "string"},' \
                          '                "CDMS/JPL Intensity"    :   {"type" : "double"},' \
                          '                "Sijmu2"                :   {"type" : "double"},' \
                          '                "Sij"                   :   {"type" : "double"},' \
                          '                "Log10(Aij)"            :   {"type" : "double"},' \
                          '                "Lovas/AST Intensity"   :   {"type" : "string"},' \
                          '                "E_L (cm^-1)"           :   {"type" : "double"},' \
                          '                "E_L (K)"               :   {"type" : "double"},' \
                          '                "E_U (cm^-1)"           :   {"type" : "double"},' \
                          '                "E_U (K)"               :   {"type" : "double"},' \
                          '                "Linelist"              :   {"type" : "string"}' \
                          '            }' \
                          '        }' \
                          '    }' \
                          '}'

    def __upload_line(self, data, index, mapping, retries=10,):
        """
        Upload a Single Line (Row) of Data to the Server
        :param data: Array of Data to upload
        :type data: Array of Strings
        :param retries: Number of times to retry uploading before giving up
        :type retries: int
        :return: Return a Status Number. For Success => 0, For Connection Error => 1, For Malformation Error => 2,
        Other => 3
        :rtype: int
        """
        success = 3
        if len(data) >= 17:
            try:
                line = {
                    "Species": data[0],
                    "Chemical Name": data[1],
                    "Freq-MHz": float(data[2]) if data[2] else None,
                    "Freq Err": float(data[3]) if data[3] else None,
                    "Meas Freq-MHz": float(data[4]) if data[4] else None,
                    "Meas Freq Err": float(data[5]) if data[5] else None,
                    "Resolved QNs": data[6],
                    "CDMS/JPL Intensity": float(data[7]) if data[7] else None,
                    "Sijmu2": float(data[8]) if data[8] else None,
                    "Sij": float(data[9]) if data[9] else None,
                    "Log10(Aij)": float(data[10]) if data[10] else None,
                    "Lovas/AST Intensity": data[11] if data[10] else None,
                    "E_L (cm^-1)": float(data[12]) if data[12] else None,
                    "E_L (K)": float(data[13]) if data[13] else None,
                    "E_U (cm^-1)": float(data[14]) if data[14] else None,
                    "E_U (K)": float(data[15]) if data[15] else None,
                    "Linelist": data[16]
                }
                try_count = 0
                while try_count < retries:
                    try:
                        self.__server_connection.create(index, mapping, json.dumps(line))
                        success = 0
                    except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout,
                            elasticsearch.ElasticsearchException), ese:
                        try_count += 1
                        if try_count == retries:
                            print("Could not Upload DataLine, after " + retries + "retries => " + str(data))
                            print(ese)
                            success = 1
                        else:
                            sleep(1 * try_count)
            except Exception as e:
                print "Error in Data Line -> " + str(data)
                success = 2
        else:
            print "DataLine is malformed -> " + str(data)
            success = 2
        return success

    def upload_single_file(self, index, mapping, filename, delimiter=":", retries=10):
        """
        Upload a single CSV file to the Server.
        :param filename: File to upload.
        :type filename: str
        :param delimiter: Delimiter character used in the CSV, defaults to ":"
        :type delimiter: str
        :param retries: Number of retries before giving up.
        :type retries: int
        :param index: The index into which upload the data
        :type index: str
        """

        if index and mapping:
            if self.__server_connection.indices.exists(index) \
                    and self.__server_connection.indices.get_mapping(index,mapping):
                time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fails = {"1": ["connection", []], "2": ["malformed", []], "3": ["other", []]}
                with open(filename) as csvfile:
                    data = csv.reader(csvfile, delimiter=delimiter)
                    first = True
                    for row in data:
                        if first:
                            first = False
                        else:
                            success = self.__upload_line(row, index, mapping, retries)
                            if success != 0:
                                fails[str(success)][1].append(row)

                for key, value in fails.iteritems():
                    if len(value[1]) > 0:
                        fail_file = os.path.join(self.fail_directory,
                                                 "Fail-" + filename.split("/")[-1] + "-" + value[0] + " " + time)
                        with open(fail_file, "w") as f:
                            for row in value[1]:
                                f.write(row)
            else:
                raise elasticsearch.ConnectionError("The selected index or mapping doesn't exist!")
        else:
            raise ValueError("The index or the mapping to use wasn't defined!")

    def __get_file_list(self, directory=os.getcwd(), ext=".csv"):
        """
        Gets a list of files from a directory and subdirectory that match the specified extension
        :param directory: The root directory to search in
        :type directory: String
        :param ext: The extension to search for
        :type ext: String
        :return: An array of file names
        :rtype: Array of Strings
        """
        f = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(ext):
                    f.append(os.path.join(root, file))
        return f

    def upload_multiple_files(self, directory=os.getcwd(), ext=".csv", delimeter=":", retries=10):
        """
        Uploads multiple CSV files from a directory to the Server.
        :param directory: The root directory to search in
        :type directory: String
        :param ext: The extension to search for
        :type ext: String
        :param delimiter: Delimiter character used in the CSV, defaults to ":"
        :type delimiter: String
        :param retries: Number of retries before giving up.
        :type retries: Integer
        """

        data_files = self.__get_file_list(directory, ext)
        for f in data_files:
            print("Uploading File:" + f)
            self.upload_single_file(f, delimeter, retries)

    def create_index(self, index_name, index_options={}):
        """
        Creates a Index in the ES. By default, it has no mappings and the default settings
        :param index_name: Name of the index
        :type index_name: String
        :param index_options: Dictionary with the creation options,
        refer to https://www.elastic.co/guide/en/elasticsearch/reference/1.6/indices-create-index.html
        :type index_options: Dictionary
        """
        lower_index_name = index_name.lower()
        if not self.__server_connection.indices.exists(lower_index_name):
            self.__server_connection.indices.create(lower_index_name, json.dumps(index_options))

    def create_mapping(self, index_name, mapping_name, mapping_options):
        index_to_lower = index_name.lower()
        if self.__server_connection.indices.exists(index_to_lower) and not self.__server_connection.indices.get_mapping(index_to_lower, mapping_name):
                self.__server_connection.indices.put_mapping(mapping_name, mapping_options, index_to_lower)


if __name__ == "__main__":
    loader = ElasticSearchLoader()
    index_options = {"settings": {"number_of_shards": 10, "number_of_replicas": 2}}
    mapping_options = {"properties": {"CDMS/JPL Intensity": {"type": "double"},"Chemical Name": {"type": "string"}, "Freq-MHz": {"type": "double"}, "Meas Freq-MHz": { "type": "double" }, "Species": {"type": "string"}}}
    loader.create_index("Test", index_options)
    loader.create_mapping("Test", "DUmmy",mapping_options)



# units = {
#     "Species"               :   ("String", ""),
#     "Chemical Name"         :   ("String", ""),
#     "Freq-MHz"              :   ("Double", "Mhz"),
#     "Freq Err"              :   ("Double", "Mhz"),
#     "Meas Freq-MHz"         :   ("Double", "Mhz"),
#     "Meas Freq Err"         :   ("Double", "Mhz"),
#     "Resolved QNs"          :   ("String", ""),
#     "CDMS/JPL Intensity"    :   ("Double", ""),
#     "Sijmu2"                :   ("Double", "Devye"),
#     "Sij"                   :   ("Double", ""),
#     "Log10(Aij)"            :   ("Double", "1/Sec"),
#     "Lovas/AST Intensity"   :   ("String", ""),
#     "E_L (cm^-1)"           :   ("Double", "1/cm"),
#     "E_L (K)"               :   ("Double", "Kelvin"),
#     "E_U (cm^-1)"           :   ("Double", "1/cm"),
#     "E_U (K)"               :   ("Double", "Kelvin"),
#     "Linelist"              :   ("String", "")
# }
