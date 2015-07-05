__author__ = 'teohoch'
import csv
import elasticsearch
import json

json_index ='{' \
       '    "settings" :' \
       '    {' \
       '        "number_of_shards" : 10,' \
       '        "number_of_replicas"  : 0' \
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

def load_to_memory(file):
    with open(file) as csvfile:
        first = True
        data = csv.reader(csvfile, delimiter=':')
        first = True
        for row in data:
            if first:
                first = False
            else:
                upload(row)


def upload(data):
    if len(data) >=17:
        try:
            line = {
                "Species"               : data[0],
                "Chemical Name"         : data[1],
                "Freq-MHz"              : float(data[2]) if data[2] else None,
                "Freq Err"              : float(data[3]) if data[3] else None,
                "Meas Freq-MHz"         : float(data[4]) if data[4] else None,
                "Meas Freq Err"         : float(data[5]) if data[5] else None,
                "Resolved QNs"          : data[6],
                "CDMS/JPL Intensity"    : float(data[7]) if data[7] else None,
                "Sijmu2"                : float(data[8]) if data[8] else None,
                "Sij"                   : float(data[9]) if data[9] else None,
                "Log10(Aij)"            : float(data[10]) if data[10] else None,
                "Lovas/AST Intensity"   : data[11] if data[10] else None,
                "E_L (cm^-1)"           : float(data[12]) if data[12] else None,
                "E_L (K)"               : float(data[13]) if data[13] else None,
                "E_U (cm^-1)"           : float(data[14]) if data[14] else None,
                "E_U (K)"               : float(data[15]) if data[15] else None,
                "Linelist"              : data[16]
            }
            el = elasticsearch.Elasticsearch()
            el.create("data","line",json.dumps(line))
        except Exception as e:
            print data
            print e
            raise e
    else:
        print data



def create_index():
    el = elasticsearch.Elasticsearch()
    if not el.indices.exists("data"):
        el.indices.create("data",json_index,)


#test_file = "splatalogue_test.csv"

#test_file = "splatalogue 0-500000.csv"
test_file = "splatalogue 1500000-inf.csv"
create_index()
load_to_memory(test_file)

# for item in load_to_memory(test_file):
#     try:
#         print item[11]
#     except:
#         print("error     ")
#         print item

units = {
    "Species"               :   ("String", ""),
    "Chemical Name"         :   ("String", ""),
    "Freq-MHz"              :   ("Double", "Mhz"),
    "Freq Err"              :   ("Double", "Mhz"),
    "Meas Freq-MHz"         :   ("Double", "Mhz"),
    "Meas Freq Err"         :   ("Double", "Mhz"),
    "Resolved QNs"          :   ("String", ""),
    "CDMS/JPL Intensity"    :   ("Double", ""),
    "Sijmu2"                :   ("Double", "Devye"),
    "Sij"                   :   ("Double", ""),
    "Log10(Aij)"            :   ("Double", "1/Sec"),
    "Lovas/AST Intensity"   :   ("String", ""),
    "E_L (cm^-1)"           :   ("Double", "1/cm"),
    "E_L (K)"               :   ("Double", "Kelvin"),
    "E_U (cm^-1)"           :   ("Double", "1/cm"),
    "E_U (K)"               :   ("Double", "Kelvin"),
    "Linelist"              :   ("String", "")
}
print json.dumps(units)
