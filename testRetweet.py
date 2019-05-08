from pyspark import SparkContext, SparkConf
import pprint
import sys
import json
import datetime
import xlsxwriter
import pandas as pd
import openpyxl


class FileManagement:
    def __init__(self):
        self.json_file_name = "json/tes1_freddiemercury.json"
        self.excel_file_name = "freddiemercury"
        self.idSinger = 16
        self.array_x = []
        self.array_y = []

    def read_text_file(self):
        # membersihkan data dan merubah ke bentuk .txt
        try:
            dataHasil = []
            with open(self.json_file_name, 'r', encoding="utf-8") as data_file:
                data = json.load(data_file)
                for each_axis in data["data"]:

                    b = str(each_axis["a_created_at"])
                    c = int(each_axis["e_retweet"])

                    hasil = b.split(" ")
                    date = hasil[0].split("-")
                    dataHasil = date[2] + "/" + date[1] + "/" + date[0]

                    self.array_x.append(dataHasil)
                    self.array_y.append(c)

            # with open('tweet.txt', 'w', encoding="utf-8") as file:
            #     file.write(dataHasil)

                # for i in range(0,len(self.array_x)-1):
                #     for j in range(0,len(self.array_x)):
                #     if(self.array_x[i]==self.array_x[i+1]):
                #         self.array_y[]
                #     pprint.pprint(self.array_x[i])
                # pprint.pprint(self.array_x)
                # pprint.pprint(self.array_y)
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise

if __name__ == '__main__':
    file_management = FileManagement()
    file_management.read_text_file()
