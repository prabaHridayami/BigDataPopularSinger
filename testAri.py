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
        self.json_file_name = "json/tes1_xxxtentacion.json"
        self.excel_file_name = "xxxtentacion"
        self.idSinger = 40
        self.array_x = []
        self.array_y = []
        self.array_z = []
        self.array_tot = []

    def read_text_file(self):
        # membersihkan data dan merubah ke bentuk .txt
        try:
            dataHasil = ""
            with open(self.json_file_name, 'r', encoding="utf-8") as data_file:
                data = json.load(data_file)
                for each_axis in data["data"]:
                    b = str(each_axis["a_created_at"])
                    hasil = b.split(" ")
                    date = hasil[0].split("-")
                    dataHasil += date[2] + "/" + date[1] + "/" + date[0] + "\n"

            with open('tweet.txt', 'w', encoding="utf-8") as file:
                file.write(dataHasil)
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise

    def spark_count(self):
        # menghitung banyak tweet per tanggal
        conf = SparkConf().setAppName("word Count").setMaster("local[3]")
        sc = SparkContext(conf=conf)
        dataLoc = ""

        lines = sc.textFile("tweet.txt")

        wordCounts = lines.countByValue()

        for word, count in wordCounts.items():
            # dataLoc += ("{} : {}".format(word, count)) + "\n"
            x = str(word)
            y = count
            self.array_x.append(x)
            self.array_y.append(y)
            pprint.pprint("x = {0}".format(x))
            pprint.pprint("y = {0}".format(y))

    def retweet(self):
        # membersihkan data dan merubah ke bentuk .txt
        try:
            dataHasil = ""
            sum =0

            with open(self.json_file_name, 'r', encoding="utf-8") as data_file:
                data = json.load(data_file)
                for tanggal in range(0, len(self.array_x)):
                    retweet = 0
                    total = 0
                    for each_axis in data["data"]:
                        b = str(each_axis["a_created_at"])
                        c = int(each_axis["e_retweet"])

                        hasil = b.split(" ")
                        date = hasil[0].split("-")
                        dataHasil = date[2] + "/" + date[1] + "/" + date[0]

                        if (self.array_x[tanggal] == dataHasil):
                            retweet = retweet + c
                    total = self.array_y[tanggal]+retweet
                    self.array_tot.append(total)
                    self.array_z.append(retweet)

                for i in range(0,len(self.array_x)):
                    pprint.pprint(str(self.array_x[i])+"   "+str(self.array_y[i])+"   "+str(self.array_z[i])+"   "+str(self.array_tot[i]))
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise

    def save_to_xlsx(self):
        # menyimpan ke bentuk excel
        workbook = xlsxwriter.Workbook("output/output_" + self.excel_file_name + ".xlsx")
        worksheet = workbook.add_worksheet()
        for index, value in enumerate(self.array_x):
            worksheet.write(index, 0, self.array_x[index])
            worksheet.write(index, 1, self.array_y[index])
            worksheet.write(index, 2, self.array_z[index])
            worksheet.write(index, 3, self.array_tot[index])
            worksheet.write(index, 4, self.idSinger)
        workbook.close()


if __name__ == '__main__':
    file_management = FileManagement()
    file_management.read_text_file()
    file_management.spark_count()
    file_management.retweet()
    file_management.save_to_xlsx()
