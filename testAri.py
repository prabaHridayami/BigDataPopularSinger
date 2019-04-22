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
        self.json_file_name = "json/tes1_camilacabello.json"
        self.excel_file_name = "output_camilacabello.xlsx"
        self.name = "Camila Cabello"
        self.array_x = []
        self.array_y = []
        self.array_sum = []
        print("self")


    def read_text_file(self):
        # membersihkan data dan merubah ke bentuk .txt
        try:
            dataHasil = ""
            tweet = 0
            with open(self.json_file_name, 'r', encoding="utf-8") as data_file:
                data = json.load(data_file)
                for each_axis in data["data"]:
                    a = str(each_axis["id"])
                    b = str(each_axis["a_created_at"])
                    c = str(each_axis["c_location"])
                    f = (each_axis["e_retweet"])

                    hasil = b.split(" ")
                    date = hasil[0].split("-")
                    dd = datetime.date(int(date[0]),int(date[1]),int(date[2]))
                    dataHasil += date[2] + "/" + date[1] + "/" + date[0]+"\n"

                    # pprint.pprint(date[1])
                    # pprint.pprint(dd.strftime("%B"))

                    pprint.pprint(date[2] +" "+dd.strftime("%b")+ " " + date[0])
                    # self.array_x.append(b)
                    tweet += 1
                pprint.pprint("tweet : "+str(tweet))

            with open('Ari.txt', 'w', encoding="utf-8") as file:
                file.write(dataHasil)
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise

    def spark_count(self):
        # menghitung banyak tweet per tanggal
        conf = SparkConf().setAppName("word Count").setMaster("local[3]")
        sc = SparkContext(conf=conf)
        dataLoc = ""

        lines = sc.textFile("Ari.txt")

        wordCounts = lines.countByValue()

        for word, count in wordCounts.items():
            dataLoc += ("{} : {}".format(word, count)) + "\n"
            x = str(word)
            y = str(count)
            self.array_x.append(x)
            self.array_y.append(y)
            pprint.pprint("x = {0}".format(x))
            pprint.pprint("y = {0}".format(y))

        with open('Ari_tweet.txt', 'w', encoding="utf-8") as file:
            file.write(dataLoc)

    def save_to_xlsx(self):
        #menyimpan ke bentuk excel
        workbook = xlsxwriter.Workbook(self.excel_file_name)
        worksheet = workbook.add_worksheet()
        for index, value in enumerate(self.array_x):
            worksheet.write(index, 0, self.array_x[index])
            worksheet.write(index, 1, self.array_y[index])
            worksheet.write(index, 2, self.name)
        workbook.close()

if __name__ == '__main__':
    file_management = FileManagement()
    file_management.read_text_file()
    file_management.spark_count()
    file_management.save_to_xlsx()
