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
        self.country = "country/countries.json"
        self.array_x = []
        self.array_y = []

    def read_text_file(self):
        # membersihkan data dan merubah ke bentuk .txt
        try:
            dataHasil = ""
            tweet = 1
            with open(self.json_file_name, 'r', encoding="utf-8") as data_file:
                data = json.load(data_file)
                for each_axis in data["data"]:
                    locations = (each_axis["c_location"])
                    locations = locations.split(',')
                    for x in range(len(locations)):
                        locations[x] = locations[x].replace(' ', '')
                        locations[x] = filter_country(str(locations[x]))

                        if locations[x] == '':
                            locations[x] = 'none'
                        loc = locations[x]
                        dataHasil += str(loc) + "\n"
                    tweet += 1

                pprint.pprint("tweet : " + str(tweet))

            with open('Location.txt', 'w', encoding="utf-8") as file:
                file.write(dataHasil)
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise

    def spark_count(self):
        # menghitung banyak tweet per tanggal
        conf = SparkConf().setAppName("word Count").setMaster("local[3]")
        sc = SparkContext(conf=conf)
        dataLoc = ""

        lines = sc.textFile("Location.txt")

        wordCounts = lines.countByValue()

        for word, count in wordCounts.items():
            dataLoc += ("{} : {}".format(word, count)) + "\n"
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
            tweet = 1
            with open(self.json_file_name, 'r', encoding="utf-8") as data_file:
                data = json.load(data_file)
                for each_axis in data["data"]:
                    locations = (each_axis["c_location"])
                    retweet = (each_axis["e_retweet"])
                    locations = locations.split(',')
                    for x in range(len(locations)):
                        locations[x] = locations[x].replace(' ', '')
                        locations[x] = filter_country(str(locations[x]))

                        if locations[x] == '':
                            locations[x] = 'none'
                        loc = locations[x]
                        dataHasil += str(loc) + "\n"
                    tweet += 1

                pprint.pprint("tweet : " + str(tweet))

            with open('Location.txt', 'w', encoding="utf-8") as file:
                file.write(dataHasil)
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise

    def save_to_xlsx(self):
        # menyimpan ke bentuk excel
        workbook = xlsxwriter.Workbook("output/output_loc_"+self.excel_file_name+".xlsx")
        worksheet = workbook.add_worksheet()
        for index, value in enumerate(self.array_x):
            worksheet.write(index, 0, self.array_x[index])
            worksheet.write(index, 1, self.array_y[index])
            worksheet.write(index, 2, self.idSinger)
        workbook.close()


def filter_country(c):
    with open('country/countries.json', 'r', encoding="utf-8") as loc:
        location = json.load(loc)
    country = ''
    for each_loc in location:
        if c.lower() == each_loc.lower():
            country = each_loc
        else:
            for x in location[each_loc]:
                if c.lower() == 'usa' or c.lower() == 'u.s.a' or c.lower() == 'us' or c.lower() == 'nyc':
                    country = 'United States'
                    break
                elif c.lower() == 'uk' or c.lower() == 'u.k':
                    country = 'United Kingdom'
                    break
                elif c.lower() == x.lower():
                    country = each_loc
                    break
    return (country)


if __name__ == '__main__':
    file_management = FileManagement()
    file_management.read_text_file()
    file_management.spark_count()
    file_management.save_to_xlsx()
