from pyspark import SparkContext, SparkConf
import sys
import pprint
import xlsxwriter
import json

class FileManagement:
    def __init__(self):
        self.json_file_name = "Marvel.txt"
        self.excel_file_name = "output.xlsx"
        self.array_x = []
        self.array_y = []
        self.array_sum = []

    def read_text_file(self):
        try:
            conf = SparkConf().setAppName("word count").setMaster("local[3]")
            sc = SparkContext(conf=conf)

            lines = sc.textFile(self.json_file_name)

            words = lines.flatMap(lambda line: line.split(" "))

            wordCounts = words.countByValue()

            for word, count in wordCounts.items():
                x = str(word)
                y = str(count)
                self.array_x.append(x)
                self.array_y.append(y)
                pprint.pprint("x = {0}".format(x))
                pprint.pprint("y = {0}".format(y))
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise

    def save_to_xlsx(self):
        workbook = xlsxwriter.Workbook(self.excel_file_name)
        worksheet = workbook.add_worksheet()
        for index, value in enumerate(self.array_x):
            worksheet.write(index, 0, self.array_x[index])
            worksheet.write(index, 1, self.array_y[index])
        workbook.close()

if __name__ == "__main__":
    file_management = FileManagement()
    file_management.read_text_file()
    file_management.save_to_xlsx()