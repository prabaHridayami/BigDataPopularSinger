import pyspark
import pprint
import sys
import json


class FileManagement:
    def __init__(self):
        self.json_file_name = "CMarvel.json"
        self.array_x = []
        self.array_y = []
        self.array_sum = []
        print("self")

    def read_text_file(self):
        try:
            dataHasil = ""
            with open(self.json_file_name, 'r', encoding="utf-8") as data_file:
                data = json.load(data_file)
                count = 0;
                for each_axis in data["data"]:
                    count += 1;
                    x = str(each_axis["time"])
                    y = str(each_axis["_id"])
                    hasil = x.split(" ")
                    dataHasil += y+", "+hasil[0] + " " + hasil[1] + " " + hasil[2] + "\n"
                    pprint.pprint("ID :" +y +", Hari :" + hasil[0] + " , " + "Bulan :" + hasil[1]+ ", Jam :" + hasil[3])
            with open('Marvel.txt', 'w', encoding="utf-8") as file:
                file.write(dataHasil)
        except:
            print("Unexpected Error :", sys.exc_info()[0])
            raise


if __name__ == '__main__':
    file_management = FileManagement()
    file_management.read_text_file()
