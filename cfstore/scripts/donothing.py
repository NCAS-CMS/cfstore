import os
import json
if __name__ == '__main__':
    variable_output_list={}
    for filename in os.listdir(os.curdir):
        print(filename)
        variable_output_list[filename]=True

    print("Success")
    with open("tempfile.json","a") as writepath:
        json.dump(variable_output_list,writepath)
