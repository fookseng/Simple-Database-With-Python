# import difflib
# with open('3j.output') as f1:
#     f1_text = f1.read()
# with open('3.output') as f2:
#     f2_text = f2.read()
# # Find and print the diff:
# for line in difflib.unified_diff(f1_text, f2_text, fromfile='file1', tofile='file2', lineterm=''):
#     print(line)
import os
import json
import argparse
import sys
import time

################################### VARIABLES DECLARATIONS ###################################################
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--input", required=True, help="path to the input file")
args = vars(ap.parse_args())

#print(os.getcwd())
# create folder and empty file
if not os.path.exists('storage'):
    try:
        os.makedirs('storage')
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
if not os.path.exists('./storage/db_search.txt'):
    try:
        with open('./storage/db_search.txt', 'w') as f:
            declaration = {"COUNTER":"1"}
            json.dump(declaration, f)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
if not os.path.exists('./storage/index.txt'):
    try:
        with open('./storage/index.txt', 'w') as f:
            f.write("1")
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

input_file_path = args["input"]
#print(input_file_path)

output_file_name = input_file_path.split('/')
#print(output_file_name)
output_file_name = output_file_name[-1].split('.')
#print(output_file_name)
output_file_name = output_file_name[0]
#print(output_file_name)

dict = {}
search_dict = {}
file_counter = 0
list = []
filesize = 5428800 # 0.5 Gb
#filesize = 10 # 0.5 Gb
eof = 0
eof_flag = 0
beginning = 0
db_beginning = 0
eofc = 0


f = open("./storage/index.txt", "r")
file_counter = f.read()
file_counter = int(file_counter)

######################################### FUNCTIONS    #################################################

def PUT(key, value):
    global dict
    global file_counter
    global list
    global search_dict
    global filesize
    global eof_flag
    global db_beginning
    global eofc

    check_exist = 0
    # check if current data exist in db
    check_exist = search_key(key)

    # add if not exist, else update
    if check_exist == None:
        #print("None")
        # add data to dict and list
        dict.update({key: value})
        if key not in list:
            #print("add")
            list.append(key)
            if not db_beginning:
                # print("1")
                with open("./storage/" + str(file_counter) + '.db.txt', 'a') as f:
                    f.write("{" + "\"" + key + "\"" + ":" + "\"" + value + "\"" + "}")
                    # json.dump(dict, outfile)
                    db_beginning = + 1
                # print("begin" + key +"    "+ value)
            else:
                # print("2")
                f = open("./storage/" + str(file_counter) + '.db.txt', 'ab');
                f.seek(-1, os.SEEK_END)
                f.truncate()
                with open("./storage/" + str(file_counter) + '.db.txt', 'a') as f:
                    f.write("," + "\"" + key + "\"" + ":" + "\"" + value + "\"" + "}")
                # print("AFTER" + key +"    "+ value)
        else:
            #print("update")
            # key in list, update current db
            json_file = open('./storage/' + str(file_counter) + '.db.txt', 'r')
            update_data = json.load(json_file)
            json_file.close()
            update_data[key] = value
            json_file = open('./storage/' + str(file_counter) + '.db.txt', 'w')
            json.dump(update_data, json_file)
            json_file.close()
        # update db

        # check if db is full
        if os.path.getsize('./storage/' + str(file_counter) + '.db.txt') >= filesize or eof_flag:
            #CLOSE CURRENT DATABASE
            search_dict.update({file_counter: list})
            with open('./storage/db_search.txt') as f:
                db_data = json.load(f)
            db_data.update(search_dict)
            with open('./storage/db_search.txt', 'w') as outfile:
                json.dump(db_data, outfile)
            list.clear()
            dict.clear()
            search_dict.clear()
            file_counter += 1
            db_beginning = 0
    else:
        #print("YES")
        #print(key, value)
        #print("update2")
        json_file = open('./storage/'+str(check_exist)+'.db.txt', 'r')
        update_data = json.load(json_file)
        json_file.close()
        update_data[key] = value
        json_file = open('./storage/' + str(check_exist) + '.db.txt', 'w')
        json.dump(update_data, json_file)
        json_file.close()

def GET(key):
    global dict
    global output_file_name
    global beginning
    check_locat = 0

    x = dict.get(key, "NO")
    if x == "NO":
        check_locat = search_key(key)
        if check_locat == None:
            #print("Data not found!")
            f = open(str(output_file_name) + ".output", "a")
            if not beginning:
                f.write("EMPTY")
                f.close()
                beginning += 1
            else:
                f.write('\n'+"EMPTY")
                f.close()
        else:
            json_file = open('./storage/' + str(check_locat) + '.db.txt', 'r')
            read_data = json.load(json_file)
            json_file.close()
            x = read_data.get(key, 'EMPTY')
            f = open(str(output_file_name) + ".output", "a")
            if not beginning:
                f.write(x)
                f.close()
                beginning += 1
            else:
                f.write('\n' + x)
                f.close()
    else:
        f = open(str(output_file_name) + ".output", "a")
        if not beginning:
            f.write(x)
            f.close()
            beginning += 1
        else:
            f.write('\n' + x)
            f.close()

def SCAN(key1, key2):
    global dict
    global output_file_name
    global beginning
    check_locat = 0

    for i in range(int(key1), int(key2)+1):
        x = dict.get(str(i), "NO")
        if x == "NO":
            check_locat = search_key(str(i))
            if check_locat == None:
                # print("Data not found!")
                f = open(str(output_file_name) + ".output", "a")
                if not beginning:
                    f.write("EMPTY")
                    f.close()
                    beginning += 1
                else:
                    f.write('\n' + "EMPTY")
                    f.close()
            else:
                json_file = open('./storage/' + str(check_locat) + '.db.txt', 'r')
                read_data = json.load(json_file)
                json_file.close()
                x = read_data.get(str(i), 'EMPTY')
                f = open(str(output_file_name) + ".output", "a")
                if not beginning:
                    f.write(x)
                    f.close()
                    beginning += 1
                else:
                    f.write('\n' + x)
                    f.close()
        else:
            f = open(str(output_file_name) + ".output", "a")
            if not beginning:
                f.write(x)
                f.close()
                beginning += 1
            else:
                f.write('\n' + x)
                f.close()

def search_key(input):
    temp_list = []
    with open('./storage/db_search.txt') as json_file:
        data = json.load(json_file)
        #print(data)
        for i in range(len(data)):
            #print(data.get(str(i)))
            temp_list.append(data.get(str(i)))
            #print(temp_list)
            #print(len(temp_list))
        temp_list.pop(0)
        #print(temp_list)
        #print(len(temp_list))
        for j in range(len(temp_list)-1, -1, -1):
            #print(temp_list[j])
            for k in range(len(temp_list[j])-1, -1, -1):
                #print(temp_list[j][k])
                if input == temp_list[j][k]:
                    return j+1


######################################## MAIN ###############################################
# Using readlines()
input_file = open(input_file_path, 'r')
Lines = input_file.readlines()
count = 1
#print(len(Lines))
start = time.time()
for line in Lines:
    #print(count)
    count += 1
    temp = line.split()
    eof += 1
    #print(temp)
    if eof == len(Lines):
        eof_flag = 1
    if temp[0] == 'PUT':
        PUT(temp[1], temp[2])
    elif temp[0] == 'GET':
        GET(temp[1])
    elif temp[0] == 'SCAN':
        SCAN(temp[1], temp[2])
    else:
        print("ERROR")
#print(search_key("17"))
end = time.time()
total_time = end - start
print("Total run time: " + str(total_time))

if eof_flag and list:
    search_dict.update({file_counter: list})
    with open('./storage/db_search.txt') as f:
        db_data = json.load(f)
    db_data.update(search_dict)
    with open('./storage/db_search.txt', 'w') as outfile:
        json.dump(db_data, outfile)
    list.clear()
    dict.clear()
    search_dict.clear()
    file_counter += 1

with open('./storage/index.txt', 'w') as f:
    f.write(str(file_counter))
