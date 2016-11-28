import csv
import sys
import random
import name

header = ["id", "name", "gender", "age", "country"]

for i in range(1, int(sys.argv[1])):
	gender = random.choice(["male", "female"])
	new_name = names.get_first_name(gender=gender)
	age = random.choice(range(10,99))
	country = 
	mydata.append([i, new_name, gender, age, country])
with open("output.csv", "wb") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(mydata)
