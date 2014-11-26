import bsddb3 as bsddb
import random
import sys
import time
import os

# Make sure you run "mkdir /tmp/my_db" first!

# Answers file
ANSWER_FILE = "answers.txt"

# Path of the database file
DA_FILE = "tmp/my_db/bsmolley_db"

# Number of records in each database
DB_SIZE = 100000

# Random generator seed
SEED = 10000000

# Generate a random number
def get_random():
    return random.randint(0, 63)


# Generate a random character
def get_random_char():
    return chr(97 + random.randint(0, 25))

# Write the results file answers.txt
def writeAnswerFile(answers) :
	with open(ANSWER_FILE, "w") as file:
		for answer in answers:
			key = answer[0]
			value = answer[1]
			file.write("Key: " + str(key) + "\n")
			file.write("Value: " + str(value) + "\n")
			file.write("\n")


# Function to make a database
def createDatabase(db):
	# Start timer
	start = time.time()

	# Populate database
	for index in range(DB_SIZE):
		krng = 64 + get_random()
		key = ""
		for i in range(krng):
		    key += str(get_random_char())
		vrng = 64 + get_random()
		value = ""
		for i in range(vrng):
		    value += str(get_random_char())
		print (key + ":" + value)
	#	print (value)
		print ("")
		key = key.encode(encoding='UTF-8')
		value = value.encode(encoding='UTF-8')
		db[key] = value

	end = time.time()
	print("Time Elapsed: %s" %(end-start))

# Function used  to retrieve a data by key
def retrieveByKey(db, key):
	start = time.time()
	answers = []
	records = 0
	if db.has_key(key):
		value = db[key]
		print("\nKey: %s\n\nValue %s\n" %(key, value))
		records += 1
		pair = [key, value]
		answers.append(pair)

	else:
		print("No value exists for key %s" %key)

	end = time.time()
	taken = end - start
	print("Records retrieved %s" %records)
	print("Time Elapsed: %s" %taken)
#	writeAnswerFile(answers, taken)

# Function used to retrieve records in a range of keys
def retrieveInRange(db, keys):
	answers = []
	records = 0
	start = time.time()

	for k,d in db.iteritems():
		if (k > keys[0]) and (k < keys[1]):
			answers.append([k,d])
			records += 1
	end = time.time()
	taken = end - start
	print("Records retrieved %s" %records)
	print("Time Elapsed: %s" %taken)

# Function to destroy the database
def destroy(db):
	start = time.time()

	keys = db.keys()
	for key in keys:
		del db[key]
		print(key)
	db.sync()

	end = time.time()

	print("Time Elapsed: %s" %(end-start))
	os.remove(DA_FILE)


def main():
	# Grab command line input
	command = sys.argv
	print(command)

	# Parse command line input, grab Database Option
	mode = command[1]
	
	# Set the random number generator seed
	random.seed(SEED)

	# Do stuff based on selected mode, Btree, Hash, or Indexfile
	if mode == "btree":
		try:
			# Open existing database
			db = bsddb.btopen(DA_FILE, "w")
		except:
			# Create database if it does not exist
			print("Database does not exist, making a new one")
			db = bsddb.btopen(DA_FILE, "c")

		print("Btree selected")


	elif mode == "hash":
		try:
			db = bsddb.hashopen(DA_FILE, "w")
		except:
			print("Database does not exist, making a new one")
			db = bsddb.hashopen(DA_FILE, "c")

		print("Hash table selected")


	elif mode == "indexfile":
		print("Indexfile selected")

	
	

	# Main menu
	option = ""
	while option != "6":
		print("\nMain Menu")
		print("(1) Create and populate a database")
		print("(2) Retrieve records with a given key")
		print("(3) Retrieve records with a given data")
		print("(4) Retrieve records with a given range of keys")
		print("(5) Destroy the database")
		print("(6) Quit")

		option = input("Enter number of task: ")

		# Option switch 
		if option == "1":
			createDatabase(db)

		elif option == "2":
			key_str = input("  Enter key: ")
			key = str.encode(key_str)
			retrieveByKey(db, key)

		elif option == "4":
			start_key = input("  Starting key: ")
			end_key = input("  Ending key: ")
			keys = [str.encode(start_key, 'utf-8'), str.encode(end_key, 'utf-8')]
			retrieveInRange(db, keys)

		elif option == "5":
			destroy(db)


	# Close database file, end of program
	try:
		db.close()

	except Exception as e:
		print (e)


if __name__ == "__main__":
	main()
