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
DI_FILE = "tmp/my_db/bsmolley_dbi"

# Number of records in each database
DB_SIZE = 10

# Random generator seed
SEED = 10000000

# Convert to microseconds
MICRO = 100000

# Generate a random number
def get_random():
    return random.randint(0, 63)


# Generate a random character
def get_random_char():
    return chr(97 + random.randint(0, 25))

# Write the results file answers.txt
def writeAnswerFile(answers) :
	with open(ANSWER_FILE, "a") as file:
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

		key = key.encode(encoding='UTF-8')
		value = value.encode(encoding='UTF-8')
		db[key] = value

	end = time.time()
	print("Time Elapsed: %s" %((end-start)*MICRO))

# Function to make indexed database
def createIndexedDatabase(db, dbrev):
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

		key = key.encode(encoding='UTF-8')
		value = value.encode(encoding='UTF-8')
		db[key] = value
		dbrev[value] = key

	end = time.time()
	print("Time Elapsed: %s" %((end-start)* MICRO))

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
	taken = (end - start) * MICRO
	print("Records retrieved %s" %records)
	print("Time Elapsed: %s" %taken)
	writeAnswerFile(answers)

# Function used  to retrieve a data by key, quickly
def retrieveByReversedData(db, key):
	start = time.time()
	answers = []
	records = 0
	if db.has_key(key):
		value = db[key]
		print("\nValue: %s\n\nKey: %s\n" %(key, value))
		records += 1
		pair = [key, value]
		answers.append(pair)

	else:
		print("No value exists for key %s" %key)

	end = time.time()
	taken = (end - start) * MICRO
	print("Records retrieved %s" %records)
	print("Time Elapsed: %s" %taken)
	writeAnswerFile(answers)

# Function used to retrieve records with a given data
def retrieveData(db, data):
	start = time.time()
	answers = []
	records = 0
	for k,d in db.iteritems():
		val = db[k]
		records += 1
		if (val == data):
			pair = [k, val]
			print("\nKey: %s\n\nValue %s\n" %(k, val))
			answers.append(pair)
			print(pair)
	end = time.time()
	taken = (end - start) * MICRO
	print("Time elapsed: %s" %taken)
	print("Records retrieved: "+str(records))

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
	taken = (end - start) * MICRO
	print("Records retrieved %s" %records)
	print("Time Elapsed: %s" %taken)

# Function to destroy the database
def destroy(db):
	start = time.time()

	keys = db.keys()
	for key in keys:
		del db[key]

	db.sync()
	end = time.time()
	taken = (end - start) * MICRO
	print("Time Elapsed: %s" %taken)

# Function to destroy both of the databases
def destroyBoth(db, dbrev):
	start = time.time()

	keys = db.keys()
	for key in keys:
		del db[key]
	db.sync()

	keys = dbrev.keys()
	for key in keys:
		del dbrev[key]
	dbrev.sync()

	end = time.time()
	taken = (end - start) * MICRO
	print("Time Elapsed: %s" %taken)	
	
def printDatabase(db, dbrev):
	print("Primary")
	for i in db.iteritems():
		print(i)
	print()
	print()
	print("Secondary")
	if dbrev != "Non-existant":
		for i in dbrev.iteritems():
			print(i)


def main():
	# Grab command line input
	command = sys.argv
	print(command)

	# Parse command line input, grab Database Option
	mode = command[1]
	
	# Set the random number generator seed
	random.seed(SEED)

	# Do stuff based on selected mode, Btree, Hash, or indexfile
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
		try:
			db = bsddb.btopen(DA_FILE, "w")
			dbrev = bsddb.btopen(DI_FILE, "w")
		except:
			# Create database if it does not exist
			print("Database does not exist, making a new one")
			db = bsddb.btopen(DA_FILE, "c")
			dbrev = bsddb.btopen(DI_FILE, "c")

		print("indexfile selected")
	else:
		print("Not an option, exiting.")
		return

	
	

	# Main menu
	option = ""
	while option != "7":
		print("\nMain Menu")
		print("(1) Create and populate a database")
		print("(2) Retrieve records with a given key")
		print("(3) Retrieve records with a given data")
		print("(4) Retrieve records with a given range of keys")
		print("(5) Destroy the database")
		print("(6) Show all keys to databases")
		print("(7) Quit")

		option = input("Enter task number: ")

		# Option switch 
		if option == "1":
			if mode == "indexfile":
				createIndexedDatabase(db, dbrev)
			else:
				createDatabase(db)

		elif option == "2":
			key_str = input("  Enter key: ")
			if len(key_str) > 0:
				key = str.encode(key_str, 'utf-8')
				retrieveByKey(db, key)

		elif option == "3":
			data_str = input(" Enter data: ")
			if len(data_str) > 0:
				data = str.encode(data_str, 'utf-8')
				if mode == "indexfile":
					retrieveByReversedData(dbrev, data)
				else:
					retrieveData(db, data)

		elif option == "4":
			start_key = input("  Starting key: ")
			end_key = input("  Ending key: ")
			if len(start_key) > 0 or len(end_key) > 0:
				key1 = str.encode(start_key, 'utf-8')
				key2 = str.encode(end_key, 'utf-8')
				keys = [key1, key2]
				retrieveInRange(db, keys)

		elif option == "5":
			if mode == "indexfile":
				destroyBoth(db, dbrev)
			else:
				destroy(db)

		elif option == "6":
			if mode == "indexfile":
				printDatabase(db, dbrev)
			else:
				printDatabase(db, "Non-existant")
		elif option == "7":
			print("Exiting.")

		else:
			print("Not an option, choose again.")


	# Close database file, end of program
	try:
		if mode == "indexfile":
			db.close()
			dbrev.close()
			os.remove(DA_FILE)
			os.remove(DI_FILE)
		else:
			db.close()
			os.remove(DA_FILE)

	except Exception as e:
		print (e)


if __name__ == "__main__":
	main()
