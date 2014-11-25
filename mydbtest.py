#import bsddb3 as bsddb
import random
import sys
import time

# Make sure you run "mkdir /tmp/my_db" first!

# Answers file
ANSWER_FILE = "answers.txt"

# Path of the database file
DA_FILE = "/tmp/my_db/bsmolley_db"

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
	with open("answers.txt", "w") as file:
		for answer in answers:
			key = answer[0]
			value = answer[1]
			file.write(str(key) + "\n")
			file.write(str(value) + "\n")
			file.write("\n")


# Function to make a database
def createDatabase(database):
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
		print (key)
		print (value)
		print ("")
		key = key.encode(encoding='UTF-8')
		value = value.encode(encoding='UTF-8')
		db[key] = value

	end = time.time()
	print("Time Elapsed: %s" %(end-start))

def retrieveByKey(db, key):
	start = time.time()
	answers = []
	records = 0
	if db.has_key(key):
		value = db[key]
		print("Key: %s, Value %s" %(key, value))
		records += 1
		pair = [key, value]
		answers.append(pair)

	else:
		print("No value exists for key %s" %key)

	end = time.time()
	print("Records retrieved %s" %records)
	print("Time Elapsed: %s" %(end-start))
	writeAnswerFile(answers)



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
			#db = bsddb.btopen(DA_FILE, "c")

		print("Btree selected")


	elif mode == "hash":
		try:
			db = bsddb.hashopen(DA_FILE, "w")
		except:
			print("Database does not exist, making a new one")
			#db = bsddb.hashopen(DA_FILE, "c")

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
		print(option)

		# Option switch 
		if option == "1":
			createDatabase(db)


	# Close database file, end of program
	try:
		db.close()

	except Exception as e:
		print (e)


if __name__ == "__main__":
	main()
