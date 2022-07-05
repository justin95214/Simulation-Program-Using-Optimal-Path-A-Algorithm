import re
def parse1():
	for line in open("log1.txt"):
		print(line.split("[")[1].split("]")[0])

def parse2():
	for line in open("log1.txt", "r"):
		print(line.split()[3].strip("[]"))

def parse3():
	for line in open("log1.txt", "r"):
		print(" ".join(line.split("[" or "]")[3:5]))

def parse4():
	for line in open("log1.txt", "rw"):
		print(" ".join(line.split()[3:5]).strip("[]"))
  
def parse5():
	for line in open("log1.txt"):
		print(re.split("\[|\]", line)[1])


parse1()
parse2()
parse3()
#parse4()
parse5()
