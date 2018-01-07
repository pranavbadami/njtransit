f = open('./terminals.txt', 'r')
terminals = f.read()
terminals = terminals.split('\n')
unique = {}

for terminal in terminals:
	if terminal not in unique:
		unique[terminal] = True
	else:
		pass

for terminal in unique: 
	print terminal