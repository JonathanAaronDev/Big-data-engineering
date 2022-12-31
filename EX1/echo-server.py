# python3 ./echo-server.py 127.0.0.1:8081 127.0.0.1:8080 127.0.0.1:8082 
# python3 ./echo-server.py 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082
# python3 ./echo-server.py 127.0.0.1:8082 127.0.0.1:8081 127.0.0.1:8080 

import socket
import sys
import threading
import traceback
import json


# ---- This class describes a server ---- #
class State(object):
    # ---- Class constructor ---- #
    def __init__(self, args):
        self.host = self._parseHost(args[0]) # Get the current host IP and Port
        self.siblings = [self._parseHost(x) for x in args[1:]] # Save the siblings adresses in a list
        self.connections = {} # This dictionary saves the ip adresses and their ports 
        self.data = {} #This dictionary saves the keys and values inserted to the dictionary

    # ---- This function handles connection to the siblings and returns the file descriptor we use to talk with this sibling ---- #
    def siblingFile(self, sibling):
        flag = 0 # Flag to indicate If we are creating a new connection
        (con, f) = self.connections.get(sibling, (None, None)) # Get the values from the dicitonary
        if con is None or con.fileno() == -1: # Check if we already have a connection
            try: # Try to create a connection with a socket
                con = socket.create_connection(sibling) # Create a connection
                f = con.makefile(mode='rw', encoding='utf-8') # Make a file for this connection
                f.write('אח\n') # Write Sibling in hebrew to recognise that its a sibling server and the user wont be able to replicate it
                self.connections[sibling] = (con, f) # Enter the connection to the dictionary
                flag = 1 # Indicate a new connection was created
            except: # We couldnt create a connection, return None
                return None
        if(flag == 1 and len(self.data) != 0): # If the flag is 1 we created a new connection
            f.write(json.dumps(self.data))
            f.write("\n")
            f.flush()
            f.readline()
        return f # Return the file descriptor

    # ---- This function recives a string and separate it to host and port parts ---- #
    def _parseHost(self, hostAndPort):
        parts = hostAndPort.split(':') # Split the string
        host = parts[0] # Save the ip
        port = int(parts[1]) # Save the port
        return (host, port) # Return the host and the port

    # ---- This function handles the process of communication with a client ---- #

    def client(self, cmd, f):
        cmd = cmd.rstrip('\n\r \t') # Remove unwanted chars from the command we recived
        print("got command:",cmd)
        command = cmd[0:3].lower() # Get the command type
        values = cmd[4:] # Get the command values
        key = values.split(' ')[0] # Save the key value
        for sibling in self.siblings: # Iterate through sibling list
            con = self.siblingFile(sibling) # Try to create connection with a sibling
            if(con != None): # If we got None we dont have connection
                try: # Try to write
                    con.write(cmd) # Write the recived command
                    con.write('\n') # Write a new line
                    con.flush() # Flush
                    line = con.readLine() # After we wrote the message check to see if we recived confrmation from sibling server
                    if line != "Processed": 
                        raise "Info didnt register" # If not raise an error
                except:
                    self.connections.pop(sibling) # If the connection wasnt sucsseful remove the sibling from the dicitionary
                    pass
        if (command == 'set' and cmd.count(' ') >= 2):  # If the command is set and valid use it
            if(key in self.data): # Check if we have this key already
                if(self.data[key] <= values[len(key)+1:]): # Check if a the value for this key is bigger than the one already in
                    self.data[key] = values[len(key)+1:] # Set the key and value in the dictionary
                    f.write("Your information was saved") # Write the message
                elif(self.data[key] > values[len(key)+ 1:]): # A bigger value exists
                    f.write("A bigger value exists") # Write the message
            else:
                self.data[key] = values[len(key)+1:] # If its not in our data save it
                f.write("Your information was saved")
        elif (command == 'get' and cmd.count(' ') == 1): # If the command is get and in a proper build(command space key) 
            if(key in self.data): # Check if the key recived is in the dictionary
                f.write(self.data[key]) # Write the value to the client
            else: # The key is not in the db
                f.write("No such Key exists in DB") # Write it to the client
        else:
            f.write("No such command exists") # If the command enterd is not set or get
        f.write("\n") # Write a new line
        f.flush() # Flush

    # ---- This function handles the communication with the sibling servers ---- #
    def sibling(self, cmd, f):
        print("from sibling",cmd)
        cmd = cmd.rstrip('\n\r \t') # Strip the command from unwanted chars
        if(cmd[0:1] == "{" and len(json.loads(cmd)) > len(self.data)): # Check if the info recived from the sibling server is a json object of a dictionary and check it size
            self.data = json.loads(cmd) # If its a dictionary and the dictionary is bigger than the server one, replace his dicitionary with the one reicved
        print("From Sibling: ", cmd)
        command = cmd[0:3].lower() # Get the command type(get or set)
        values = cmd[4:] # Get the values
        if (command == 'set'): # If the command is set
            key = values.split(' ')[0] # Get the key of the pair
            self.data[key] = values[len(key)+1:] # Save the key and value pair
            f.write('Processed\n') # Alert the sibling server that you recived the info
            f.flush() #clears the internal buffer of the file
        if(cmd == '' and len(self.data) != 0): # If we recived empty command(which means for us that the sibling server just got up)
            f.write(json.dumps(self.data)) # If the dictionary has any value send him
            f.write("\n") 
            f.flush()
            return "O" # Get out of the function
        else:
            f.write('Processed\n') # Alert the sibling server that you recived the info
            f.flush() #clears the internal buffer of the file
    # ---- This function handles the thread that is created after a request is accepted ---- #
    def run(self, s, addr):
        a = ''
        for j in self.siblings: # Iterate over the siblings
            a = self.siblingFile(j) # Send each server to siblingFile
        try: # Try to create a connection 
            with s, s.makefile(mode='rw', encoding='utf-8') as f: # Use the connection recvied
                firstLine = f.readline() # Read the first line
                isSibling = firstLine.rstrip('\n\r \t') == 'אח' # Check if the line tells its from a sibling
                if(firstLine.rstrip('\n\r \t') == '' and a != None and len(self.data) == 0): #Check if its a server that went down and came back(we have a known previous connection with this server but we recived empty message)
                    isSibling = True # If so treat the first interaction like a sibling interaction
                if not isSibling: # If not
                    self.client(firstLine, f) # Call the client function
                for line in f: # Iterate through the file
                    if isSibling: # If its a connection from sibling call Sibling on each line
                        o = self.sibling(line, f)
                        if(o == "O"):
                            isSibling = False
                    else: # If not call client for each command
                        self.client(line, f)
        except: # Couldnt create connection
            pass

# ---- End of class, start of Main program ---- #

state = State(sys.argv[1:]) # Create an object from State class
listener = socket.socket() # Create a socket object
listener.bind(state.host) # Bind him to a host
listener.settimeout(0.2) # don't hang for 1 second
listener.listen(2) # Listen to 2 connections
print("Ready! Listening on ", state.host, "siblings: ", state.siblings)
while True: # Server loop
    try: # Try to accept request
        clientSocket, addr = listener.accept() # Accept the request
        threading.Thread(target=state.run, args=[clientSocket, addr]).start() # Send the request to a thread
    except socket.timeout: # If couldnt send the socket to timeout
        pass
