import socket
import time
 
def Main():
        host = 'localhost'
        port = 9999
         
        mySocket = socket.socket()
        mySocket.connect((host,port))
         
        while 1:
            with open("./../src/data/2018-example.csv") as fp:
                line = fp.readline()
                while line:
                    time.sleep(3)
                    line = fp.readline()

                    mySocket.send(line.strip().encode())

                    data = mySocket.recv(1024).decode()
                    print (" - " + data)
                 
        mySocket.close()
 
if __name__ == '__main__':
    Main()