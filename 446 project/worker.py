from xmlrpc.server import SimpleXMLRPCServer
import sys
import json

# Storage of data
data_table = {}

# allows pinging
def pingWorker():
    return {
        'error': False,
        'result': True
    }
    

def load_data(group):
    # load data depending on am or nz
    global data_table
    # open both files and merge them
    if group == 'am':
        with open('data-am.json') as f1, open('data-nz.json') as f2:
                   
            data_table = json.load(f1) | json.load(f2) 
    elif group == "nz":
        with open('data-nz.json') as f1, open('data-am.json') as f2:
            data_table = json.load(f1) | json.load(f2)
    
def getbyname(name):
    # append by users name
    try:
        result = data_table[name]
        return {
            'error': False,
            'result': [result]
        }
    except KeyError:
        return {
            'error': False,
            'result': []
        }
            
def getbylocation(location):
    # look for location
    if not data_table:
        return {
            'error': True,
            'result': 'No data loaded'
        }
    # appends user location if no error
    results = []
    for record in data_table.values():
        if record['location'] == location:
            results.append(record)
    # if no error print results
    return {
        'error': False,
        'result': results
    }

def getbyyear(location, year):
    # look for records of location and year
    if not data_table:
        return {
            'error': True,
            'result': 'No data loaded'
        }
    # append location and year
    results = []
    for record in data_table.values():
        if record['location'] == location and record['year'] == year:
            results.append(record)
    # if no error print results
    return {
        'error': False,
        'result': results
    }


def main():
    # if argument is less than 3 tell user correct input
    if len(sys.argv) < 3:
        print('Usage: worker.py <port> <group: am or nz>')
        sys.exit(0)
    # set port as first argument and group as second argument
    port = int(sys.argv[1])
    group = sys.argv[2]
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Listening on port {port}...")
    # loads data into group
    load_data(group) 

    # register RPC functions
    server.register_function(getbyname)
    server.register_function(getbylocation)
    server.register_function(getbyyear)
    server.register_function(pingWorker)

    server.serve_forever()

if __name__ == '__main__':
    main()