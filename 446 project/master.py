from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import time
import threading


worker_addresses = ["http://localhost:23001/", 
                    "http://localhost:23002/" 
                    ]
# track the workers request and their status
worker_requests = [0, 0]  
worker_status = [True, True]  

def pingWorker(index):
    # ping at certain index of worker
    result = querywork(index,'ping')
    if result == True:
        return 'pong'
    

    
def choose_worker():
    # worker is picked by min amount of work requests
    worker_index = worker_requests.index(min(worker_requests))

    #if worker status is true pick them, if not use other worker
    if worker_status[worker_index] == True:
        return worker_index   
    else:
        return 1 - worker_index
    
    
def querywork(worker_index, method, *args):
    # send query and return result via worker
    worker_addr = worker_addresses[worker_index]
    try:
        worker = ServerProxy(worker_addr)
        result = getattr(worker, method)(*args)
        if not result['error']:
            return result['result']
    except ConnectionRefusedError:
        print(f"Worker {worker_index+1} at {worker_addr} is down.")
        worker_status[worker_index] = False
    return None


def getbylocation(location):
    # get location request to worker with least requests
    worker_index = choose_worker()
    query_result = querywork(worker_index, 'getbylocation', location)
    if query_result is not None:
        # update the workers request
        worker_requests[worker_index] += 1
        print(f"Worker {worker_index+1}: {worker_requests[worker_index]} requests")
        return query_result

    # return empty list when workers are uncappable
    print("All workers are down.")
    return []

def getbyname(name):
 # Forward query to the appropriate worker based on the first letter of the name
    worker_index = choose_worker()
    query_result = querywork(worker_index, 'getbyname', name)
    if query_result is not None:
        # Update the number of requests handled by the worker
        worker_requests[worker_index] += 1
        print(f"Worker {worker_index+1}: {worker_requests[worker_index]} requests")
        return query_result
    
       # return empty list when workers are uncappable
    print("All workers are down.")
    return []

def getbyyear(location, year):
    # Forward query to the worker with the lowest number of requests handled
    worker_index = choose_worker()
    query_result = querywork(worker_index, 'getbyyear', location, year)
    if query_result is not None:
            # update the workers request
            worker_requests[worker_index] += 1
            print(f"Worker {worker_index+1}: {worker_requests[worker_index]} requests")
            return query_result

        # return empty list when workers are uncappable
    print("All workers are down.")  
    return []

def monitorWorkers():
    # check status notifying
    while True:
        for i, worker_addr in enumerate(worker_addresses):
            if worker_status[i] == True:
                # send ping request
                worker = ServerProxy(worker_addr)
                response = pingWorker(i)
                if response != "pong":
                    #Worker is not responding properly, mark as down
                    print(f"Worker {i+1} is down.")
                    worker_status[i] = False
            else:
                worker = ServerProxy(worker_addr)
                response = pingWorker(i)
                if response == "pong":
                    print(f"Worker {i+1} is up.")
                    worker_status[i] = True
                    worker_requests[0] = 0
                    worker_requests[1] = 0
                
        # timer to check
        time.sleep(1)
    

def main():
    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Listening on port {port}...")
    

    # Register RPC functions
    server.register_function(getbylocation)
    server.register_function(getbyname)
    server.register_function(getbyyear)
    monitor_thread = threading.Thread(target=monitorWorkers)
    monitor_thread.daemon = True
    monitor_thread.start()
    server.serve_forever()
   

if __name__ == '__main__':
    main()

