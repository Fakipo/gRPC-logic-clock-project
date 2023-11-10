import json
import multiprocessing
from time import sleep
from concurrent import futures
import grpc
import example_pb2_grpc
from Branch import Branch
from Customer import Customer

def read_input_file(input_file_path):
    try:
        with open(input_file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_file_path}'")
        return None

def initialize_processes_from_input(processes):
    branches = []
    branchIds = []
    branchProcessList = []

    for process in processes:
        if process["type"] == "branch":
            branch = Branch(process["id"], process["balance"], branchIds)
            branches.append(branch)
            branchIds.append(branch.id)

    for branch in branches:
        branch_process = multiprocessing.Process(target=startBranchServers, args=(branch,))
        branchProcessList.append(branch_process)
        branch_process.start()

    # Allow branch processes to start
    sleep(0.25)
    customers = []
    customerProcessList = []

    for process in processes:
        if process["type"] == "customer":
            customer = Customer(process["id"], process["customer-requests"])
            customers.append(customer)

    initiate_customers_from_list(customers, customerProcessList)
    await_customer_processes(customerProcessList)
    terminate_branch_processes(branchProcessList)

def initiate_customers_from_list(customers, customerProcessList):
    for customer in customers:
        customer_process = multiprocessing.Process(target=customerProcessing, args=(customer,))
        customerProcessList.append(customer_process)
        customer_process.start()
        sleep(0.4)

def terminate_branch_processes(branchProcessList):
    for branchProcess in branchProcessList:
        branchProcess.terminate()

def await_customer_processes(customerProcessList):
    for customerProcess in customerProcessList:
        customerProcess.join()

def startBranchServers(branch):
    branch.createStubs()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    example_pb2_grpc.add_BranchServicer_to_server(branch, server)
    server.add_insecure_port("localhost:" + str(60000 + branch.id))
    server.start()
    server.wait_for_termination()

def customerProcessing(customer):
    customer.createStub()
    customer.executeEvents()
    output = customer.output()
    writeOutputToFile(output)

def writeOutputToFile(output):
    with open("output.json", "a") as output_file:
        if not output_file.tell():
            # If the file is empty, write the starting bracket
            output_file.write("[\n")
        else:
            # If the file is not empty, add a comma to separate entries
            output_file.write(",\n")
        output_file.write(json.dumps(output, indent=2))

def closeOutputFile():
    with open("output.json", "a") as output_file:
        output_file.write("\n]")
def theCallFunc():
    try:
        # Read input data from input.json
        input_data = read_input_file('input.json')
        open("output.json", "w").close()
        initialize_processes_from_input(input_data)
        closeOutputFile()  # Close the JSON array with a closing bracket

    except FileNotFoundError:
        print("input.json not found")
    except Exception as e:
        print("unknown error", e)

if __name__ == "__main__":
    theCallFunc()
