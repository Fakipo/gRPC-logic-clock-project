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
    branch_stubs = []

    branch_to_customer_req_id_map = {}

    for entry in processes:
        if entry["type"] == "customer":
            customer_id = entry["id"]
            request_ids = [req["customer-request-id"] for req in entry["customer-requests"]]
            branch_to_customer_req_id_map[customer_id] = request_ids

    print(branch_to_customer_req_id_map)


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

    branch_stubs = branch.getStubs()

    customers = []
    customerProcessList = []

    for process in processes:
        if process["type"] == "customer":
            customer = Customer(process["id"], process["customer-requests"], branch_to_customer_req_id_map)
            customers.append(customer)

    initiate_customers_from_list(customers, customerProcessList, branch_stubs)
    await_customer_processes(customerProcessList)
    terminate_branch_processes(branchProcessList)

def initiate_customers_from_list(customers, customerProcessList, branch_stubs):
    for customer in customers:
        customer_process = multiprocessing.Process(target=customerProcessing, args=(customer,branch_stubs))
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

    sleep(0.5 * branch.id)
    output = ({"id": branch.id, "type":"branch","events": branch.output()})
    writeOutputToFile2(output)
    sleep(1)
    server.wait_for_termination()

def customerProcessing(customer, branch_stubs):
    customer.createStub()

    # Execute events and get combined output
    customer.executeEvents()
    combined_output = customer.output()
    # Write combined output to output files
    writeOutputToFile(combined_output[0])
    # writeOutputToFile2(combined_output[1])
    sleep(2)

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

def writeOutputToFile2(output):
    with open("output2.json", "a") as output_file:
        if not output_file.tell():
            output_file.write("[\n")
        else:
            output_file.write(",\n")

        formatted_output = json.dumps(output, indent=2)
        output_file.write(formatted_output)
def closeOutputFile2():
    with open("output2.json", "a") as output_file:
        output_file.write("\n]")

def create_output3(customer_events, branch_events):
    output3 = []

    for customer_event in customer_events:
        customer_request_id = customer_event['events'][0]['customer-request-id']

        # Add customer event to output3
        output3.append({
            'id': customer_event['id'],
            'customer-request-id': customer_request_id,
            'type': 'customer',
            'logical_clock': customer_event['events'][0]['logical_clock'],
            'interface': customer_event['events'][0]['interface'],
            'comment': customer_event['events'][0]['comment']
        })

        # Add corresponding branch events to output3
        for branch_event in branch_events:
            if branch_event['events'][0]['customer-request-id'] == customer_request_id:
                output3.append({
                    'id': branch_event['id'],
                    'customer-request-id': customer_request_id,
                    'type': 'branch',
                    'logical_clock': branch_event['events'][1]['logical_clock'],
                    'interface': branch_event['events'][1]['interface'],
                    'comment': branch_event['events'][1]['comment']
                })

    return output3

def theCallFunc():
    try:
        # Read input data from input.json
        input_data = read_input_file('input.json')
        open("output.json", "w").close()
        open("output2.json", "w").close()
        initialize_processes_from_input(input_data)
        closeOutputFile()  # Close the JSON array with a closing bracket
        closeOutputFile2() # Close the json array with a closing bracket for output file number 2

        sleep(2)

        with open('output.json', 'r') as file:
            output_json = json.load(file)

        with open('output2.json', 'r') as file:
            output2_json = json.load(file)

        # Generate output3.json
            output3_json = create_output3(output_json, output2_json)

        with open('output3.json', 'w') as file:
            json.dump(output3_json, file, indent=2)

    except FileNotFoundError:
        print("input.json not found")
    except Exception as e:
        print("unknown error", e)

if __name__ == "__main__":
    theCallFunc()
