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
            customer = Customer(process["id"], process["customer-requests"])
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
        co
    except FileNotFoundError:
        print("input.json not found")
    except Exception as e:
        print("unknown error", e)

if __name__ == "__main__":
    theCallFunc()


    def combine_events(customer_events, branch_events):
        combined_events = []

        for customer_event in customer_events:
            customer_id = customer_event['id']
            for event in customer_event['events']:
                combined_event = {
                    'id': customer_id,
                    'customer-request-id': event['customer-request-id'],
                    'type': 'customer',
                    'logical_clock': event['logical_clock'],
                    'interface': event['interface'],
                    'comment': event['comment'],
                }
                combined_events.append(combined_event)

        for branch_event in branch_events:
            branch_id = branch_event['id']
            for event in branch_event['events']:
                combined_event = {
                    'id': branch_id,
                    'customer-request-id': event['customer-request-id'],
                    'type': 'branch',
                    'logical_clock': event['logical_clock'],
                    'interface': event['interface'],
                    'comment': event['comment'],
                }
                combined_events.append(combined_event)

        return combined_events

    # Example usage:
    customer_events = [...]  # Your list of customer events
    branch_events = [...]    # Your list of branch events

    combined_events = combine_events(customer_events, branch_events)

    # Print or write combined_events as needed
    print(combined_events)
