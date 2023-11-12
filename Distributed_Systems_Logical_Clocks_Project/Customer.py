import grpc
import example_pb2_grpc
import example_pb2


class Customer:
    def __init__(self, id, events, branch_to_customer_req_id):
        self.id = id
        self.events = events
        self.recvMsg = list()
        self.stub = None
        self.logical_clock = 1
        self.local_clock = 1
        self.branch_to_customer_req_id = branch_to_customer_req_id
    def createStub(self):
        port = str(60000 + self.id)
        channel = grpc.insecure_channel("localhost:" + port)
        self.stub = example_pb2_grpc.BranchStub(channel)

    def executeEvents(self):
        self.logical_clock = 1
        for event in self.events:
            logical_clock = self.logical_clock
            id = event.get("id", self.id)


            customer_request_map = example_pb2.MsgRequest()

            # Populate branch_id and request_ids in CustomerRequestMap
            customer_request_map.branch_to_customer_req_id_map.extend([
                example_pb2.CustomerRequestMap(
                    branch_id=branch_id,
                    corresponding_customer_request_id=request_ids
                ) for branch_id, request_ids in self.branch_to_customer_req_id.items()
            ])
            response = self.stub.MsgDelivery(
                example_pb2.MsgRequest(
                    id=id,
                    customer_request_id=event["customer-request-id"],
                    interface=event["interface"],
                    logical_clock=logical_clock,
                branch_to_customer_req_id_map = customer_request_map.branch_to_customer_req_id_map
                )
            )

            # Customer output
            stringToAppend = {
                "customer-request-id": event["customer-request-id"],
                "logical_clock": self.local_clock,
                "interface": event["interface"],
                "comment": f"event_sent from customer {id}",
            }
            print('logical clock in customer is =' , response.logical_clock)
            self.local_clock += 1
            self.logical_clock = response.logical_clock + 1
            self.recvMsg.append(stringToAppend)

    def output(self):
        combined_output = [
            {"id": self.id, "type": "customer", "events": self.recvMsg.copy()},
        ]
        return combined_output
