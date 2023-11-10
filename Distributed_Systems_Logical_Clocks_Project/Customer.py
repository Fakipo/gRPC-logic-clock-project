import grpc
import example_pb2_grpc
import example_pb2

class Customer:
    def __init__(self, id, events):
        self.id = id
        self.events = events
        self.recvMsg = list()
        self.stub = None
        self.logical_clock = 1

    # Setup gRPC channel & client stub for branch
    def createStub(self):
        port = str(60000 + self.id)
        channel = grpc.insecure_channel("localhost:" + port)
        self.stub = example_pb2_grpc.BranchStub(channel)

    # Send gRPC request for each event
    def executeEvents(self):
        for event in self.events:
            logical_clock = self.logical_clock
            id = event.get("id", self.id)  # Use 'id' from the event, or customer's 'id' if not present
            if event["interface"] != "query":
                response = self.stub.MsgDelivery(
                    example_pb2.MsgRequest(id=id, customer_request_id = event["customer-request-id"], interface=event["interface"], money=event["money"], logical_clock=logical_clock)
                )
            else:
                response = self.stub.MsgDelivery(
                    example_pb2.MsgRequest(id=id, customer_request_id = event["customer-request-id"], interface=event["interface"], logical_clock=logical_clock)
                )

            if response.interface != "query":
                stringToAppend = {
                    "customer-request-id": response.customer_request_id,
                    "logical_clock": response.logical_clock,
                    "interface": response.interface,
                    "comment": "event_sent from customer" + str(self.id),
                }
            else:
                stringToAppend = {
                    "customer-request-id": response.customer_request_id,
                    "logical_clock": response.logical_clock,
                    "interface": response.interface,
                    "comment": "event_sent from customer" + str(self.id),
                }

            logical_clock = max(logical_clock, response.logical_clock) + 1
            self.logical_clock = logical_clock
            self.recvMsg.append(stringToAppend)

    def output(self):
        return {"id": self.id, "type": "customer" ,"events": self.recvMsg}
