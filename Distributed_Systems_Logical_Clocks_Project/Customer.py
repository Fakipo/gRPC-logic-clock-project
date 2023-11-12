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
        self.local_clock = 1

    def createStub(self):
        port = str(60000 + self.id)
        channel = grpc.insecure_channel("localhost:" + port)
        self.stub = example_pb2_grpc.BranchStub(channel)

    def executeEvents(self):
        self.logical_clock = 1
        for event in self.events:
            logical_clock = self.logical_clock
            id = event.get("id", self.id)

            response = self.stub.MsgDelivery(
                example_pb2.MsgRequest(
                    id=id,
                    customer_request_id=event["customer-request-id"],
                    interface=event["interface"],
                    logical_clock=logical_clock,
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
