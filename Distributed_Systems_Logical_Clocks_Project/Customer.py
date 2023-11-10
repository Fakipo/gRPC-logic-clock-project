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

    def createStub(self):
        port = str(60000 + self.id)
        channel = grpc.insecure_channel("localhost:" + port)
        self.stub = example_pb2_grpc.BranchStub(channel)

    def executeEvents(self):
        for event in self.events:
            logical_clock = self.logical_clock
            id = event.get("id", self.id)

            response = self.stub.MsgDelivery(
                example_pb2.MsgRequest(
                    id=id,
                    customer_request_id=event["customer-request-id"],
                    interface=event["interface"],
                    money=event.get("money", 0),
                    logical_clock=logical_clock,
                )
            )

            # Customer output
            stringToAppend = {
                "customer-request-id": event["customer-request-id"],
                "logical_clock": response.logical_clock,
                "interface": event["interface"],
                "comment": f"event_recv from customer {id}",
            }
            logical_clock = max(logical_clock, response.logical_clock) + 1
            self.logical_clock = logical_clock
            self.recvMsg.append(stringToAppend)

    def output(self):
        combined_output = [
            {"id": self.id, "type": "customer", "events": self.recvMsg.copy()},
            # {"id": self.id, "type": "branch", "events": self.collectBranchLogicalClocks([self.stub])},
        ]
        return combined_output

    # def collectBranchLogicalClocks(self, branch_stubs):
    #     branch_events = []
    #     for stub in branch_stubs:
    #         response = stub.GetBranchDetails(example_pb2.Empty())
    #         branch_id = response.id
    #         branch_logical_clocks = response.logical_clocks
    #         branch_events.extend(
    #             {
    #                 "customer-request-id": event.customer_request_id,
    #                 "logical_clock": event.logical_clock,
    #                 "interface": event.interface,
    #                 "comment": f"event_recv from branch {branch_id}",
    #             }
    #             for event in branch_logical_clocks
    #         )
    #     return branch_events
