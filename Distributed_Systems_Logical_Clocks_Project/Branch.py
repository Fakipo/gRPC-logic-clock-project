import grpc
import example_pb2_grpc
import example_pb2

class Branch(example_pb2_grpc.BranchServicer):
    def __init__(self, id, balance, branches):
        self.id = id
        self.balance = balance
        self.branches = branches
        self.stubList = list()
        self.recvMsg = list()
        self.logical_clock = 1


    # Setup gRPC channel & client stub for each branch
    def createStubs(self):
        self.stubList = [
            example_pb2_grpc.BranchStub(grpc.insecure_channel(f"localhost:{60000 + branchId}"))
            for branchId in self.branches if branchId != self.id
        ]

    def extendedMsgForProp(self, request, propagate):
        result = "success"
        logical_clock = self.logical_clock

        if request.money < 0:
            result = "fail"
        elif request.interface == "query":
            return example_pb2.MsgResponse(interface=request.interface, money=self.balance, logical_clock=logical_clock)
        elif request.interface == "deposit":
            self.balance += request.money
            logical_clock += 1
            if propagate:
                self.Propagate_Deposit(request, logical_clock)
        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money
                logical_clock += 1
                if propagate:
                    self.Propagate_Withdraw(request, logical_clock)
            else:
                result = "fail"
        else:
            result = "fail"

        msg = {
            "interface": request.interface,
            "result": result,
            "logical_clock": logical_clock,
        }

        if request.interface != "query":
            msg["result"] = result
        else:
            msg["money"] = request.money
        self.recvMsg.append(msg)
        return example_pb2.MsgResponse(interface=request.interface, customer_request_id = request.customer_request_id, result=result, logical_clock=logical_clock)

    def Propagate_Withdraw(self, request, logical_clock):
        for stub in self.stubList:
            stub.MsgPropagation(example_pb2.MsgRequest(id=request.id, interface="withdraw", customer_request_id = request.customer_request_id, logical_clock=logical_clock))

    def Propagate_Deposit(self, request, logical_clock):
        for stub in self.stubList:
            stub.MsgPropagation(example_pb2.MsgRequest(id=request.id, interface="deposit", customer_request_id = request.customer_request_id, logical_clock=logical_clock))

    def MsgDelivery(self, request, context):
        self.logical_clock += 1
        return self.extendedMsgForProp(request, True)

    def MsgPropagation(self, request, context):
        self.logical_clock = max(self.logical_clock, request.logical_clock) + 1
        return self.extendedMsgForProp(request, False)
