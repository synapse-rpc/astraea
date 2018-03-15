class RpcClient:
    def __init__(self, synapse):
        s = synapse
        ch = s.create_channel(desc="RpcClient")
