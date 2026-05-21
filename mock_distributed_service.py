import hashlib

class StorageNode:
    """Simulates an independent database server/node."""
    def __init__(self, name):
        self.name = name
        self.storage = {}

    def put(self, key, value):
        self.storage[key] = value
        print(f"[{self.name}] Stored -> {key}: {value}")

    def get(self, key):
        return self.storage.get(key, None)


class DistributedConsistentHashRing:
    """Simulates a Coordinator that shards data across nodes using consistent hashing."""
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas  # Virtual nodes to ensure even distribution
        self.ring = {}            # sorted_hash_value -> node
        self.sorted_keys = []     # List of sorted hashes

        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        """Generates an integer hash for a given string key."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        """Adds a node and its virtual replicas to the ring."""
        for i in range(self.replicas):
            val = f"{node.name}-replica-{i}"
            key_hash = self._hash(val)
            self.ring[key_hash] = node
            self.sorted_keys.append(key_hash)
        self.sorted_keys.sort()

    def get_node(self, key):
        """Routes the key to the nearest node on the ring clockwise."""
        if not self.ring:
            return None
        
        key_hash = self._hash(key)
        # Find the first node hash greater than the key hash
        for node_hash in self.sorted_keys:
            if key_hash <= node_hash:
                return self.ring[node_hash]
        
        # If it falls past the last node, wrap around to the first
        return self.ring[self.sorted_keys[0]]


# --- Simulation Running ---
if __name__ == "__main__":
    print("--- Initializing Mock Distributed Cluster ---")
    # 1. Spin up 3 mock data centers/nodes
    node_A = StorageNode("Node-US-East")
    node_B = StorageNode("Node-EU-West")
    node_C = StorageNode("Node-AP-South")

    # 2. Setup the Coordinator Router
    cluster = DistributedConsistentHashRing([node_A, node_B, node_C])

    # 3. Simulate client Write requests (Data Sharding)
    print("\n--- Writing Data (Sharding Context) ---")
    test_data = {
        "user_101": "Peyali",
        "user_202": "Alex",
        "session_xyz": "Active",
        "config_dark_mode": "True"
    }

    for k, v in test_data.items():
        target_node = cluster.get_node(k)
        target_node.put(k, v)

    # 4. Simulate client Read requests (Routing)
    print("\n--- Reading Data (Routing Context) ---")
    query_key = "user_101"
    target_node = cluster.get_node(query_key)
    retrieved_val = target_node.get(query_key)
    print(f"Routed request for '{query_key}' to [{target_node.name}]. Found: {retrieved_val}")
