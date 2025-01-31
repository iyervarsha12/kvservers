# kvservers
Built a fault-tolerant key-value store using the OmniPaxos library. The key-value store has of several key-value servers that use OmniPaxos for replication and fault-tolerance.
Clients can send three different RPCs to the key-value store: `Put(key, value)`, `Append(key, arg)`, and `Get(key)` - all are linearizable. The store maintains a database of key-value pairs.
The files with the source code are in server.go (to simulate KV servers), common.go and in client.go (to simulate the client).

To run, please use the command 'go test'. 
