# Notes for Lab3

## Duplicate RPC detection (Lab 3)

Draw clients, k/v service, OmniPaxos

### What should a client do if a Put or Get RPC times out?
  i.e. Call() returns false
  if server is dead, or request was dropped: re-send
  if server executed, but reply was lost: re-send is dangerous

### Problem:
  these two cases look the same to the client (no reply)
  if already executed, client still needs the result

### Idea: duplicate RPC detection
  let's have the k/v service detect duplicate client requests
  client picks a unique ID for each request, sends in RPC
    same ID in re-sends of same RPC
  k/v service maintains a "duplicate table" indexed by ID
  makes a table entry for each RPC
    after executing, record reply content in duplicate table
  if 2nd RPC arrives with the same ID, it's a duplicate
    generate reply from the value in the table

### How does a new leader get the duplicate table?
  put ID in logged operations handed to OmniPaxos
  all replicas should update their duplicate tables as they execute
  so the information is already there if they become leader

### If server crashes how does it restore its table?
  replay of log will populate the table

### What if a duplicate request arrives before the original executes?
  could just call Proposal() (again)
  it will probably appear twice in the log (same client ID, same seq #)
  when cmd appears on applyCh, don't execute if table says already seen

### Idea to keep the duplicate table small
  one table entry per client, rather than one per RPC
  each client has only one RPC outstanding at a time
  each client numbers RPCs sequentially
  when server receives client RPC #10,
    it can forget about client's lower entries
    since this means client won't ever re-send older RPCs

### Some details:
  each client needs a unique client ID -- perhaps a 64-bit random number
  client sends client ID and seq # in every RPC
    repeats seq # if it re-sends
  duplicate table in k/v service indexed by client ID
    contains just seq #, and value if already executed
  RPC handler first checks table, only Proposal()s if seq # > table entry
  each log entry must include client ID, seq #
  when operation appears on applyCh
    update the seq # and value in the client's table entry
    wake up the waiting RPC handler (if any)

### But wait!
  the k/v server is now returning old values from the duplicate table
  what if the reply value in the table is no longer up to date?
  is that OK?

```
example:
  C1           C2
  --           --
  put(x,10)
               first send of get(x), reply(10) dropped
  put(x,20)
               re-sends get(x), server gets 10 from table, not 20

get(x) and put(x,20) run concurrently, so could run before or after;
so, returning the remembered value 10 is correct
```