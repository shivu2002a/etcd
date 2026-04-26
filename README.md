# etcd
pkill -9 -f java

Run : 
1. Boot the Cluster:
Run your 3 background node commands in Cloud Shell just like last time:

Bash
<java
mvn exec:java -Dexec.mainClass="com.auditraft.RaftApplication" -Dexec.args="Node-A 50051 localhost:50052 localhost:50053" &
mvn exec:java -Dexec.mainClass="com.auditraft.RaftApplication" -Dexec.args="Node-B 50052 localhost:50051 localhost:50053" &
mvn exec:java -Dexec.mainClass="com.auditraft.RaftApplication" -Dexec.args="Node-C 50053 localhost:50051 localhost:50052" &
/>

2. Note the Leader:
Watch the logs and see who becomes the 👑 LEADER. Let's assume Node-B won the election on port 50052.

3. Act as a Client (Using gRPCurl):
Because we don't have a frontend client yet, we will use a tool to send a raw gRPC request.
(Google Cloud Shell has grpcurl installed by default).

Send a PutConfig request to the Leader's port:

Bash
<bash
grpcurl -plaintext -d '{"key": "max_connections", "value": "5000"}' localhost:50052 auditraft.AuditConfigClientService/PutConfig
>
(You should see a log print out: 💾 [Node-B] LEADER accepted PutConfig: max_connections=5000)

grpcurl -plaintext -proto src/main/proto/audit_raft.proto -d '{"key": "max_connections"}' localhost:50053 auditraft.AuditConfigClientService/GetConfig

You'll get the leaderId 

grpcurl -plaintext  src/main/proto/audit_raft.proto -d '{"key": "max_connections"}' localhost:50052 auditraft.AuditConfigClientService/GetConfig

{
  "success": true,
  "value": "5000",
  "leaderId": "Node-B"
}


4. The Chaos Drop:
Kill the background Java processes:

Bash
<bash
pkill -f auditraft
/>

5. The Resurrection:
Restart the cluster using the exact same commands from Step 1. Wait for them to elect a leader.

Now, ask any node (even if it's a new leader) for the configuration:

Bash
<bash
grpcurl -plaintext -d '{"key": "max_connections"}' localhost:50051 auditraft.AuditConfigClientService/GetConfig
/>

If it returns 5000, you have successfully built a persistent, crash-tolerant distributed system.

Kill all java processes
<bash pkill -9 -f java>

Kill particular 

ss -tlnp | grep 50051
# Look for the PID in the output, then:
kill <PID>