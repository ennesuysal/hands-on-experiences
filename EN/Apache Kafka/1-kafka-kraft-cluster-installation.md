# 3-Node Bare Metal Kafka Cluster Setup (KRaft + SASL_SSL)

This document covers the installation, testing and troubleshooting of a 3-node Apache Kafka 4.x cluster running without ZooKeeper (in KRaft mode), with TLS encryption, SCRAM-SHA-512 authentication and ACL-based authorization.

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Prerequisites](#2-prerequisites)
3. [TLS Certificates](#3-tls-certificates)
4. [server.properties](#4-serverproperties)
5. [Storage Formatting](#5-storage-formatting)
6. [systemd Service](#6-systemd-service)
7. [Cluster Verification](#7-cluster-verification)
8. [User and ACL Management](#8-user-and-acl-management)
9. [Authentication Tests](#9-authentication-tests)
10. [Troubleshooting](#10-troubleshooting)
11. [Additional Information](#11-additional-information)

---

## 1. Architecture

Each node acts as both **broker** and **controller** (combined mode). A quorum of 3 controllers tolerates the loss of 1 node.

| Node | node.id | IP | Hostname |
|---|---|---|---|
| 1 | 1 | 10.1.1.20 | kafka-1 |
| 2 | 2 | 10.1.1.21 | kafka-2 |
| 3 | 3 | 10.1.1.22 | kafka-3 |

| Listener | Port | Protocol | Purpose |
|---|---|---|---|
| `SASL_SSL` | 9092 | TLS + SCRAM-SHA-512 | Client and inter-broker traffic |
| `CONTROLLER` | 9093 | mTLS (mutual certificates) | KRaft controller quorum |

> **Note:** SCRAM cannot be used on the controller listener. SCRAM users are stored in the metadata log, which is not accessible until the quorum is up. For this reason, controller traffic is secured with mTLS.

**Identity / authorization model:**

- Inter-broker and admin access: SCRAM user `admin`
- Controller-to-controller: certificate CN → `User:kafka-1`, `User:kafka-2`, `User:kafka-3`
- Authorization: `StandardAuthorizer`; any request without a matching ACL is denied

---

## 2. Prerequisites

The following steps are performed on **all three nodes**.

### 2.1 Java and Kafka

Kafka 4.x brokers require Java 17 or later.

```bash
apt install -y openjdk-17-jre-headless     # or 21
tar -xzf kafka_2.13-<VERSION>.tgz -C /opt
ln -s /opt/kafka_2.13-<VERSION> /opt/kafka
```

### 2.2 User and directories

```bash
useradd -r -s /usr/sbin/nologin kafka
mkdir -p /data/kafka /var/log/kafka /etc/kafka/ssl
chown -R kafka:kafka /data/kafka /var/log/kafka /etc/kafka /opt/kafka/config
```

### 2.3 /etc/hosts

Ubuntu/Debian maps the hostname to `127.0.1.1` by default. The node then resolves its own hostname to loopback, which causes connection and certificate verification issues. For this reason, the `127.0.1.1` line is removed:

```text
# 127.0.1.1 kafka-1   ← should be removed or commented out
10.1.1.20 kafka-1
10.1.1.21 kafka-2
10.1.1.22 kafka-3
```

### 2.4 OS settings

```bash
cat >> /etc/sysctl.d/90-kafka.conf <<'EOF'
vm.swappiness=1
vm.max_map_count=262144
EOF
sysctl --system
```

- XFS or ext4 is used for the data disk, and the disk is mounted with `noatime`.
- The file descriptor limit is set in the systemd unit (`LimitNOFILE`).

### 2.5 Firewall

```bash
firewall-cmd --permanent --add-port=9092/tcp --add-port=9093/tcp
firewall-cmd --reload
```

Restricting port 9093 to the other Kafka nodes only is more secure.

---

## 3. TLS Certificates

If a corporate PKI exists, it is used. Otherwise, an internal CA is created.

### 3.1 CA (once)

```bash
openssl req -x509 -newkey rsa:4096 -days 3650 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=Kafka-Internal-CA"
```

The `ca.key` file should be stored in a secure location.

### 3.2 Node certificates

Repeated for each node, changing `N` and `IP`:

```bash
N=kafka-1; IP=10.1.1.20

openssl req -newkey rsa:2048 -nodes -keyout $N.key -out $N.csr -subj "/CN=$N"

openssl x509 -req -in $N.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 -out $N.crt \
  -extfile <(printf "subjectAltName=DNS:$N,IP:$IP\nextendedKeyUsage=serverAuth,clientAuth")

openssl pkcs12 -export -in $N.crt -inkey $N.key -certfile ca.crt -name $N \
  -out $N.keystore.p12 -passout pass:'KEYSTORE_PASS'
```

Key points:

- The **SAN** must contain both hostname and IP. Clients connect by hostname, controller voters by IP.
- The hostname must be **exactly the same** as in `advertised.listeners` (`kafka-1`, not `kafka1`).
- The **`clientAuth`** EKU is required for mTLS.

SAN check:

```bash
openssl x509 -in kafka-1.crt -noout -ext subjectAltName
```

### 3.3 Truststore (once)

```bash
keytool -importcert -alias kafka-ca -file ca.crt -keystore truststore.p12 \
  -storetype PKCS12 -storepass 'TRUST_PASS' -noprompt
```

### 3.4 Distribution

Each node receives its own keystore and the shared truststore:

```bash
cp kafka-1.keystore.p12 truststore.p12 /etc/kafka/ssl/
chown -R kafka:kafka /etc/kafka/ssl
chmod 600 /etc/kafka/ssl/*
```

---

## 4. server.properties

File: `/opt/kafka/config/server.properties`

### 4.1 Node 1

```properties
############################ KRaft / Roles ############################
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@10.1.1.20:9093,2@10.1.1.21:9093,3@10.1.1.22:9093

############################ Listeners ############################
listeners=SASL_SSL://10.1.1.20:9092,CONTROLLER://10.1.1.20:9093
advertised.listeners=SASL_SSL://kafka-1:9092
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=SASL_SSL
controller.listener.names=CONTROLLER

############################ Disk ############################
log.dirs=/data/kafka

############################ TLS ############################
ssl.keystore.type=PKCS12
ssl.keystore.location=/etc/kafka/ssl/kafka-1.keystore.p12
ssl.keystore.password=KEYSTORE_PASS
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/ssl/truststore.p12
ssl.truststore.password=TRUST_PASS
ssl.enabled.protocols=TLSv1.3,TLSv1.2
ssl.endpoint.identification.algorithm=https

# Mutual certificate authentication is required on the controller listener
listener.name.controller.ssl.client.auth=required
# Maps the "CN=kafka-1" certificate to the "User:kafka-1" principal
ssl.principal.mapping.rules=RULE:^CN=([a-zA-Z0-9.-]*).*$/$1/,DEFAULT

############################ SASL / SCRAM ############################
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="ADMIN_PASS";

############################ Authorization ############################
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:kafka-1;User:kafka-2;User:kafka-3
allow.everyone.if.no.acl.found=false

############################ Replication / Durability ############################
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
auto.create.topics.enable=false
num.partitions=6

############################ Retention ############################
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

############################ Performance ############################
num.network.threads=8
num.io.threads=16
num.replica.fetchers=4
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600
num.recovery.threads.per.data.dir=4
```

### 4.2 Per-node differences

Only the following lines are node-specific; everything else is identical across the three files.

| Setting | Node 1 | Node 2 | Node 3 |
|---|---|---|---|
| `node.id` | `1` | `2` | `3` |
| `listeners` | `SASL_SSL://10.1.1.20:9092,CONTROLLER://10.1.1.20:9093` | `SASL_SSL://10.1.1.21:9092,CONTROLLER://10.1.1.21:9093` | `SASL_SSL://10.1.1.22:9092,CONTROLLER://10.1.1.22:9093` |
| `advertised.listeners` | `SASL_SSL://kafka-1:9092` | `SASL_SSL://kafka-2:9092` | `SASL_SSL://kafka-3:9092` |
| `ssl.keystore.location` | `.../kafka-1.keystore.p12` | `.../kafka-2.keystore.p12` | `.../kafka-3.keystore.p12` |

`controller.quorum.voters` must be **exactly the same on all three nodes**.

> **Tip:** `0.0.0.0` can be used for `listeners`; the node then listens on all interfaces and the line becomes identical on every node. `advertised.listeners`, however, must always be a real address reachable by clients.

### 4.3 File permissions and format

```bash
chown kafka:kafka /opt/kafka/config/server.properties
chmod 600 /opt/kafka/config/server.properties
```

The file **must not** contain a BOM or Windows line endings (CRLF) (see [10.3](#103-missing-required-configuration-processroles)). Keeping the JAAS setting on a single line prevents `\` line-continuation errors.

---

## 5. Storage Formatting

In KRaft, storage directories must be formatted before startup. The inter-broker SCRAM user (`admin`) is created **during formatting**, before the cluster comes up.

### 5.1 Generating the cluster ID (once)

```bash
/opt/kafka/bin/kafka-storage.sh random-uuid
```

### 5.2 Formatting all three nodes with the same ID

```bash
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
  -t <CLUSTER_ID> \
  -c /opt/kafka/config/server.properties \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=ADMIN_PASS]'
```

Rules:

- The cluster ID and password must be **the same on all three nodes**.
- The password in `--add-scram` must be **exactly the same** as in `server.properties` and `admin.properties`.
- Formatting should be done as the `kafka` user (prevents permission issues).
- SCRAM records are **not added** to directories skipped with `--ignore-formatted`.

Check:

```bash
cat /data/kafka/meta.properties    # cluster.id, node.id, version=1
```

---

## 6. systemd Service

File: `/etc/systemd/system/kafka.service`

```ini
[Unit]
Description=Apache Kafka (KRaft)
Documentation=https://kafka.apache.org/documentation/
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xms6g -Xmx6g"
Environment="LOG_DIR=/var/log/kafka"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties

# Graceful (controlled) shutdown: waits until leadership is transferred
KillSignal=SIGTERM
TimeoutStopSec=180
SuccessExitStatus=143
Restart=on-failure
RestartSec=10

# Limits
LimitNOFILE=200000
LimitNPROC=65536

# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=/data/kafka /var/log/kafka
ProtectKernelTunables=true
ProtectControlGroups=true

[Install]
WantedBy=multi-user.target
```

Notes:

- In production, Kafka **should not run as root**.
- `ExecStop` is intentionally omitted: `kafka-server-stop.sh` kills all Kafka processes on the machine. The SIGTERM sent by systemd already triggers a controlled shutdown.
- With `ProtectSystem=strict`, all data disks must be added to `ReadWritePaths` if there are several.
- A 6 GB heap is sufficient; the rest of the memory should be left to the OS page cache.

Enabling:

```bash
systemctl daemon-reload
systemctl enable --now kafka
journalctl -u kafka -f
```

On configuration changes, nodes are restarted **one at a time**, and the quorum status is checked after each restart.

---

## 7. Cluster Verification

### 7.1 Admin client config

File: `/etc/kafka/admin.properties`

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="ADMIN_PASS";
ssl.truststore.type=PKCS12
ssl.truststore.location=/etc/kafka/ssl/truststore.p12
ssl.truststore.password=TRUST_PASS
```

```bash
chmod 600 /etc/kafka/admin.properties
```

### 7.2 TLS

```bash
openssl s_client -connect kafka-1:9092 -CAfile ca.crt </dev/null 2>/dev/null | grep -E 'Verify return|Protocol'
```

Expected: `Verify return code: 0 (ok)` and `TLSv1.3`.

### 7.3 Quorum status

```bash
bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/admin.properties describe --status
```

Expected output:

```text
LeaderId:               1
MaxFollowerLag:         0
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://10.1.1.20:9093"]},
                         {"id": 2, "endpoints": ["CONTROLLER://10.1.1.21:9093"]},
                         {"id": 3, "endpoints": ["CONTROLLER://10.1.1.22:9093"]}]
```

Checklist:

- `LeaderId` is set
- All three voters are listed with **the correct IPs**
- `MaxFollowerLag` is close to 0

For per-voter details:

```bash
bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/admin.properties describe --replication
```

### 7.4 Broker registrations

```bash
bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/admin.properties | grep -E '^kafka-'
```

Expected:

```text
kafka-1:9092 (id: 1 rack: null isFenced: false) -> (
kafka-2:9092 (id: 2 rack: null isFenced: false) -> (
kafka-3:9092 (id: 3 rack: null isFenced: false) -> (
```

`isFenced: false` indicates that the broker is following the metadata and is ready to serve traffic.

---

## 8. User and ACL Management

### 8.1 Creating an application user

```bash
bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --alter --entity-type users --entity-name app-orders \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=APP_PASS]'
```

### 8.2 Defining ACLs

Producer + consumer permissions (example: `orders` topic, `orders-consumers` group):

```bash
bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --add --allow-principal User:app-orders --producer --consumer \
  --topic orders --group orders-consumers
```

### 8.3 Listing ACLs

```bash
bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties --list
```

### 8.4 Application client config

```properties
bootstrap.servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="app-orders" password="APP_PASS";
ssl.truststore.type=PKCS12
ssl.truststore.location=/path/to/truststore.p12
ssl.truststore.password=TRUST_PASS
acks=all
```

Together, `acks=all` and `min.insync.replicas=2` allow writes to continue without data loss when 1 node is lost.

---

## 9. Authentication Tests

### 9.1 Preparation

```bash
# Test topic
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --create --topic auth-test --partitions 3 --replication-factor 3

bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --describe --topic auth-test

# Test user and ACL
bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --alter --entity-type users --entity-name app-test \
  --add-config 'SCRAM-SHA-512=[password=AppTest123]'

bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --add --allow-principal User:app-test --producer --consumer \
  --topic auth-test --group test-group

# Client config for the test user
sed -e 's/username="admin"/username="app-test"/' \
    -e 's/password="ADMIN_PASS"/password="AppTest123"/' \
    /etc/kafka/admin.properties > /tmp/app.properties
```

In the `describe` output, 3 brokers should appear in the `Replicas` and `Isr` fields for each partition.

### 9.2 Positive test: produce / consume

```bash
# Terminal 1
bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 \
  --producer.config /tmp/app.properties --topic auth-test

# Terminal 2 (through a different broker)
bin/kafka-console-consumer.sh --bootstrap-server kafka-2:9092 \
  --consumer.config /tmp/app.properties --topic auth-test \
  --group test-group --from-beginning
```

The message sent should appear on the consumer.

### 9.3 Negative tests

**All of the following should be rejected.**

| Test | How | Expected result |
|---|---|---|
| Wrong password | The password in `/tmp/app.properties` is changed and the producer is run | `SaslAuthenticationException: Authentication failed` |
| Unauthenticated connection | `bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list` (without config) | No connection (timeout / disconnect) |
| Topic without ACL | Commands below | `TopicAuthorizationFailedException` |
| Group without ACL | The consumer is run with `--group other-group` | `GroupAuthorizationFailedException` |
| Controller access without certificate | Command below | TLS handshake failure |

Topic without ACL:

```bash
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --create --topic forbidden-topic --partitions 1 --replication-factor 3

bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 \
  --producer.config /tmp/app.properties --topic forbidden-topic
```

Controller port (without certificate):

```bash
openssl s_client -connect 10.1.1.20:9093 -CAfile ca.crt </dev/null
```

For comparison, the connection should succeed when `-cert kafka-1.crt -key kafka-1.key` is added.

### 9.4 Cleanup

```bash
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties --delete --topic auth-test
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties --delete --topic forbidden-topic

bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --remove --allow-principal User:app-test --producer --consumer \
  --topic auth-test --group test-group --force

bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --alter --entity-type users --entity-name app-test --delete-config 'SCRAM-SHA-512'

rm /tmp/app.properties
```

---

## 10. Troubleshooting

Errors encountered during installation and their solutions.

### 10.1 `No readable meta.properties files found`

Kafka cannot find or read the `meta.properties` file under `log.dirs`.

| Cause | Solution |
|---|---|
| Storage not formatted | The format command in Section 5 is run |
| Formatted with a different config | The same file is used for formatting and startup; the `log.dirs` path is checked |
| Formatted as root, Kafka running as `kafka` | `chown -R kafka:kafka /data/kafka` |
| `log.dirs` under `/tmp`, wiped on reboot | A persistent directory is used |

### 10.2 `the loader is still catching up because we still don't know the high water mark yet`

This is not an error; the node is waiting for the controller quorum to form. At least 2 of the 3 controllers must be up. If it persists while the other nodes are running:

```bash
nc -vz 10.1.1.21 9093                          # port reachability / firewall
ss -ltnp | grep 9093                           # listening on the correct IP?
grep cluster.id /data/kafka/meta.properties    # identical on all three nodes?
```

It should also be verified that `controller.quorum.voters` is identical on all three nodes.

### 10.3 `Missing required configuration "process.roles"`

The line appears in the file, but Kafka cannot read it. Possible causes:

1. **systemd is reading a different file:**
   ```bash
   systemctl cat kafka | grep ExecStart
   ```
2. **The previous line ends with `\`** and swallows this line:
   ```bash
   grep -n '\\$' /opt/kafka/config/server.properties
   ```
   Only the JAAS line (if written on multiple lines) should appear.
3. **BOM / CRLF characters** (if the file was edited on Windows):
   ```bash
   head -1 /opt/kafka/config/server.properties | cat -A
   ```
   If the line starts with `M-oM-;M-?` or lines end with `^M`:
   ```bash
   sed -i '1s/^\xEF\xBB\xBF//' /opt/kafka/config/server.properties
   sed -i 's/\r$//' /opt/kafka/config/server.properties
   ```

After fixing, a manual test run first is useful:

```bash
sudo -u kafka /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```

### 10.4 Kafka tries to write to `/tmp/kafka-logs`

`/tmp/kafka-logs` is the **default value** of `log.dirs`. It indicates that Kafka is not reading the `log.dirs` line.

```bash
grep -nE 'process.roles|log.dirs|node.id' /opt/kafka/config/server.properties
```

If the line is missing, it is added (`log.dirs=/data/kafka`); if present, the formatting issues in 10.3 are checked. Afterwards, `/tmp/kafka-logs` is removed and `/data/kafka` must be formatted.

### 10.5 `Authentication failed ... invalid credentials with SASL mechanism SCRAM-SHA-512`

TLS works, but the SCRAM credentials do not match.

1. **Do the passwords match?**
   ```bash
   grep -o 'password="[^"]*"' /opt/kafka/config/server.properties
   grep -o 'password="[^"]*"' /etc/kafka/admin.properties
   ```
2. **Is the mechanism `SCRAM-SHA-512` everywhere?**
3. **Does the `admin` user exist in the metadata?**
   ```bash
   bin/kafka-dump-log.sh --cluster-metadata-decoder \
     --files /data/kafka/__cluster_metadata-0/00000000000000000000.log | grep -i scram
   ```
   `UserScramCredentialRecord ... "name":"admin"` should appear.

If the user does not exist (formatting was done without `--add-scram` or with `--ignore-formatted`), reformatting is the cleanest solution for a new installation:

```bash
# On all three nodes
systemctl stop kafka
rm -rf /data/kafka/*
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format -t <CLUSTER_ID> \
  -c /opt/kafka/config/server.properties \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=ADMIN_PASS]'
systemctl start kafka
```

If the password contains special characters such as `"`, `\` or `$`, it should first be tested with a simple password.

### 10.6 Hostname resolves to `127.0.1.1`

If `kafka-1/127.0.1.1:9092` appears in the logs, the `127.0.1.1` line in `/etc/hosts` should be removed (see [2.3](#23-etchosts)).

### 10.7 Wrong voter IP

If a voter appears with a wrong IP in the `describe --status` output (e.g. `10.0.0.22` instead of `10.1.1.22`), the quorum may stay up with 2/3 and hide the error.

1. The `controller.quorum.voters` line is corrected on all three nodes.
2. Nodes are restarted **one at a time**, and the quorum status is checked each time.
3. It is verified that the SAN of the affected node's certificate contains the correct IP.

An increase in `LeaderEpoch` after a rolling restart is normal.

---

## 11. Additional Information

### 11.1 Authentication options in Kafka

**Security protocols:**

| Protocol | Encryption | Authentication |
|---|---|---|
| `PLAINTEXT` | None | None |
| `SSL` | TLS | None or mTLS (`ssl.client.auth=required`) |
| `SASL_PLAINTEXT` | None | SASL |
| `SASL_SSL` | TLS | SASL |

**SASL mechanisms:**

| Mechanism | Description | When |
|---|---|---|
| `GSSAPI` | Kerberos | If AD / Kerberos infrastructure exists |
| `PLAIN` | Username + password sent in clear | Only with TLS and external validation |
| `SCRAM-SHA-256` / `SCRAM-SHA-512` | Challenge-response, users stored in Kafka metadata | When no separate identity infrastructure exists (this setup) |
| `OAUTHBEARER` | OAuth 2.0 / OIDC token | If an IdP such as Keycloak, Okta or Azure AD exists |

In addition, **delegation tokens** (short-lived tokens over SCRAM) are used in scenarios where many workers connect.

### 11.2 Additional listener (e.g. 9094)

Port 9094 is not used in this setup. If a separate replication network, different internal/external addresses or separate firewall rules are required, inter-broker traffic can be moved to a separate listener:

```properties
listeners=SASL_SSL://10.1.1.20:9092,INTERNAL://10.1.1.20:9094,CONTROLLER://10.1.1.20:9093
advertised.listeners=SASL_SSL://kafka-1:9092,INTERNAL://10.1.1.20:9094
listener.security.protocol.map=SASL_SSL:SASL_SSL,INTERNAL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=INTERNAL
listener.name.internal.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="ADMIN_PASS";
```

### 11.3 Dynamic quorum

If adding/removing controllers will be needed later, `controller.quorum.bootstrap.servers` can be used instead of `controller.quorum.voters`; in that case formatting is done with the `--initial-controllers` parameter. For a fixed 3-node setup, the static configuration is simpler.

### 11.4 Operational recommendations

- **Separating the inter-broker user:** For stricter separation, a dedicated `broker` user can be created during formatting and `admin` added later with `kafka-configs`.
- **Certificate renewal:** A new keystore can be placed at the same path and reloaded without a restart:
  ```bash
  bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
    --alter --entity-type brokers --entity-name 1 \
    --add-config listener.name.sasl_ssl.ssl.keystore.location=/etc/kafka/ssl/kafka-1.keystore.p12
  ```
- **Passwords:** Instead of plaintext passwords, `config.providers=file` can be used to read them from a separate, restricted file.
- **Backup:** `ca.key`, keystore passwords and the cluster ID should be stored in a secure location.
