# 3 Node Bare Metal Kafka Cluster Kurulumu (KRaft + SASL_SSL)

Bu doküman, ZooKeeper'sız (KRaft modunda), TLS şifrelemeli, SCRAM-SHA-512 kimlik doğrulamalı ve ACL tabanlı yetkilendirmeli 3 node'luk bir Apache Kafka 4.x cluster'ının kurulumunu, testini ve kurulum sırasında karşılaşılan sorunların çözümünü anlatır.

---

## İçindekiler

1. [Mimari](#1-mimari)
2. [Ön Hazırlık](#2-ön-hazırlık)
3. [TLS Sertifikaları](#3-tls-sertifikaları)
4. [server.properties](#4-serverproperties)
5. [Storage Formatlama](#5-storage-formatlama)
6. [systemd Servisi](#6-systemd-servisi)
7. [Cluster Doğrulama](#7-cluster-doğrulama)
8. [Kullanıcı ve ACL Yönetimi](#8-kullanıcı-ve-acl-yönetimi)
9. [Auth Testleri](#9-auth-testleri)
10. [Sorun Giderme](#10-sorun-giderme)
11. [Ek Bilgiler](#11-ek-bilgiler)

---

## 1. Mimari

Her node hem **broker** hem **controller** rolündedir (combined mode). 3 controller'lı quorum, 1 node kaybını tolere eder.

| Node | node.id | IP | Hostname |
|---|---|---|---|
| 1 | 1 | 10.1.1.20 | kafka-1 |
| 2 | 2 | 10.1.1.21 | kafka-2 |
| 3 | 3 | 10.1.1.22 | kafka-3 |

| Listener | Port | Protokol | Kullanım |
|---|---|---|---|
| `SASL_SSL` | 9092 | TLS + SCRAM-SHA-512 | Client'lar ve broker'lar arası trafik |
| `CONTROLLER` | 9093 | mTLS (karşılıklı sertifika) | KRaft controller quorum |

> **Not:** Controller listener'da SCRAM kullanılamaz. SCRAM kullanıcıları metadata log'unda tutulur ve quorum ayağa kalkmadan bu bilgiye erişilemez. Bu nedenle controller trafiği mTLS ile korunur.

**Kimlik / yetki modeli:**

- Broker'lar arası ve admin erişimi: SCRAM kullanıcısı `admin`
- Controller'lar arası: sertifikadaki CN → `User:kafka-1`, `User:kafka-2`, `User:kafka-3`
- Yetkilendirme: `StandardAuthorizer`, ACL'i olmayan her istek reddedilir

---

## 2. Ön Hazırlık

Aşağıdaki adımlar **üç node'da da** yapılır.

### 2.1 Java ve Kafka

Kafka 4.x broker'ları Java 17 veya üzerini gerektirir.

```bash
apt install -y openjdk-17-jre-headless     # veya 21
tar -xzf kafka_2.13-<VERSION>.tgz -C /opt
ln -s /opt/kafka_2.13-<VERSION> /opt/kafka
```

### 2.2 Kullanıcı ve dizinler

```bash
useradd -r -s /usr/sbin/nologin kafka
mkdir -p /data/kafka /var/log/kafka /etc/kafka/ssl
chown -R kafka:kafka /data/kafka /var/log/kafka /etc/kafka /opt/kafka/config
```

### 2.3 /etc/hosts

Ubuntu/Debian, hostname'i varsayılan olarak `127.0.1.1` adresine yazar. Bu durumda node kendi hostname'ini loopback'e çözer ve bağlantı / sertifika doğrulama sorunları çıkar. Bu nedenle `127.0.1.1` satırı kaldırılır:

```text
# 127.0.1.1 kafka-1   ← silinmeli veya yorum yapılmalı
10.1.1.20 kafka-1
10.1.1.21 kafka-2
10.1.1.22 kafka-3
```

### 2.4 OS ayarları

```bash
cat >> /etc/sysctl.d/90-kafka.conf <<'EOF'
vm.swappiness=1
vm.max_map_count=262144
EOF
sysctl --system
```

- Veri diski için XFS veya ext4 kullanılır ve disk `noatime` ile mount edilir.
- Dosya tanımlayıcı limiti systemd unit'inde (`LimitNOFILE`) verilir.

### 2.5 Firewall

```bash
firewall-cmd --permanent --add-port=9092/tcp --add-port=9093/tcp
firewall-cmd --reload
```

9093 portunu yalnızca diğer Kafka node'larına açmak daha güvenlidir.

---

## 3. TLS Sertifikaları

Kurumsal bir PKI varsa o kullanılır. Yoksa iç bir CA oluşturulur.

### 3.1 CA (bir kez)

```bash
openssl req -x509 -newkey rsa:4096 -days 3650 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=Kafka-Internal-CA"
```

`ca.key` dosyası güvenli bir yerde saklanmalıdır.

### 3.2 Node sertifikaları

Her node için `N` ve `IP` değiştirilerek tekrarlanır:

```bash
N=kafka-1; IP=10.1.1.20

openssl req -newkey rsa:2048 -nodes -keyout $N.key -out $N.csr -subj "/CN=$N"

openssl x509 -req -in $N.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 -out $N.crt \
  -extfile <(printf "subjectAltName=DNS:$N,IP:$IP\nextendedKeyUsage=serverAuth,clientAuth")

openssl pkcs12 -export -in $N.crt -inkey $N.key -certfile ca.crt -name $N \
  -out $N.keystore.p12 -passout pass:'KEYSTORE_PASS'
```

Kritik noktalar:

- **SAN** hem hostname hem IP içermeli. Client'lar hostname ile, controller voter'ları IP ile bağlanır.
- Hostname, `advertised.listeners` ile **birebir aynı** olmalı (`kafka-1`, `kafka1` değil).
- **`clientAuth`** EKU'su mTLS için zorunludur.

SAN kontrolü:

```bash
openssl x509 -in kafka-1.crt -noout -ext subjectAltName
```

### 3.3 Truststore (bir kez)

```bash
keytool -importcert -alias kafka-ca -file ca.crt -keystore truststore.p12 \
  -storetype PKCS12 -storepass 'TRUST_PASS' -noprompt
```

### 3.4 Dağıtım

Her node'a kendi keystore'u ve ortak truststore kopyalanır:

```bash
cp kafka-1.keystore.p12 truststore.p12 /etc/kafka/ssl/
chown -R kafka:kafka /etc/kafka/ssl
chmod 600 /etc/kafka/ssl/*
```

---

## 4. server.properties

Dosya: `/opt/kafka/config/server.properties`

### 4.1 Node 1

```properties
############################ KRaft / Rol ############################
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@10.1.1.20:9093,2@10.1.1.21:9093,3@10.1.1.22:9093

############################ Listener'lar ############################
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

# Controller listener'da karşılıklı sertifika zorunlu
listener.name.controller.ssl.client.auth=required
# "CN=kafka-1" sertifikasını "User:kafka-1" principal'ına çevirir
ssl.principal.mapping.rules=RULE:^CN=([a-zA-Z0-9.-]*).*$/$1/,DEFAULT

############################ SASL / SCRAM ############################
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="ADMIN_PASS";

############################ Yetkilendirme ############################
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:kafka-1;User:kafka-2;User:kafka-3
allow.everyone.if.no.acl.found=false

############################ Replikasyon / Dayanıklılık ############################
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

############################ Performans ############################
num.network.threads=8
num.io.threads=16
num.replica.fetchers=4
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600
num.recovery.threads.per.data.dir=4
```

### 4.2 Node'lar arası farklar

Yalnızca aşağıdaki satırlar node'a özeldir; diğer her şey üç dosyada aynıdır.

| Ayar | Node 1 | Node 2 | Node 3 |
|---|---|---|---|
| `node.id` | `1` | `2` | `3` |
| `listeners` | `SASL_SSL://10.1.1.20:9092,CONTROLLER://10.1.1.20:9093` | `SASL_SSL://10.1.1.21:9092,CONTROLLER://10.1.1.21:9093` | `SASL_SSL://10.1.1.22:9092,CONTROLLER://10.1.1.22:9093` |
| `advertised.listeners` | `SASL_SSL://kafka-1:9092` | `SASL_SSL://kafka-2:9092` | `SASL_SSL://kafka-3:9092` |
| `ssl.keystore.location` | `.../kafka-1.keystore.p12` | `.../kafka-2.keystore.p12` | `.../kafka-3.keystore.p12` |

`controller.quorum.voters` **üç node'da birebir aynı** olmalıdır.

> **İpucu:** `listeners` için `0.0.0.0` kullanılabilir; node tüm arayüzlerden dinler ve satır her node'da aynı olur. `advertised.listeners` ise her zaman client'ların erişebildiği gerçek bir adres olmalıdır.

### 4.3 Dosya yetkileri ve biçimi

```bash
chown kafka:kafka /opt/kafka/config/server.properties
chmod 600 /opt/kafka/config/server.properties
```

Dosyada BOM veya Windows satır sonu (CRLF) **olmamalıdır** (bkz. [10.3](#103-missing-required-configuration-processroles)). JAAS ayarını tek satırda tutmak, `\` devam satırı hatalarını önler.

---

## 5. Storage Formatlama

KRaft'ta storage dizinleri başlatmadan önce formatlanmalıdır. Broker'lar arası SCRAM kullanıcısı (`admin`) cluster açılmadan önce, **format sırasında** oluşturulur.

### 5.1 Cluster ID üretilmesi (bir kez)

```bash
/opt/kafka/bin/kafka-storage.sh random-uuid
```

### 5.2 Üç node'un aynı ID ile formatlanması

```bash
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format \
  -t <CLUSTER_ID> \
  -c /opt/kafka/config/server.properties \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=ADMIN_PASS]'
```

Kurallar:

- Cluster ID ve parola **üç node'da aynı** olmalı.
- `--add-scram` içindeki parola, `server.properties` ve `admin.properties` dosyalarındaki parolayla **birebir aynı** olmalı.
- Format `kafka` kullanıcısıyla yapılmalı (yetki sorunlarını önler).
- `--ignore-formatted` ile atlanan dizinlere SCRAM kaydı **eklenmez**.

Kontrol:

```bash
cat /data/kafka/meta.properties    # cluster.id, node.id, version=1
```

---

## 6. systemd Servisi

Dosya: `/etc/systemd/system/kafka.service`

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

# Graceful (controlled) shutdown: leader'lar devredilene kadar beklenir
KillSignal=SIGTERM
TimeoutStopSec=180
SuccessExitStatus=143
Restart=on-failure
RestartSec=10

# Limitler
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

Notlar:

- Production'da Kafka **root ile çalıştırılmamalıdır**.
- `ExecStop` kasıtlı olarak yoktur: `kafka-server-stop.sh` makinedeki tüm Kafka process'lerini öldürür. systemd'nin gönderdiği SIGTERM zaten controlled shutdown başlatır.
- `ProtectSystem=strict` kullanıldığında, birden fazla veri diski varsa hepsi `ReadWritePaths` satırına eklenmelidir.
- Heap 6 GB yeterlidir; belleğin geri kalanı OS page cache'e bırakılmalıdır.

Etkinleştirme:

```bash
systemctl daemon-reload
systemctl enable --now kafka
journalctl -u kafka -f
```

Değişikliklerde node'lar **teker teker** restart edilir; her restart sonrası quorum durumu kontrol edilir.

---

## 7. Cluster Doğrulama

### 7.1 Admin client config

Dosya: `/etc/kafka/admin.properties`

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

Beklenen: `Verify return code: 0 (ok)` ve `TLSv1.3`.

### 7.3 Quorum durumu

```bash
bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/admin.properties describe --status
```

Beklenen çıktı:

```text
LeaderId:               1
MaxFollowerLag:         0
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://10.1.1.20:9093"]},
                         {"id": 2, "endpoints": ["CONTROLLER://10.1.1.21:9093"]},
                         {"id": 3, "endpoints": ["CONTROLLER://10.1.1.22:9093"]}]
```

Kontrol listesi:

- `LeaderId` dolu
- Üç voter da **doğru IP'lerle** listeleniyor
- `MaxFollowerLag` 0'a yakın

Voter bazında detay için:

```bash
bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/admin.properties describe --replication
```

### 7.4 Broker kayıtları

```bash
bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/admin.properties | grep -E '^kafka-'
```

Beklenen:

```text
kafka-1:9092 (id: 1 rack: null isFenced: false) -> (
kafka-2:9092 (id: 2 rack: null isFenced: false) -> (
kafka-3:9092 (id: 3 rack: null isFenced: false) -> (
```

`isFenced: false`, broker'ın metadata'yı takip ettiğini ve trafik almaya hazır olduğunu gösterir.

---

## 8. Kullanıcı ve ACL Yönetimi

### 8.1 Uygulama kullanıcısı oluşturma

```bash
bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --alter --entity-type users --entity-name app-orders \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=APP_PASS]'
```

### 8.2 ACL tanımlama

Producer + consumer yetkisi (örnek: `orders` topic'i, `orders-consumers` group'u):

```bash
bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --add --allow-principal User:app-orders --producer --consumer \
  --topic orders --group orders-consumers
```

### 8.3 ACL listeleme

```bash
bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties --list
```

### 8.4 Uygulama client config'i

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

`acks=all` ile `min.insync.replicas=2` birlikte, 1 node kaybında veri kaybı olmadan yazmanın devam etmesini sağlar.

---

## 9. Auth Testleri

### 9.1 Hazırlık

```bash
# Test topic'i
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --create --topic auth-test --partitions 3 --replication-factor 3

bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --describe --topic auth-test

# Test kullanıcısı ve ACL
bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --alter --entity-type users --entity-name app-test \
  --add-config 'SCRAM-SHA-512=[password=AppTest123]'

bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --add --allow-principal User:app-test --producer --consumer \
  --topic auth-test --group test-group

# Test kullanıcısının client config'i
sed -e 's/username="admin"/username="app-test"/' \
    -e 's/password="ADMIN_PASS"/password="AppTest123"/' \
    /etc/kafka/admin.properties > /tmp/app.properties
```

`describe` çıktısında her partition için `Replicas` ve `Isr` alanlarında 3 broker görünmelidir.

### 9.2 Pozitif test: produce / consume

```bash
# Terminal 1
bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 \
  --producer.config /tmp/app.properties --topic auth-test

# Terminal 2 (farklı broker üzerinden)
bin/kafka-console-consumer.sh --bootstrap-server kafka-2:9092 \
  --consumer.config /tmp/app.properties --topic auth-test \
  --group test-group --from-beginning
```

Gönderilen mesaj consumer'da görünmelidir.

### 9.3 Negatif testler

Aşağıdakilerin **hepsi reddedilmelidir**.

| Test | Nasıl | Beklenen sonuç |
|---|---|---|
| Yanlış parola | `/tmp/app.properties` içindeki parola değiştirilip producer çalıştırılır | `SaslAuthenticationException: Authentication failed` |
| Kimliksiz bağlantı | `bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list` (config olmadan) | Bağlantı kurulamaz (timeout / disconnect) |
| ACL'siz topic | Aşağıdaki komutlar | `TopicAuthorizationFailedException` |
| ACL'siz group | Consumer `--group baska-group` ile çalıştırılır | `GroupAuthorizationFailedException` |
| Sertifikasız controller erişimi | Aşağıdaki komut | TLS handshake hatası |

ACL'siz topic:

```bash
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --create --topic yasak-topic --partitions 1 --replication-factor 3

bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 \
  --producer.config /tmp/app.properties --topic yasak-topic
```

Controller portu (sertifikasız):

```bash
openssl s_client -connect 10.1.1.20:9093 -CAfile ca.crt </dev/null
```

Karşılaştırma için `-cert kafka-1.crt -key kafka-1.key` eklendiğinde bağlantı kurulmalıdır.

### 9.4 Temizlik

```bash
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties --delete --topic auth-test
bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties --delete --topic yasak-topic

bin/kafka-acls.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --remove --allow-principal User:app-test --producer --consumer \
  --topic auth-test --group test-group --force

bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
  --alter --entity-type users --entity-name app-test --delete-config 'SCRAM-SHA-512'

rm /tmp/app.properties
```

---

## 10. Sorun Giderme

Kurulum sırasında karşılaşılan hatalar ve çözümleri.

### 10.1 `No readable meta.properties files found`

Kafka, `log.dirs` altında `meta.properties` dosyasını bulamıyor veya okuyamıyor.

| Sebep | Çözüm |
|---|---|
| Storage formatlanmamış | Bölüm 5'teki format komutu çalıştırılır |
| Format farklı bir config ile yapılmış | Format ve başlatmada aynı dosya kullanılır; `log.dirs` path'i kontrol edilir |
| Format root ile yapılmış, Kafka `kafka` kullanıcısıyla çalışıyor | `chown -R kafka:kafka /data/kafka` |
| `log.dirs` `/tmp` altında, reboot'ta silinmiş | Kalıcı bir dizin kullanılır |

### 10.2 `the loader is still catching up because we still don't know the high water mark yet`

Hata değildir; node controller quorum'unun oluşmasını bekliyordur. 3 controller'dan en az 2'si ayakta olmalıdır. Diğer node'lar açık olmasına rağmen devam ediyorsa:

```bash
nc -vz 10.1.1.21 9093                          # port erişimi / firewall
ss -ltnp | grep 9093                           # doğru IP'de dinliyor mu
grep cluster.id /data/kafka/meta.properties    # üç node'da aynı mı
```

`controller.quorum.voters` satırının üç node'da aynı olduğu da kontrol edilmelidir.

### 10.3 `Missing required configuration "process.roles"`

Satır dosyada görünse de Kafka okuyamıyor. Olası sebepler:

1. **systemd farklı bir dosyayı okuyor:**
   ```bash
   systemctl cat kafka | grep ExecStart
   ```
2. **Önceki satır `\` ile bitiyor** ve bu satırı yutuyor:
   ```bash
   grep -n '\\$' /opt/kafka/config/server.properties
   ```
   Yalnızca (çok satırlı yazıldıysa) JAAS satırı çıkmalıdır.
3. **BOM / CRLF karakterleri** (dosya Windows'ta düzenlendiyse):
   ```bash
   head -1 /opt/kafka/config/server.properties | cat -A
   ```
   Satır `M-oM-;M-?` ile başlıyorsa veya sonlarda `^M` varsa:
   ```bash
   sed -i '1s/^\xEF\xBB\xBF//' /opt/kafka/config/server.properties
   sed -i 's/\r$//' /opt/kafka/config/server.properties
   ```

Düzeltme sonrası önce elle test etmek faydalıdır:

```bash
sudo -u kafka /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
```

### 10.4 Kafka `/tmp/kafka-logs` dizinine yazmaya çalışıyor

`/tmp/kafka-logs`, `log.dirs` için **varsayılan değerdir**. Kafka'nın `log.dirs` satırını okumadığını gösterir.

```bash
grep -nE 'process.roles|log.dirs|node.id' /opt/kafka/config/server.properties
```

Satır yoksa eklenir (`log.dirs=/data/kafka`), varsa 10.3'teki biçim sorunları kontrol edilir. Ardından `/tmp/kafka-logs` silinir ve `/data/kafka` formatlanmış olmalıdır.

### 10.5 `Authentication failed ... invalid credentials with SASL mechanism SCRAM-SHA-512`

TLS çalışıyor, SCRAM kimlik bilgileri tutmuyor.

1. **Parolalar eşleşiyor mu?**
   ```bash
   grep -o 'password="[^"]*"' /opt/kafka/config/server.properties
   grep -o 'password="[^"]*"' /etc/kafka/admin.properties
   ```
2. **Mekanizma her yerde `SCRAM-SHA-512` mi?**
3. **`admin` kullanıcısı metadata'da var mı?**
   ```bash
   bin/kafka-dump-log.sh --cluster-metadata-decoder \
     --files /data/kafka/__cluster_metadata-0/00000000000000000000.log | grep -i scram
   ```
   `UserScramCredentialRecord ... "name":"admin"` görünmelidir.

Kullanıcı yoksa (format `--add-scram` olmadan veya `--ignore-formatted` ile yapılmışsa), yeni kurulumda en temiz çözüm yeniden formatlamaktır:

```bash
# Üç node'da da
systemctl stop kafka
rm -rf /data/kafka/*
sudo -u kafka /opt/kafka/bin/kafka-storage.sh format -t <CLUSTER_ID> \
  -c /opt/kafka/config/server.properties \
  --add-scram 'SCRAM-SHA-512=[name=admin,password=ADMIN_PASS]'
systemctl start kafka
```

Parolada `"`, `\`, `$` gibi özel karakterler varsa önce sade bir parolayla test edilmelidir.

### 10.6 Hostname `127.0.1.1` adresine çözülüyor

Loglarda `kafka-1/127.0.1.1:9092` görülüyorsa `/etc/hosts` dosyasındaki `127.0.1.1` satırı kaldırılmalıdır (bkz. [2.3](#23-etchosts)).

### 10.7 Voter IP'si yanlış

`describe --status` çıktısında bir voter yanlış IP ile görünüyorsa (örn. `10.0.0.22` yerine `10.1.1.22`), quorum 2/3 ile ayakta kalıp hatayı gizleyebilir.

1. Üç node'da `controller.quorum.voters` satırı düzeltilir.
2. Node'lar **teker teker** restart edilir, her seferinde quorum durumu kontrol edilir.
3. İlgili node sertifikasının SAN alanında doğru IP olduğu doğrulanır.

Rolling restart sonrası `LeaderEpoch` değerinin artması normaldir.

---

## 11. Ek Bilgiler

### 11.1 Kafka'da kimlik doğrulama seçenekleri

**Security protocol'ler:**

| Protokol | Şifreleme | Kimlik doğrulama |
|---|---|---|
| `PLAINTEXT` | Yok | Yok |
| `SSL` | TLS | Yok veya mTLS (`ssl.client.auth=required`) |
| `SASL_PLAINTEXT` | Yok | SASL |
| `SASL_SSL` | TLS | SASL |

**SASL mekanizmaları:**

| Mekanizma | Açıklama | Ne zaman |
|---|---|---|
| `GSSAPI` | Kerberos | AD / Kerberos altyapısı varsa |
| `PLAIN` | Kullanıcı adı + parola düz gönderilir | Yalnızca TLS ile ve harici doğrulamayla |
| `SCRAM-SHA-256` / `SCRAM-SHA-512` | Challenge-response, kullanıcılar Kafka metadata'sında | Ayrı kimlik altyapısı yoksa (bu kurulum) |
| `OAUTHBEARER` | OAuth 2.0 / OIDC token | Keycloak, Okta, Azure AD gibi bir IdP varsa |

Ek olarak **delegation token** (SCRAM üzerinden kısa ömürlü token) çok sayıda worker'ın bağlandığı senaryolarda kullanılır.

### 11.2 Ek listener (örn. 9094)

Bu kurulumda 9094 kullanılmaz. Ayrı bir replikasyon ağı, farklı iç/dış adresler veya ayrı firewall kuralları gerekiyorsa broker'lar arası trafik ayrı bir listener'a alınabilir:

```properties
listeners=SASL_SSL://10.1.1.20:9092,INTERNAL://10.1.1.20:9094,CONTROLLER://10.1.1.20:9093
advertised.listeners=SASL_SSL://kafka-1:9092,INTERNAL://10.1.1.20:9094
listener.security.protocol.map=SASL_SSL:SASL_SSL,INTERNAL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=INTERNAL
listener.name.internal.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="ADMIN_PASS";
```

### 11.3 Dinamik quorum

İleride controller ekleme/çıkarma gerekecekse `controller.quorum.voters` yerine `controller.quorum.bootstrap.servers` kullanılabilir; bu durumda format `--initial-controllers` parametresiyle yapılır. Sabit 3 node için statik yapı daha basittir.

### 11.4 Operasyonel öneriler

- **Inter-broker kullanıcısını ayırma:** Daha sıkı ayrım için format sırasında ayrı bir `broker` kullanıcısı oluşturulup `admin` sonradan `kafka-configs` ile eklenebilir.
- **Sertifika yenileme:** Yeni keystore aynı path'e konup restart olmadan yeniden yüklenebilir:
  ```bash
  bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --command-config /etc/kafka/admin.properties \
    --alter --entity-type brokers --entity-name 1 \
    --add-config listener.name.sasl_ssl.ssl.keystore.location=/etc/kafka/ssl/kafka-1.keystore.p12
  ```
- **Parolalar:** Düz metin parolalar yerine `config.providers=file` ile ayrı, kısıtlı yetkili bir dosyadan okutulabilir.
- **Yedekleme:** `ca.key`, keystore parolaları ve cluster ID güvenli bir yerde saklanmalıdır.
