# tcp-sync

Bir HTTP isteğini alıp **TCP üzerinden** bir sunucuya ileten, gelen cevabı **aynı HTTP isteğine yanıt olarak** dönen küçük bir Spring Boot uygulaması.

TCP doğası gereği asenkrondur: cevap, isteği gönderen thread'e değil, ayrı bir dinleme (reader) thread'ine gelir. Bu proje, o asenkron cevabı **`correlationId` + `CompletableFuture`** ile senkron bir HTTP yanıtına köprüler.

---

## Ne yapıyor?

```
İstemci ──HTTP POST──► CheckHealthController
                              │  frame'i byte olarak kurar: "HEALTH|<id>|<mesaj>"
                              ▼
                        TcpService.exchange(id, bytes)
                              │  id → CompletableFuture (pending map'e kaydeder)
                              ▼
                    TcpTransportEngine ──TCP──► TcpHealthServer (ayrı süreç)
                                                     │  "HEALTH_ACK|<id>|UP|..." yazar
                    reader thread ◄──TCP───────────────┘
                              │
                        onDataReceived(byte[]) → DTO'ya çevirir
                              │  pending.get(id).complete(dto)  ◄── iki thread burada buluşur
                              ▼
                        CompletableFuture dolar → HTTP 200 + HealthStatusDto
```

Ayrıca aynı DTO, isteğe bağlı bir **WebSocket** kanalından (`/ws/health`) da yayınlanır.

---

## Mimari — parçalar

| Katman | Sınıf | Görevi |
|---|---|---|
| Simüle dış SDK | `sdk/TcpClient` | İmplement edilmesi gereken arayüz (`connect`, `disconnect`, `sendMessage`, `onDataReceived`, `isConnected`) |
| | `sdk/TcpConnectionInfo` | Bağlantı bilgisi (host, port, timeout) taşıyan `record` |
| | `sdk/TcpTransportEngine` | Gerçek `java.net.Socket` açar; arka planda reader thread ile gelen byte'ları callback'e iletir |
| | `sdk/TcpTransportException` | Transport hataları |
| Servis | `tcp/TcpService` | SDK arayüzünü implement eder; `@PostConstruct`'ta bağlanır; `exchange()` ile async→sync köprüsünü kurar |
| HTTP | `controller/CheckHealthController` | Byte frame'i kurar, `exchange` çağırır, `CompletableFuture` döndürür (non-blocking) |
| WebSocket | `ws/HealthWebSocketHandler` | Bağlı client'lara DTO'yu JSON olarak yayınlar |
| | `ws/WebSocketConfig` | Handler'ı `/ws/health` yoluna bağlar |
| DTO | `dto/CheckHealthRequest` | Gelen HTTP gövdesi (`{"message": "..."}`) |
| | `dto/HealthStatusDto` | Dönen cevap (correlationId, status, latency, rawFrame...) |
| Test sunucusu | `mockserver/TcpHealthServer` | **Ayrı çalıştırılan** gerçek TCP echo sunucusu |

---

## İşin kalbi: asenkron → senkron köprü

Sorun: **iki farklı thread** vardır — HTTP isteğini işleyen thread ve TCP cevabını dinleyen reader thread.

Çözüm — `TcpService.exchange`:

```
1. new CompletableFuture()               → "sonra dolacak" boş kutu
2. pending.put(correlationId, future)    → GÖNDERMEDEN ÖNCE kaydet (hızlı cevabı kaçırmamak için)
3. sendMessage(payload)                  → byte'ı TCP'ye yaz
4. future.orTimeout(...)                 → cevap gelmezse süre aşımı
```

Cevap geldiğinde — `onDataReceived` (reader thread'inde):

```
1. byte[] → HealthStatusDto (parse)
2. pending.get(dto.correlationId()).complete(dto)   → bekleyen kutuyu doldur
```

`correlationId`, aynı anda giden birçok isteğin cevaplarının **karışmadan** doğru isteğe ulaşmasını sağlar. Sunucu, aldığı id'yi cevabında aynen geri yansıttığı için bu çalışır. Bu desenin adı: **request–response correlation**.

---

## Çalıştırma

Gereksinimler: **Java 21+**, Maven wrapper (`mvnw`) projeyle birlikte gelir.

### 1) TCP sunucusunu başlat (ayrı terminal)

IntelliJ'de `TcpHealthServer` sınıfına sağ tık → Run, veya terminalden:

```bash
./mvnw -q compile
java -cp target/classes com.tr.tcpsync.mockserver.TcpHealthServer
# özel port: java -cp target/classes com.tr.tcpsync.mockserver.TcpHealthServer 9099
```

### 2) Spring Boot uygulamasını başlat

```bash
./mvnw spring-boot:run
```

> Not: Uygulama, TCP sunucusu kapalıyken de açılır; ilk istekte yeniden bağlanmayı dener.

---

## Kullanım

### HTTP — senkron cevap

```bash
curl -X POST http://localhost:8080/api/check-health \
  -H "Content-Type: application/json" \
  -d '{"message":"selam"}'
```

Tarayıcıdan hızlı deneme için `GET` de var:

```bash
curl http://localhost:8080/api/check-health
```

Örnek yanıt (`HTTP 200`):

```json
{
  "correlationId": "af3be735-89d7-458b-bf6b-310dea240883",
  "status": "UP",
  "peerEpochMillis": 1786713435842,
  "latencyMs": 0,
  "rawFrame": "HEALTH_ACK|af3be735-...|UP|1786713435842|0",
  "receivedAt": 1786713435842
}
```

`rawFrame`, sunucunun tam olarak ne gönderdiğini gösterir — yani veri gerçekten TCP sunucusundan gelir.

### WebSocket — asenkron kanal (opsiyonel)

`ws://localhost:8080/ws/health` adresine bağlanan her client, her sağlık cevabında aynı `HealthStatusDto`'yu JSON olarak alır.

---

## HTTP durum kodları

| Durum | Kod | Ne zaman |
|---|---|---|
| Başarılı | `200 OK` | Sunucu cevabı zamanında geldi |
| Süre aşımı | `504 Gateway Timeout` | `tcp.request-timeout-ms` içinde cevap gelmedi |
| Sunucu erişilemez | `503 Service Unavailable` | TCP sunucusuna bağlanılamadı |

---

## Yapılandırma (`application.properties`)

```properties
tcp.host=127.0.0.1
tcp.port=9099
tcp.client-id=tcp-sync-client
tcp.connect-timeout-ms=5000
tcp.request-timeout-ms=5000   # HTTP isteği TCP cevabını kaç ms bekler
```

---

## Wire protokolü

Satır-sonu (`\n`) ile ayrılmış UTF-8 metin frame'leri:

```
istek : HEALTH|<correlationId>|<mesaj>
cevap : HEALTH_ACK|<correlationId>|UP|<serverEpochMillis>|<processingMs>
```

Tanınmayan her satır sunucu tarafından `ECHO|<satır>` olarak geri döner.

---

## Teknoloji

- Spring Boot 4.1.0 (Spring MVC, WebSocket)
- Java 21
- Jackson 3 (`tools.jackson.databind` — Boot 4 ile paket adı değişti)
- Lombok
- Bağımlılıksız `java.net.Socket` (transport ve test sunucusu)
