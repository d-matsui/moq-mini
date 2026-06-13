# MoQT Session Establishment と認証 (Raw QUIC)

draft-ietf-moq-transport-14 ベース


## シーケンス

### パターン A: 独自 JWT (Relay が VTS で検証)

```
Client                          Relay                          VTS
  |                               |                              |
  | --- QUIC Initial ------------->                              |
  |      (ALPN: "moq-00")        |                              |
  |                               |                              |
  | <-- QUIC Handshake -----------|                              |
  |                               |                              |
  | ===== QUIC Connection 確立 ===|                              |
  |                               |                              |
  | --- CLIENT_SETUP ------------>|                              |
  |      AUTHORIZATION TOKEN      |                              |
  |      = <独自 JWT>             |                              |
  |                               | --- JWT 検証 --------------->|
  |                               |                              |
  |                               | <-- OK/NG ------------------|
  |                               |                              |
  | <-- SERVER_SETUP -------------|                              |
  |                               |                              |
  | ===== MoQT Session 確立 =====|                              |
  |                               |                              |
  | --- SUBSCRIBE_NAMESPACE ----->|                              |
  |      namespace = ("live",     |                              |
  |       "cam1")                 |                              |
  |      AUTHORIZATION TOKEN      | Relay が JWT のスコープを見て |
  |      = <JWT or alias>         | 認可判断                     |
  |                               |                              |
  | <-- SUBSCRIBE_NAMESPACE_OK ---|                              |
  |                               |                              |
  | <-- Object data --------------|                              |
  |      ...                      |                              |
```

### パターン B: SAT (RAPI 経由)

```
Client                          RAPI                         Relay
  |                               |                              |
  | --- SAT (WS) ---------------->|                              |
  |                               |                              |
  |                               | 認可 (Room/Member/methods)   |
  |                               |                              |
  |                               | --- (認可結果を伝える?) ---->|
  |                               |      ※ 要設計               |
  | <-- OK ----------------------|                              |
  |                               |                              |
  | --- QUIC Initial ------------------------------------->      |
  |      (ALPN: "moq-00")        |                              |
  |                               |                              |
  | <-- QUIC Handshake ------------------------------------      |
  |                               |                              |
  | ===== QUIC Connection 確立 ============================      |
  |                               |                              |
  | --- CLIENT_SETUP ---------------------------------------->   |
  |      AUTHORIZATION TOKEN      |                              |
  |      = <???>                  |  ← Relay は何で認可する?    |
  |                               |                              |
  | <-- SERVER_SETUP -----------------------------------------   |
  |                               |                              |
  | ===== MoQT Session 確立 ================================     |
  |                               |                              |
```

パターン B は未解決の問題がある:
- RAPI → Relay に認可結果をどう伝えるか
- Client が RAPI をバイパスして直接 Relay に繋いだ場合どうするか
- CLIENT_SETUP の AUTHORIZATION TOKEN に何を載せるか (RAPI が発行する短命 token? → 結局パターン A と同じ構造になる)


## 補足

### QUIC Initial / Handshake

- Client が QUIC Initial パケットを送り、Handshake 応答が返って QUIC Connection が確立する
- ALPN で「このコネクションは MoQT だ」と合意する。draft-14 以前は `"moq-00"` 固定
- 内部的には TLS 1.3。以降の通信はすべて暗号化される

### CLIENT_SETUP / SERVER_SETUP

- QUIC Connection の上で、Client が CLIENT_SETUP、Relay が SERVER_SETUP を送り合って MoQT Session が確立する
- CLIENT_SETUP には Setup Parameters として AUTHORIZATION TOKEN (JWT) を載せられる
- Relay は JWT の署名と有効期限を検証し、NG なら `UNAUTHORIZED (0x2)` でセッションを切る

### AUTHORIZATION TOKEN と alias

- CLIENT_SETUP で JWT を送るとき、同時に alias (番号) を REGISTER できる
- 以降の SUBSCRIBE 等では JWT 本体を送らず、alias だけで参照できる (USE_ALIAS)
- Relay が alias 用にキャッシュできるサイズは SERVER_SETUP の MAX_AUTH_TOKEN_CACHE_SIZE で決まる。デフォルト 0 = alias 禁止
- alias は最適化。使わず毎回 JWT を送っても動く

### 認可

- SUBSCRIBE, SUBSCRIBE_NAMESPACE, PUBLISH, PUBLISH_NAMESPACE 等のコントロールメッセージにも AUTHORIZATION TOKEN を載せられる (section 9.2.1.1)
- Relay は JWT の中のスコープを見て、要求された namespace へのアクセスを許可/拒否する


## JWT 設計

### 前提

- appId は SkyWay と共通 (secret の払い出しを別サービスでやるのは現実的でない)
- JWT の署名検証は VTS (appId の secret を持つ検証サーバー) が行う。Relay は VTS に問い合わせる

### 認可の流れ

1. バックエンド (secret を知っている) がユーザーの役割に応じた JWT を発行する
2. Client が CLIENT_SETUP で JWT を Relay に送る
3. Relay が VTS に JWT を送り、署名と有効期限を検証する → NG なら UNAUTHORIZED でセッション切断
4. Client が SUBSCRIBE_NAMESPACE 等を送ったとき、Relay が JWT のスコープを見て認可判断する

### パターン A: SAT に寄せない (独自 JWT)

appId は共通だが、スコープの構造は MoQT 独自で設計する。

**A-1: namespace 全許可**

```json
{
  "exp": 1712703600,
  "appId": "app-123",
  "namespaces": ["live/room1", "live/room2"]
}
```

- namespace に対する操作 (publish, subscribe, fetch, ...) はすべて許可される

**A-2: namespace + methods**

```json
{
  "exp": 1712703600,
  "appId": "app-123",
  "namespaces": [
    { "name": "live/room1", "methods": ["publish"] },
    { "name": "live/room2", "methods": ["subscribe"] }
  ]
}
```

- namespace ごとに publish/subscribe を分離できる

### パターン B: SAT に載せる

既存の SkyWay Auth Token の scope に MoQT の機能フラグを追加する。

```json
{
  "iat": 1712700000,
  "exp": 1712703600,
  "scope": {
    "appId": "app-123",
    "moq": { "enabled": true },
    "rooms": [
      {
        "name": "room1",
        "methods": ["create"],
        "member": {
          "name": "user-a",
          "methods": ["publish", "subscribe"]
        }
      }
    ]
  }
}
```

- `moq: { enabled: true }` は `turn` や `sfu` と同列の機能フラグ
- namespace の認可は既存の rooms / member.methods から導出する
  - 例: rooms に room1 があり member.methods に publish があれば、`{appId}/room1` への publish を許可
- publish/subscribe の分離は既存の member.methods をそのまま使える

#### 検討事項

- **Relay の認可をどうするか**: 既存の SkyWay は Client → RAPI (WS) → 認可 → SFU の流れ。MoQT でも同様に Client → RAPI → 認可 → Relay とするなら、RAPI-Relay 間の通信設計が必要。ただし Client は JS なので RAPI をバイパスして直接 Relay に繋ぐことも可能 → Relay 側でも何かしらの検証が必要
- **RAPI が token を発行する案**: RAPI が認可後に短命の MoQT 用 token を発行し Client に渡す → Client が CLIENT_SETUP でそれを Relay に送る → Relay が検証。ただしこれだとパターン A (独自 JWT) とほぼ同じ構造になる
- **Room / Member と namespace のマッピング**: MoQT の namespace を SkyWay の Room にどう対応付けるか
- **既存 WebRTC との共存**: 同じ SAT で WebRTC (SFU) と MoQT (Relay) の両方を認可できるメリットはある

### 比較

| | パターン A (独自 JWT) | パターン B (SAT に載せる) |
|---|---|---|
| appId / secret | SkyWay と共通 | SkyWay と共通 |
| JWT の署名検証 | Relay → VTS | RAPI (既存) |
| 認可判断 | Relay が JWT のスコープを見る | RAPI が Room/Member を見る |
| 認可モデル | namespace ベース | Room / Member ベース (既存 SAT の拡張) |
| 柔軟性 | MoQT の概念に合わせやすい | SkyWay の概念との整合性が必要 |
| publish/subscribe 分離 | A-2 で対応可能 | 既にある (member.methods) |
| 追加の開発 | Relay → VTS の通信 | RAPI の拡張 + Relay の認可問題 (バイパス対策) |
| PoC 向き | ◎ | △ (検討事項が多い) |


## References

- RFC 9000: QUIC: A UDP-Based Multiplexed and Secure Transport
- draft-ietf-moq-transport-14: Media over QUIC Transport
