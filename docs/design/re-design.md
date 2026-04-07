# API 設計

## 概念と使い方

### Session

MOQT 接続が確立し SETUP 交換が完了すると Session が生まれる。
Session は Publisher と Subscriber を生成し、`run()` でバックグラウンドの stream 受信・振り分けを駆動する。

```rust
// Client 側: 接続する
let (session, publisher, subscriber) = Session::connect(addr).await?;
tokio::spawn(session.run());

// Relay 側: 接続を受け入れる
let (session, publisher, subscriber) = Session::accept(incoming).await?;
tokio::spawn(session.run());
```

### Publisher / Subscriber

Publisher はデータを配信する側、Subscriber はデータを受信する側。
1つの Session で両方を同時に使える（Relay がこのパターン）。

Publisher は `publish_namespace()` で配信可能な namespace を登録し、`next_request()` で Subscriber からのリクエストを待つ。
Subscriber は `subscribe()` や `fetch()` でリクエストを送り、配信を要求する。

> **なぜ Session に統合せず分けるのか？**
> Session に next_request() も subscribe() も全部持たせる設計もあり得る。
> ただし next_request() はループで回し続けるため、そのタスクが Session を占有してしまい、別のタスクから subscribe() を呼べなくなる。
> Session を Clone 可能（内部 Arc）にすれば並行の問題は解決できる。
> しかし Session に全メソッドが集まると型が大きくなる。Publisher と Subscriber に分ければ、関心の異なるメソッドが分離され、各型が小さく保てる。

```rust
// Publisher: namespace を登録してリクエストを待つ
publisher.publish_namespace(ns).await?;
loop {
    match publisher.next_request().await? {
        Request::Subscribe(req) => { /* ... */ }
        Request::Fetch(req) => { /* ... */ }
    }
}

// Subscriber: SUBSCRIBE を送る
let subscribe_receiver = subscriber.subscribe(ns, "video").await?;

// Subscriber: FETCH を送る（Standalone / Joining）
let fetch_receiver = subscriber.fetch(ns, "video", range).await?;
let fetch_receiver = subscriber.fetch_joining(request_id, range).await?;
```

### Request

bidi stream 上でやり取りされるリクエスト。
Publisher と Subscriber はそれぞれ異なる種類のリクエストを受け取る:

- **Publisher が受け取る**: SUBSCRIBE, FETCH, TrackStatus（`publisher.next_request()`）
- **Subscriber が受け取る**: PUBLISH_NAMESPACE（`subscriber.next_request()`）

受け取った側は `ok()` で承認するか `error()` で拒否する。
承認すると、データの送受信に使う型が返る:

- `SubscribeRequest.ok()` → SubscribeSender（Object を書いて配信する）
- `FetchRequest.ok()` → FetchSender（Object を書いて返す）

```rust
match publisher.next_request().await? {
    Request::Subscribe(req) => {
        // req.namespace, req.track_name, req.parameters で判断
        let subscribe_sender = req.ok().await?;   // SUBSCRIBE_OK → SubscribeSender
    }
    Request::Fetch(req) => {
        let fetch_sender = req.ok().await?;   // FETCH_OK → FetchSender
    }
}
// or
req.error().await?;  // REQUEST_ERROR
```

### PublishNamespaceRequest

Subscriber 側が受け取るリクエスト。Publisher が `publish_namespace()` で namespace を登録したとき、
相手側（Relay や Subscriber）の `subscriber.next_request()` で受け取る。

`ok()` で承認するか `error()` で拒否する。
承認後は bidi stream が開いたままになり、Publisher が取り下げるまで namespace が有効。

```rust
match subscriber.next_request().await? {
    SubscriberRequest::PublishNamespace(req) => {
        // req.namespace で判断
        req.ok().await?;
    }
}
```

### SubscribeSender / SubscribeReceiver

SUBSCRIBE が成立したときに生まれるハンドル。SUBSCRIBE の bidi stream を内部に持つ。

SUBSCRIBE では複数の uni stream が並行に来うるため、受信側で stream を1つずつ受け取る `accept()` が必要。
送信側も対称性のために `open()` を持つ。
`open()` は uni stream を開き、`accept()` は相手が開いた uni stream を受け取る。
返ってくる SubgroupWriter / SubgroupReader で Object を読み書きする。

全データ送信後は `done()` で PUBLISH_DONE を送って配信を終了する。
受信側は PUBLISH_DONE が来ると `accept()` が None を返す。

現在は 1 Group = 1 Subgroup = 1 uni stream を前提としており、
`open()` は内部で group_id を自動インクリメントし、subgroup_id は 0 固定。

```rust
// 送信側（Publisher）
let subscribe_sender = req.ok().await?;

let subgroup = subscribe_sender.open().await?;
subgroup.write_object(&Object {
    id: 0, status: Normal, properties: Default::default(), payload,
}).await?;
subgroup.finish()?;  // uni stream を閉じる

let subgroup = subscribe_sender.open().await?;  // 次の Group
subgroup.write_object(&Object {
    id: 0, status: Normal, properties: Default::default(), payload2,
}).await?;
subgroup.finish()?;

subscribe_sender.done().await?;  // PUBLISH_DONE を送って配信終了

// 受信側（Subscriber）
while let Some(subgroup) = subscribe_receiver.accept().await {
    tokio::spawn(async move {
        while let Some(object) = subgroup.read_object().await {
            process(object);
        }
    });
}
```

> **なぜ FetchSender/FetchReceiver には open/accept がないのか？**
> FETCH は1本の uni stream しか使わないため、accept で待つ必要がない。直接 Object を読み書きする。

> **将来 1 Group に複数 Subgroup が必要になった場合**
> `open(group_id, subgroup_id)` で group_id / subgroup_id を明示的に指定できるようにする。

### SubgroupWriter / SubgroupReader

`open()` / `accept()` で得られる、1本の uni stream に対応するハンドル。
Object を読み書きし、`finish()` で uni stream を閉じる。

`group_id()` / `subgroup_id()` で、どの Group / Subgroup に属するかを参照できる。

```rust
// 書き込み
let subgroup = subscribe_sender.open().await?;
subgroup.write_object(&Object {
    id: 0, status: Normal, properties: Default::default(), payload,
}).await?;
subgroup.finish()?;

// 読み出し
let subgroup = subscribe_receiver.accept().await?;
while let Some(object) = subgroup.read_object().await {
    // subgroup.group_id(), subgroup.subgroup_id()
    process(object);
}
```

### Object

データの最小単位。各フィールド:

- **id** — Object ID。ユーザーが指定する（自動採番しない。Relay が元の ID を保持して転送するため）
- **status** — Normal / EndOfGroup / EndOfTrack。Normal 以外は payload が空
- **properties** — Key-Value メタデータ。Relay はそのまま転送する。アプリケーション固有の用途に使える
- **payload** — バイト列。MOQT はメディアに依存しないので、中身はアプリケーションが決める

### FetchSender / FetchReceiver

1つの FETCH に対応するハンドル。SUBSCRIBE と異なり、1本の uni stream で全 Object を送受する。

```rust
// 送信側（Publisher）
let fetch_sender = req.ok().await?;
fetch_sender.write_object(&object).await?;
fetch_sender.finish()?;

// 受信側（Subscriber）
let fetch_receiver = subscriber.fetch(ns, "video", range).await?;
while let Some(object) = fetch_receiver.read_object().await {
    process(object);
}
```

### Relay

Relay は Client と同じ Session / Publisher / Subscriber を使う。
1つの Session で Publisher と Subscriber を両方使い、upstream と downstream の間でデータを中継する。

**RelayState** は SUBSCRIBE の中継先を判断するための状態を持つ。
Publisher が PUBLISH_NAMESPACE で「この namespace のデータは自分が持っている」と登録し、
Relay はそれを `publisher_by_namespace` に記録する。
SUBSCRIBE が来たら namespace を引いて対応する Publisher session を見つけ、upstream に中継する。

```rust
struct RelayState {
    publisher_by_namespace: HashMap<Namespace, SessionId>,
}
```

```rust
let endpoint = quinn::Endpoint::server(config, addr)?;

while let Some(incoming) = endpoint.accept().await {
    let relay_state = relay_state.clone();
    tokio::spawn(async move {
        let (session, publisher, subscriber) = Session::accept(incoming).await?;
        tokio::spawn(session.run());

        // 相手が Subscriber として振る舞う場合
        tokio::spawn(async move {
            loop {
                match publisher.next_request().await? {
                    Request::Subscribe(req) => {
                        tokio::spawn(handle_subscribe(req, relay_state.clone()));
                    }
                    _ => { /* TODO */ }
                }
            }
        });

        // 相手が Publisher として振る舞う場合
        tokio::spawn(async move {
            loop {
                match subscriber.next_request().await? {
                    SubscriberRequest::PublishNamespace(req) => {
                        relay_state.publisher_by_namespace
                            .insert(req.namespace, session_id);
                        req.ok().await?;
                    }
                }
            }
        });
    });
}

async fn handle_subscribe(req: SubscribeRequest, relay_state: RelayState) {
    let upstream_subscriber = relay_state.find_publisher(&req.namespace);
    let upstream = upstream_subscriber.subscribe(req.namespace, req.track_name).await?;
    let downstream = req.ok().await?;

    while let Some(upstream_subgroup) = upstream.accept().await {
        let downstream_subgroup = downstream.open().await?;
        tokio::spawn(async move {
            while let Some(object) = upstream_subgroup.read_object().await {
                downstream_subgroup.write_object(&object).await?;
            }
            downstream_subgroup.finish()?;
        });
    }
    downstream.done().await?;
}
```

## 型と API（リファレンス）

```
Session
├── Publisher                  データを配信する側。SUBSCRIBE を受けて配信を開始する
│   ├── .publish_namespace(ns)    配信可能な namespace を Relay に登録する
│   └── .next_request() → Request
│       ├── Subscribe(SubscribeRequest)
│       │   ├── .ok()    → SubscribeSender    承認して Object の送信を開始する
│       │   └── .error()                  拒否する
│       ├── Fetch(FetchRequest)
│       │   ├── .ok()    → FetchSender    承認して Object の送信ハンドルを得る
│       │   └── .error()                  拒否する
│       └── TrackStatus(TrackStatusRequest)
│           └── .error()                  （最小実装では未対応）
│
└── Subscriber                 データを受信する側。SUBSCRIBE / FETCH を送って受信を開始する
    ├── .subscribe(ns, track) → SubscribeReceiver
    ├── .fetch(ns, track, range) → FetchReceiver           Standalone Fetch
    └── .fetch_joining(request_id, range) → FetchReceiver   Joining Fetch（既存の SUBSCRIBE に紐づく）
```

```
SubscribeSender                        1 つの SUBSCRIBE に対応。Subgroup を送る側
├── .open()       → SubgroupWriter     uni stream を開く（group_id 自動、subgroup_id = 0）
└── .done()                            bidi stream に PUBLISH_DONE を送り、配信を終了する

SubscribeReceiver                      1 つの SUBSCRIBE に対応。Subgroup を受け取る側
└── .accept()     → SubgroupReader?    次の uni stream を待つ（PUBLISH_DONE で None）

SubgroupWriter                         1 Subgroup = 1 uni stream の書き込み側
├── .write_object(&Object)             Object を書く
└── .finish()                          uni stream を閉じる（FIN）

SubgroupReader                         1 Subgroup = 1 uni stream の読み出し側
├── .read_object() → Object?           次の Object を読む
├── .group_id()                        メタデータ参照
└── .subgroup_id()

FetchSender                        1 つの FETCH に対応。1 本の uni stream で Object を送る
├── .write_object(&Object)         Object を書く
└── .finish()                      uni stream を閉じる（FIN）

FetchReceiver                      1 つの FETCH に対応。1 本の uni stream から Object を読む
└── .read_object() → Object?       次の Object を読む（FIN で None）

Object                             1 つの Object のデータ
├── id: u64                        Object ID
├── status: ObjectStatus           Normal / EndOfGroup / EndOfTrack
├── properties: Properties         Key-Value メタデータ（Relay はそのまま転送）
└── payload: Bytes                 ペイロード（status が Normal のときのみ）
```

## 内部構造

### stream の種類

MOQT では 3 種類の stream を使う:

- **control stream（uni × 2）**: 各 peer が 1 本ずつ uni stream を開き、SETUP を交換する。セッション中ずっと開いたまま
- **request stream（bidi）**: SUBSCRIBE, FETCH, PUBLISH_NAMESPACE 等のリクエストごとに 1 本開く
- **data stream（uni）**: Subgroup の Object データ、FETCH のレスポンスデータ

### session.run()

バックグラウンドで bidi / uni stream を accept し、内部の channel を通じて Publisher / Subscriber / SubscribeReceiver に振り分ける。

```rust
async fn run(self) {
    loop {
        tokio::select! {
            stream = self.conn.accept_bi() => {
                // bidi stream の最初のメッセージを読んで種類を判別
                let msg = read_message(&stream).await;
                match msg {
                    // SUBSCRIBE, FETCH 等 → Publisher の channel に送る
                    Subscribe(req) => self.publisher_tx.send(Request::Subscribe(req)),
                    // PUBLISH_NAMESPACE 等 → Subscriber の channel に送る
                    PublishNamespace(req) => self.subscriber_tx.send(...),
                }
            }
            stream = self.conn.accept_uni() => {
                // uni stream のヘッダから track_alias を読む
                let header = read_subgroup_header(&stream).await;
                // 対応する SubscribeReceiver の channel に送る
                let rx = self.subscribe_receivers.get(header.track_alias);
                rx.send(stream);
            }
        }
    }
}
```

- `publisher.next_request()` は内部で `publisher_rx.recv()` を呼ぶだけ
- `subscribe_receiver.accept()` は内部で channel から uni stream を受け取るだけ

## 検討中（案はあるが未決定）

- Relay のデータパススルー API
- Subscriber 側の next_request() は必要か（PUBLISH_NAMESPACE を受け取る側）
- publisher.next_request() / subscriber.next_request() が返す enum の型名（`publisher::Request` だと「Publisher が送るリクエスト」に見える問題）

## 未解決（まだ議論していない）

- 複数 Track を同時に publish する場合のパターン
- Subscription Filter の指定方法
- Relay の複数セッション間の連携、Cache の置き場所
- Subscription Aggregation のレイヤー
