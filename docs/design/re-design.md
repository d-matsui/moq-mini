# 概念と使い方

## Session

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

## Publisher / Subscriber

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

## Request

bidi stream 上でやり取りされるリクエスト。
Publisher と Subscriber はそれぞれ異なる種類のリクエストを受け取る:

- **Publisher が受け取る**: SUBSCRIBE, FETCH（`publisher.next_request()`）
- **Subscriber が受け取る**: PUBLISH_NAMESPACE（`subscriber.next_request()`）

受け取った側は `ok()` で承認するか `error()` で拒否する。
承認すると、データの送受信に使う型が返る:

- `SubscribeRequest.ok()` → SubscribeSender（Object を書いて配信する）
- `FetchRequest.ok()` → FetchSender（Object を書いて返す）
- `PublishNamespaceRequest.ok()` → ()（bidi stream は開いたままで、Publisher が取り下げるまで namespace が有効）

```rust
// Publisher 側が受け取るリクエスト
match publisher.next_request().await? {
    Request::Subscribe(req) => {
        // req.namespace, req.track_name, req.parameters で判断
        let subscribe_sender = req.ok().await?;   // SUBSCRIBE_OK → SubscribeSender
    }
    Request::Fetch(req) => {
        let fetch_sender = req.ok().await?;   // FETCH_OK → FetchSender
    }
}

// Subscriber 側が受け取るリクエスト
match subscriber.next_request().await? {
    SubscriberRequest::PublishNamespace(req) => {
        // req.namespace で判断
        req.ok().await?;                          // REQUEST_OK
    }
}

// or
req.error().await?;  // REQUEST_ERROR
```

## Sender / Receiver

Request の `ok()` が返す型。**request 単位の高レベル層**で、内部に bidi stream を保持する。
Sender は配信側、Receiver は受信側。
実際の Object の読み書きは行わず、`open()` / `accept()` で次の層（Writer / Reader）を作る。

Request 種別ごとに対応する型がある:

- **SubscribeSender / SubscribeReceiver**（SUBSCRIBE 用）
- **FetchSender / FetchReceiver**（FETCH 用）

### SubscribeSender / SubscribeReceiver

SUBSCRIBE が成立したあと、Object を送受するのに使う型。
内部に SUBSCRIBE の bidi stream と、group_id カウンタ、track_alias を保持する。

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

> **将来 1 Group に複数 Subgroup が必要になった場合**
> `open(group_id, subgroup_id)` で group_id / subgroup_id を明示的に指定できるようにする。

### FetchSender / FetchReceiver

1つの FETCH に対応する型。内部に FETCH の bidi stream を保持する。
`open()` / `accept()` でデータ送受用の uni stream を取得し、FetchWriter / FetchReader で Object を読み書きする。

FETCH は 1 request = 1 uni stream しか使わないため、`open()` / `accept()` は consuming（`self` を取る）。
2回目を呼ぼうとするとコンパイルエラーになる。

```rust
// 送信側（Publisher）
let fetch_sender = req.ok().await?;
let writer = fetch_sender.open().await?;    // fetch_sender はここで消費される
writer.write_object(&object).await?;
writer.finish()?;

// 受信側（Subscriber）
let fetch_receiver = subscriber.fetch(ns, "video", range).await?;
let reader = fetch_receiver.accept().await?;   // fetch_receiver はここで消費される
while let Some(object) = reader.read_object().await {
    process(object);
}
```

> **なぜ SUBSCRIBE と FETCH で同じ形にするのか？**
> FETCH は実際には 1 uni stream しか使わないので、`fetch_sender.write_object()` を直接生やす設計もあり得る。
> ただし SUBSCRIBE と FETCH で呼び出しパターンが違うと、利用者は request 種別ごとに別の書き方を覚える必要がある。
> 対称にしておくと「ok() → Sender → open() → Writer → write_object → finish」という統一ルールが全 request に適用でき、認知負荷が下がる。
> FETCH で `open()` が consuming なのは、1 stream しかない制約を型レベルで表現しているため。

## Writer / Reader

Sender / Receiver の `open()` / `accept()` が返す型。**uni stream 単位の低レベル層**で、内部に uni stream を保持する。
Writer が送信側、Reader が受信側で、Object の読み書きと `finish()` による stream のクローズを行う。

Request 種別ごとに対応する型がある:

- **SubgroupWriter / SubgroupReader**（SUBSCRIBE 用、1 Subgroup = 1 uni stream）
- **FetchWriter / FetchReader**（FETCH 用、1 FETCH = 1 uni stream）

### SubgroupWriter / SubgroupReader

`SubscribeSender::open()` / `SubscribeReceiver::accept()` で得られる、1本の uni stream に対応する型。
内部に uni stream と group_id / subgroup_id を保持し、Object を読み書きする。
`finish()` で uni stream を閉じる。

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

### FetchWriter / FetchReader

`FetchSender::open()` / `FetchReceiver::accept()` で得られる、FETCH データ用の uni stream に対応する型。
内部に FETCH の bidi stream と uni stream を保持し、Object を読み書きする。
`finish()` で uni stream を閉じる。FETCH に PUBLISH_DONE 相当のメッセージはなく、uni stream の FIN がそのまま終了シグナルになる。

## Object

データの最小単位。各フィールド:

- **id** — Object ID。ユーザーが指定する（自動採番しない。Relay が元の ID を保持して転送するため）
- **status** — Normal / EndOfGroup / EndOfTrack。Normal 以外は payload が空
- **properties** — Key-Value メタデータ。Relay はそのまま転送する。アプリケーション固有の用途に使える
- **payload** — バイト列。MOQT はメディアに依存しないので、中身はアプリケーションが決める

# 型と API（リファレンス）

```
Session
├── Publisher                  データを配信する側。SUBSCRIBE を受けて配信を開始する
│   ├── .publish_namespace(ns)    配信可能な namespace を Relay に登録する
│   └── .next_request() → Request
│       ├── Subscribe(SubscribeRequest)
│       │   ├── .ok()    → SubscribeSender    承認して Object の送信を開始する
│       │   └── .error()                  拒否する
│       └── Fetch(FetchRequest)
│           ├── .ok()    → FetchSender    承認して Object の送信側を得る
│           └── .error()                  拒否する
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

FetchSender                        1 つの FETCH に対応。bidi stream を管理する
└── .open(self)   → FetchWriter    uni stream を開く（consuming、1回のみ）

FetchReceiver                      1 つの FETCH に対応。bidi stream を管理する
└── .accept(self) → FetchReader    相手が開いた uni stream を受け取る（consuming、1回のみ）

FetchWriter                        FETCH データ用の 1 uni stream の書き込み側
├── .write_object(&Object)         Object を書く
└── .finish()                      uni stream を閉じる（FIN = 終了シグナル）

FetchReader                        FETCH データ用の 1 uni stream の読み出し側
└── .read_object() → Object?       次の Object を読む（FIN で None）

Object                             1 つの Object のデータ
├── id: u64                        Object ID
├── status: ObjectStatus           Normal / EndOfGroup / EndOfTrack
├── properties: Properties         Key-Value メタデータ（Relay はそのまま転送）
└── payload: Bytes                 ペイロード（status が Normal のときのみ）
```

# Relay

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

# 内部構造

## stream の種類

MOQT では 3 種類の stream を使う:

- **control stream（uni × 2）**: 各 peer が 1 本ずつ uni stream を開き、SETUP を交換する。セッション中ずっと開いたまま
- **request stream（bidi）**: SUBSCRIBE, FETCH, PUBLISH_NAMESPACE 等のリクエストごとに 1 本開く
- **data stream（uni）**: Subgroup の Object データ、FETCH のレスポンスデータ

## session.run()

バックグラウンドで bidi / uni stream を accept し、内部の channel を通じて Publisher / Subscriber / SubscribeReceiver / FetchReceiver に振り分ける。

bidi / uni どちらも同じパターン: **先頭の type を読む → match で分岐 → 具体的な型を構築 → channel に送る**。

**bidi stream**（request stream）は request type で振り分ける:
- SUBSCRIBE / FETCH → `publisher_tx`（`publisher.next_request()` で受け取る）
- PUBLISH_NAMESPACE → `subscriber_tx`（`subscriber.next_request()` で受け取る）

**uni stream**（data stream）は stream type で振り分ける:
- SUBGROUP_HEADER → ヘッダの **track_alias** で該当する SubscribeReceiver を引いて送る
- FETCH_HEADER → ヘッダの **request_id** で該当する FetchReceiver を引いて送る

```rust
async fn run(self) {
    loop {
        tokio::select! {
            stream = self.conn.accept_bi() => {
                // Request まで組み立ててから dispatch する
                let request = Request::accept(stream).await?;
                match &request {
                    Request::Subscribe(_) | Request::Fetch(_) => {
                        self.publisher_tx.send(request).await?;
                    }
                    Request::PublishNamespace(_) => {
                        self.subscriber_tx.send(request).await?;
                    }
                }
            }
            stream = self.conn.accept_uni() => {
                // Reader まで組み立ててから dispatch する
                match read_data_stream_type(&mut stream).await? {
                    DataStreamType::Subgroup => {
                        let reader = SubgroupReader::accept(stream).await?;
                        self.subscribe_receivers.get(reader.track_alias()).send(reader).await?;
                    }
                    DataStreamType::Fetch => {
                        let reader = FetchReader::accept(stream).await?;
                        self.fetch_receivers.get(reader.request_id()).send(reader).await?;
                    }
                }
            }
        }
    }
}
```

- `Request::accept(stream)` は内部で request type を peek し、具体的な Request 型（SubscribeRequest / FetchRequest / PublishNamespaceRequest）を構築して `Request` enum で返す
- session.run() は variant を見て dispatch 先を決めるだけ。ばらして包み直す操作がない
- uni 側は umbrella 型がないので、peek → 構築 → 送信の3ステップを明示的に書く（Reader が具体的な型ごとに track_alias / request_id を持つため、match 内で直接 dispatch 先を決める方が自然）

受信側:
- `publisher.next_request()` / `subscriber.next_request()` は内部で channel から Request を受け取るだけ
- `subscribe_receiver.accept()` / `fetch_receiver.accept()` は内部で channel から Reader を受け取るだけ

# 検討中（案はあるが未決定）

- Relay のデータパススルー API
- Subscriber 側の next_request() は必要か（PUBLISH_NAMESPACE を受け取る側）
- publisher.next_request() / subscriber.next_request() が返す enum の型名（`publisher::Request` だと「Publisher が送るリクエスト」に見える問題）

# 未解決（まだ議論していない）

- 複数 Track を同時に publish する場合のパターン
- Subscription Filter の指定方法
- Relay の複数セッション間の連携、Cache の置き場所
- Subscription Aggregation のレイヤー
- Session 内部の channel 設計（publisher_tx / subscribe_receivers[track_alias] 等の生成・登録タイミング）
