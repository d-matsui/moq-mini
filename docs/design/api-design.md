# API 設計

## 確定した API

### Client: Publisher 側

```rust
// 接続 → Session, Publisher, Subscriber に分離
let (session, publisher, _subscriber) = Session::connect(addr).await?;
tokio::spawn(session.run());  // TODO: session.run() の構造は未決定

// Namespace 登録（戻り値なし。stream は Session 内部で保持。Session drop で全 stream 閉じる）
// PUBLISH_NAMESPACE なしでも SUBSCRIBE は来うる（仕様上 optional）
publisher.publish_namespace(ns).await?;

// bidi stream のリクエストを受け取る（SUBSCRIBE, FETCH が enum で返る）
// uni stream のデータは session.run() 内部で subscription に自動振り分け
loop {
    match publisher.next_request().await? {
        Request::Subscribe(req) => {
            // req.namespace, req.track_name, req.parameters で判断して ok/error
            let track = req.ok().await?;  // SUBSCRIBE_OK を送る → Track が返る（仮名）
            tokio::spawn(async move {
                // track_alias, group_id はライブラリ内部で管理
                let subgroup = track.open_subgroup().await?;  // uni stream を開く
                subgroup.write_object(&payload).await?;
                subgroup.finish()?;  // uni stream を閉じる（FIN）
                track.done().await?;  // PUBLISH_DONE を送る
                // 詳細版: open_subgroup_with(SubgroupParams) / write_object_with(&payload, ObjectParams)
            });
        }
        Request::Fetch(req) => {
            req.error().await?;  // 最小実装では未対応
        }
        Request::TrackStatus(req) => {
            req.error().await?;  // 最小実装では未対応
        }
    }
}
```

### Client: Subscriber 側

```rust
let (session, _publisher, subscriber) = Session::connect(addr).await?;
tokio::spawn(session.run());

// Subscribe → Track を得る（仮名。送信側と受信側で別の型になる可能性あり）
let track = subscriber.subscribe(ns, "video").await?;

// Subgroup 単位で受信（1 Subgroup = 1 uni stream。HoL しない）
loop {
    let subgroup = match track.next_subgroup().await {
        Some(sg) => sg,
        None => break, // PUBLISH_DONE or 接続終了
    };
    tokio::spawn(async move {
        while let Some(object) = subgroup.next_object().await {
            process(object);
        }
    });
}
```

### Relay

```rust
// TODO: 未決定
```

## 検討中（案はあるが未決定）

- session.run() の構造（バックグラウンドで accept_bi/accept_uni して振り分ける案）
- subscribe_request.accept() の戻り値（Track? 別の何か?）
- データ送信の粒度（Track → Group → Object? Track → Subgroup → Object?）
- データ受信の方法（subscription.next_subgroup() → Subgroup → next_object() の案）
- Subgroup の送信側/受信側の型名（SubgroupReader/Writer? Subgroup で統一?）
- Relay のデータパススルー API
- read_object / write_object に与える型
- Subscriber 側の next_request() は必要か（PUBLISH_NAMESPACE を受け取る側）

## 未解決（まだ議論していない）

- 複数 Track を同時に publish する場合のパターン
- Group ID は自動採番でいいか
- Subscription Filter の指定方法
- Fetch の扱い
- Relay の複数セッション間の連携、Cache の置き場所
- Subscription Aggregation のレイヤー
