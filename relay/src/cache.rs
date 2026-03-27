//! # cache: Per-track object cache for cache-through relay
//!
//! Stores objects received from publishers so that:
//! 1. All data flows through the cache (write-through model)
//! 2. Late-joining subscribers can determine the current position
//! 3. Future FETCH requests can serve cached objects
//!
//! Each `TrackCache` is associated with one upstream subscription
//! (one publisher, one track) and shared across all aggregated subscribers.

use std::collections::BTreeMap;

use tokio::sync::{Notify, RwLock};

use moqt::wire::subgroup_header::SubgroupHeader;

/// Default number of groups to retain in the cache.
const DEFAULT_MAX_GROUPS: usize = 30;

/// A cached object within a group.
pub(crate) struct CachedObject {
    /// Absolute Object ID (resolved from delta encoding).
    pub object_id: u64,
    /// Raw object header bytes (delta + length [+ properties]).
    /// Used for efficient pass-through to subscribers.
    pub header_bytes: Vec<u8>,
    /// Object payload bytes.
    pub payload: Vec<u8>,
}

/// A cached group within a track.
struct GroupCache {
    /// SubgroupHeader for this group (as received from publisher).
    header: SubgroupHeader,
    /// Objects in this group, in order of arrival.
    objects: Vec<CachedObject>,
    /// Whether the publisher has finished this group (stream FIN received).
    complete: bool,
}

/// Internal cache state, protected by RwLock.
struct TrackCacheInner {
    /// Groups keyed by Group ID, ordered.
    groups: BTreeMap<u64, GroupCache>,
    /// Largest (group_id, object_id) seen so far.
    largest_object: Option<(u64, u64)>,
    /// Whether the publisher is done (no more data will arrive).
    closed: bool,
    /// Maximum number of groups to retain. Oldest groups are evicted.
    max_groups: usize,
}

/// Per-track object cache.
///
/// Thread-safe and shared across tasks via `Arc<TrackCache>`.
/// Writers (data handler) and readers (subscriber relay tasks) coordinate
/// through the internal RwLock and Notify.
pub(crate) struct TrackCache {
    inner: RwLock<TrackCacheInner>,
    /// Notified when new objects are added, groups complete, or cache closes.
    notify: Notify,
}

impl TrackCache {
    /// Create an empty cache with the default group retention limit.
    pub fn new() -> Self {
        Self::with_max_groups(DEFAULT_MAX_GROUPS)
    }

    /// Create an empty cache with a custom group retention limit.
    pub fn with_max_groups(max_groups: usize) -> Self {
        Self {
            inner: RwLock::new(TrackCacheInner {
                groups: BTreeMap::new(),
                largest_object: None,
                closed: false,
                max_groups,
            }),
            notify: Notify::new(),
        }
    }

    /// Start a new group in the cache.
    /// If the group already exists, this is a no-op.
    pub async fn begin_group(&self, group_id: u64, header: SubgroupHeader) {
        let mut inner = self.inner.write().await;
        inner.groups.entry(group_id).or_insert_with(|| GroupCache {
            header,
            objects: Vec::new(),
            complete: false,
        });
        self.evict_old_groups(&mut inner);
        self.notify.notify_waiters();
    }

    /// Add an object to a group. Updates largest_object and notifies waiters.
    /// Panics if the group has not been started with `begin_group`.
    pub async fn push_object(&self, group_id: u64, object: CachedObject) {
        let mut inner = self.inner.write().await;
        let object_id = object.object_id;

        let group = inner
            .groups
            .get_mut(&group_id)
            .expect("push_object called before begin_group");
        group.objects.push(object);

        // Update largest_object if this is the largest seen
        let is_larger = match inner.largest_object {
            None => true,
            Some((g, o)) => group_id > g || (group_id == g && object_id > o),
        };
        if is_larger {
            inner.largest_object = Some((group_id, object_id));
        }

        self.notify.notify_waiters();
    }

    /// Mark a group as complete (publisher stream FIN received).
    pub async fn complete_group(&self, group_id: u64) {
        let mut inner = self.inner.write().await;
        if let Some(group) = inner.groups.get_mut(&group_id) {
            group.complete = true;
        }
        self.notify.notify_waiters();
    }

    /// Mark the cache as closed (publisher done, no more data will arrive).
    pub async fn close(&self) {
        let mut inner = self.inner.write().await;
        inner.closed = true;
        self.notify.notify_waiters();
    }

    /// Get the largest (group_id, object_id) in the cache, if any.
    pub async fn largest_object(&self) -> Option<(u64, u64)> {
        self.inner.read().await.largest_object
    }

    /// Get the SubgroupHeader for a group, if it exists.
    pub async fn get_group_header(&self, group_id: u64) -> Option<SubgroupHeader> {
        self.inner
            .read()
            .await
            .groups
            .get(&group_id)
            .map(|g| g.header.clone())
    }

    /// Check if a group exists in the cache.
    pub async fn has_group(&self, group_id: u64) -> bool {
        self.inner.read().await.groups.contains_key(&group_id)
    }

    /// Read objects from a group starting at the given index.
    /// Returns (object slices as (object_id, header_bytes, payload), is_complete).
    /// Returns empty vec if group doesn't exist or from_index is past end.
    pub async fn read_objects(
        &self,
        group_id: u64,
        from_index: usize,
    ) -> (Vec<(u64, Vec<u8>, Vec<u8>)>, bool) {
        let inner = self.inner.read().await;
        match inner.groups.get(&group_id) {
            Some(group) => {
                let objects: Vec<_> = group
                    .objects
                    .iter()
                    .skip(from_index)
                    .map(|o| (o.object_id, o.header_bytes.clone(), o.payload.clone()))
                    .collect();
                (objects, group.complete)
            }
            None => (Vec::new(), false),
        }
    }

    /// Check if the cache is closed.
    pub async fn is_closed(&self) -> bool {
        self.inner.read().await.closed
    }

    /// Wait for any update (new object, group complete, or close).
    pub async fn wait_for_update(&self) {
        self.notify.notified().await;
    }

    /// Evict the oldest groups if we exceed max_groups.
    fn evict_old_groups(&self, inner: &mut TrackCacheInner) {
        while inner.groups.len() > inner.max_groups {
            // Remove the smallest group_id (oldest)
            if let Some(&oldest_key) = inner.groups.keys().next() {
                inner.groups.remove(&oldest_key);
            }
        }
    }

    /// Get the number of groups currently in cache (for testing).
    #[cfg(test)]
    async fn group_count(&self) -> usize {
        self.inner.read().await.groups.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moqt::wire::subgroup_header::SubgroupHeader;

    fn make_header(group_id: u64) -> SubgroupHeader {
        SubgroupHeader {
            track_alias: 1,
            group_id,
            has_properties: false,
            end_of_group: true,
            subgroup_id: Some(0),
            publisher_priority: None,
        }
    }

    fn make_object(object_id: u64) -> CachedObject {
        CachedObject {
            object_id,
            header_bytes: vec![0x00, 0x05], // delta=0, length=5
            payload: format!("obj{object_id}").into_bytes(),
        }
    }

    #[tokio::test]
    async fn empty_cache_has_no_largest_object() {
        let cache = TrackCache::new();
        assert_eq!(cache.largest_object().await, None);
        assert!(!cache.is_closed().await);
    }

    #[tokio::test]
    async fn push_updates_largest_object() {
        let cache = TrackCache::new();

        cache.begin_group(0, make_header(0)).await;
        cache.push_object(0, make_object(0)).await;
        assert_eq!(cache.largest_object().await, Some((0, 0)));

        cache.push_object(0, make_object(1)).await;
        assert_eq!(cache.largest_object().await, Some((0, 1)));

        // New group with higher group_id
        cache.begin_group(1, make_header(1)).await;
        cache.push_object(1, make_object(0)).await;
        assert_eq!(cache.largest_object().await, Some((1, 0)));
    }

    #[tokio::test]
    async fn largest_object_not_decreased_by_lower_group() {
        let cache = TrackCache::new();

        cache.begin_group(5, make_header(5)).await;
        cache.push_object(5, make_object(3)).await;
        assert_eq!(cache.largest_object().await, Some((5, 3)));

        // Object in earlier group should not decrease largest_object
        cache.begin_group(3, make_header(3)).await;
        cache.push_object(3, make_object(10)).await;
        assert_eq!(cache.largest_object().await, Some((5, 3)));
    }

    #[tokio::test]
    async fn evicts_old_groups_beyond_max() {
        let cache = TrackCache::with_max_groups(3);

        for i in 0..5 {
            cache.begin_group(i, make_header(i)).await;
            cache.push_object(i, make_object(0)).await;
        }

        // Should have evicted groups 0 and 1
        assert_eq!(cache.group_count().await, 3);
        assert!(!cache.has_group(0).await);
        assert!(!cache.has_group(1).await);
        assert!(cache.has_group(2).await);
        assert!(cache.has_group(3).await);
        assert!(cache.has_group(4).await);
    }

    #[tokio::test]
    async fn complete_group_marks_finished() {
        let cache = TrackCache::new();
        cache.begin_group(0, make_header(0)).await;
        cache.push_object(0, make_object(0)).await;

        // Before complete
        let (_, complete) = cache.read_objects(0, 0).await;
        assert!(!complete);

        cache.complete_group(0).await;

        // After complete
        let (_, complete) = cache.read_objects(0, 0).await;
        assert!(complete);
    }

    #[tokio::test]
    async fn close_marks_closed() {
        let cache = TrackCache::new();
        assert!(!cache.is_closed().await);
        cache.close().await;
        assert!(cache.is_closed().await);
    }

    #[tokio::test]
    async fn read_objects_from_index() {
        let cache = TrackCache::new();
        cache.begin_group(0, make_header(0)).await;

        for i in 0..5 {
            cache.push_object(0, make_object(i)).await;
        }

        // Read all
        let (objects, _) = cache.read_objects(0, 0).await;
        assert_eq!(objects.len(), 5);
        assert_eq!(objects[0].0, 0); // object_id
        assert_eq!(objects[4].0, 4);

        // Read from index 3
        let (objects, _) = cache.read_objects(0, 3).await;
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].0, 3);
        assert_eq!(objects[1].0, 4);

        // Read past end
        let (objects, _) = cache.read_objects(0, 10).await;
        assert!(objects.is_empty());
    }

    #[tokio::test]
    async fn read_objects_nonexistent_group() {
        let cache = TrackCache::new();
        let (objects, complete) = cache.read_objects(99, 0).await;
        assert!(objects.is_empty());
        assert!(!complete);
    }

    #[tokio::test]
    async fn get_group_header_returns_correct_header() {
        let cache = TrackCache::new();
        let header = make_header(5);
        cache.begin_group(5, header.clone()).await;

        let retrieved = cache.get_group_header(5).await;
        assert_eq!(retrieved, Some(header));

        assert_eq!(cache.get_group_header(99).await, None);
    }

    #[tokio::test]
    async fn notify_wakes_on_push() {
        let cache = std::sync::Arc::new(TrackCache::new());
        cache.begin_group(0, make_header(0)).await;

        let cache2 = cache.clone();
        let handle = tokio::spawn(async move {
            cache2.wait_for_update().await;
            cache2.largest_object().await
        });

        // Give the waiter time to register
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        cache.push_object(0, make_object(0)).await;

        let result = tokio::time::timeout(std::time::Duration::from_millis(100), handle)
            .await
            .expect("waiter should wake up")
            .expect("task should not panic");
        assert_eq!(result, Some((0, 0)));
    }
}
