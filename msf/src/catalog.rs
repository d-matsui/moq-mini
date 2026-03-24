//! # catalog: MSF Catalog (Section 5)
//!
//! A Catalog is a JSON document that describes the tracks being produced
//! by an MSF publisher. Subscribers parse the catalog to discover
//! available tracks and their properties.
//!
//! The catalog track MUST have a case-sensitive Track Name of "catalog".

use serde::{Deserialize, Serialize};

/// The MOQT track name for the catalog track.
pub const CATALOG_TRACK_NAME: &str = "catalog";

/// MSF Catalog (Section 5).
///
/// A JSON document describing the tracks being produced by an MSF publisher.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Catalog {
    /// MSF version (Section 5.1.1). Required.
    pub version: u64,

    /// Wallclock time in ms since epoch when this catalog was generated (Section 5.1.6).
    #[serde(rename = "generatedAt", skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<u64>,

    /// Whether the broadcast is complete (Section 5.1.7).
    #[serde(rename = "isComplete", skip_serializing_if = "Option::is_none")]
    pub is_complete: Option<bool>,

    /// Array of track objects (Section 5.1.8). Required.
    pub tracks: Vec<Track>,

    // Delta update fields (Section 5.2)
    /// Marks this as a delta update (Section 5.1.2).
    #[serde(rename = "deltaUpdate", skip_serializing_if = "Option::is_none")]
    pub delta_update: Option<bool>,

    /// Tracks to add in a delta update (Section 5.1.3).
    #[serde(rename = "addTracks", skip_serializing_if = "Option::is_none")]
    pub add_tracks: Option<Vec<Track>>,

    /// Tracks to remove in a delta update (Section 5.1.4).
    #[serde(rename = "removeTracks", skip_serializing_if = "Option::is_none")]
    pub remove_tracks: Option<Vec<Track>>,

    /// Tracks to clone in a delta update (Section 5.1.5).
    #[serde(rename = "cloneTracks", skip_serializing_if = "Option::is_none")]
    pub clone_tracks: Option<Vec<Track>>,
}

/// A track object within the catalog (Section 5.1.9).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Track {
    /// Track name (Section 5.1.11). Required.
    pub name: String,

    /// Track namespace (Section 5.1.10).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Payload encapsulation type (Section 5.1.12). Required.
    pub packaging: Packaging,

    /// True if new objects will be added (Section 5.1.15). Required.
    #[serde(rename = "isLive")]
    pub is_live: bool,

    /// Target latency in ms (Section 5.1.16).
    #[serde(rename = "targetLatency", skip_serializing_if = "Option::is_none")]
    pub target_latency: Option<u64>,

    /// Content role (Section 5.1.14).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,

    /// Human-readable label (Section 5.1.17).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,

    /// Render group number (Section 5.1.18).
    #[serde(rename = "renderGroup", skip_serializing_if = "Option::is_none")]
    pub render_group: Option<u64>,

    /// Alternate group number (Section 5.1.19).
    #[serde(rename = "altGroup", skip_serializing_if = "Option::is_none")]
    pub alt_group: Option<u64>,

    /// Base64-encoded initialization data (Section 5.1.20).
    #[serde(rename = "initData", skip_serializing_if = "Option::is_none")]
    pub init_data: Option<String>,

    /// Track name dependencies (Section 5.1.21).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depends: Option<Vec<String>>,

    /// Temporal layer ID (Section 5.1.22).
    #[serde(rename = "temporalId", skip_serializing_if = "Option::is_none")]
    pub temporal_id: Option<u64>,

    /// Spatial layer ID (Section 5.1.23).
    #[serde(rename = "spatialId", skip_serializing_if = "Option::is_none")]
    pub spatial_id: Option<u64>,

    /// Codec identifier per WebCodecs Codec Registry (Section 5.1.24).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,

    /// MIME type (Section 5.1.25).
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,

    /// Video framerate in fps (Section 5.1.26).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub framerate: Option<f64>,

    /// Time units per second (Section 5.1.27).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timescale: Option<u64>,

    /// Bitrate in bits per second (Section 5.1.28).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitrate: Option<u64>,

    /// Encoded video width in pixels (Section 5.1.29).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<u64>,

    /// Encoded video height in pixels (Section 5.1.30).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<u64>,

    /// Audio sample rate (Section 5.1.31).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samplerate: Option<u64>,

    /// Audio channel configuration (Section 5.1.32).
    #[serde(rename = "channelConfig", skip_serializing_if = "Option::is_none")]
    pub channel_config: Option<String>,

    /// Display width in pixels (Section 5.1.33).
    #[serde(rename = "displayWidth", skip_serializing_if = "Option::is_none")]
    pub display_width: Option<u64>,

    /// Display height in pixels (Section 5.1.34).
    #[serde(rename = "displayHeight", skip_serializing_if = "Option::is_none")]
    pub display_height: Option<u64>,

    /// Language tag per BCP 47 (Section 5.1.35).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lang: Option<String>,

    /// Parent track name for cloning (Section 5.1.36).
    #[serde(rename = "parentName", skip_serializing_if = "Option::is_none")]
    pub parent_name: Option<String>,

    /// Track duration in ms (Section 5.1.37).
    #[serde(rename = "trackDuration", skip_serializing_if = "Option::is_none")]
    pub track_duration: Option<u64>,

    /// Event timeline type (Section 5.1.13).
    #[serde(rename = "eventType", skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
}

/// Payload encapsulation type (Section 5.1.12, Table 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Packaging {
    /// LOC packaging
    #[default]
    Loc,
    /// Media Timeline track
    #[serde(rename = "mediatimeline")]
    MediaTimeline,
    /// Event Timeline track
    #[serde(rename = "eventtimeline")]
    EventTimeline,
}

/// Track content role (Section 5.1.14, Table 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Video,
    Audio,
    #[serde(rename = "audiodescription")]
    AudioDescription,
    #[serde(rename = "mediatimeline")]
    MediaTimeline,
    #[serde(rename = "eventtimeline")]
    EventTimeline,
    Caption,
    Subtitle,
    #[serde(rename = "signlanguage")]
    SignLanguage,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Section 5.3.1: Time-aligned Audio/Video Tracks with single quality
    const EXAMPLE_5_3_1: &str = r#"{
  "version": 1,
  "generatedAt": 1746104606044,
  "tracks": [
    {
      "name": "1080p-video",
      "namespace": "conference.example.com/conference123/alice",
      "packaging": "loc",
      "isLive": true,
      "targetLatency": 2000,
      "role": "video",
      "renderGroup": 1,
      "codec": "av01.0.08M.10.0.110.09",
      "width": 1920,
      "height": 1080,
      "framerate": 30,
      "bitrate": 1500000
    },
    {
      "name": "audio",
      "namespace": "conference.example.com/conference123/alice",
      "packaging": "loc",
      "isLive": true,
      "targetLatency": 2000,
      "role": "audio",
      "renderGroup": 1,
      "codec": "opus",
      "samplerate": 48000,
      "channelConfig": "2",
      "bitrate": 32000
    }
  ]
}"#;

    #[test]
    fn deserialize_example_5_3_1() {
        let catalog: Catalog = serde_json::from_str(EXAMPLE_5_3_1).unwrap();

        assert_eq!(catalog.version, 1);
        assert_eq!(catalog.generated_at, Some(1746104606044));
        assert_eq!(catalog.tracks.len(), 2);

        // Video track
        let video = &catalog.tracks[0];
        assert_eq!(video.name, "1080p-video");
        assert_eq!(
            video.namespace.as_deref(),
            Some("conference.example.com/conference123/alice")
        );
        assert_eq!(video.packaging, Packaging::Loc);
        assert!(video.is_live);
        assert_eq!(video.target_latency, Some(2000));
        assert_eq!(video.role.as_ref(), Some(&Role::Video));
        assert_eq!(video.render_group, Some(1));
        assert_eq!(video.codec.as_deref(), Some("av01.0.08M.10.0.110.09"));
        assert_eq!(video.width, Some(1920));
        assert_eq!(video.height, Some(1080));
        assert_eq!(video.framerate, Some(30.0));
        assert_eq!(video.bitrate, Some(1500000));

        // Audio track
        let audio = &catalog.tracks[1];
        assert_eq!(audio.name, "audio");
        assert_eq!(audio.packaging, Packaging::Loc);
        assert!(audio.is_live);
        assert_eq!(audio.role.as_ref(), Some(&Role::Audio));
        assert_eq!(audio.codec.as_deref(), Some("opus"));
        assert_eq!(audio.samplerate, Some(48000));
        assert_eq!(audio.channel_config.as_deref(), Some("2"));
        assert_eq!(audio.bitrate, Some(32000));
    }

    #[test]
    fn serialize_roundtrip() {
        let catalog = Catalog {
            version: 1,
            generated_at: Some(1746104606044),
            tracks: vec![
                Track {
                    name: "1080p-video".to_string(),
                    namespace: Some("conference.example.com/conference123/alice".to_string()),
                    packaging: Packaging::Loc,
                    is_live: true,
                    target_latency: Some(2000),
                    role: Some(Role::Video),
                    render_group: Some(1),
                    codec: Some("av01.0.08M.10.0.110.09".to_string()),
                    width: Some(1920),
                    height: Some(1080),
                    framerate: Some(30.0),
                    bitrate: Some(1500000),
                    ..Track::default()
                },
                Track {
                    name: "audio".to_string(),
                    namespace: Some("conference.example.com/conference123/alice".to_string()),
                    packaging: Packaging::Loc,
                    is_live: true,
                    target_latency: Some(2000),
                    role: Some(Role::Audio),
                    render_group: Some(1),
                    codec: Some("opus".to_string()),
                    samplerate: Some(48000),
                    channel_config: Some("2".to_string()),
                    bitrate: Some(32000),
                    ..Track::default()
                },
            ],
            ..Catalog::default()
        };

        let json = serde_json::to_string(&catalog).unwrap();
        let deserialized: Catalog = serde_json::from_str(&json).unwrap();
        assert_eq!(catalog, deserialized);
    }

    #[test]
    fn minimal_catalog() {
        let json =
            r#"{"version": 1, "tracks": [{"name": "v", "packaging": "loc", "isLive": true}]}"#;
        let catalog: Catalog = serde_json::from_str(json).unwrap();
        assert_eq!(catalog.version, 1);
        assert_eq!(catalog.tracks.len(), 1);
        assert_eq!(catalog.tracks[0].name, "v");
    }

    #[test]
    fn unknown_fields_ignored() {
        let json = r#"{"version": 1, "futureField": 42, "tracks": [{"name": "v", "packaging": "loc", "isLive": true, "customProp": "x"}]}"#;
        let catalog: Catalog = serde_json::from_str(json).unwrap();
        assert_eq!(catalog.version, 1);
        assert_eq!(catalog.tracks[0].name, "v");
    }

    #[test]
    fn missing_required_version_is_error() {
        let json = r#"{"tracks": [{"name": "v", "packaging": "loc", "isLive": true}]}"#;
        assert!(serde_json::from_str::<Catalog>(json).is_err());
    }

    #[test]
    fn missing_required_track_name_is_error() {
        let json = r#"{"version": 1, "tracks": [{"packaging": "loc", "isLive": true}]}"#;
        assert!(serde_json::from_str::<Catalog>(json).is_err());
    }

    #[test]
    fn optional_fields_absent_in_json() {
        let catalog = Catalog {
            version: 1,
            tracks: vec![Track {
                name: "v".to_string(),
                packaging: Packaging::Loc,
                is_live: true,
                ..Track::default()
            }],
            ..Catalog::default()
        };
        let json = serde_json::to_string(&catalog).unwrap();
        // Optional None fields should not appear in output
        assert!(!json.contains("namespace"));
        assert!(!json.contains("codec"));
        assert!(!json.contains("width"));
        assert!(!json.contains("generatedAt"));
    }
}
