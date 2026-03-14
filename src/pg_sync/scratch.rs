//! Disk-backed scratch shard writer/reader for scatter-gather bulk loading.
//!
//! Phase 1 (scatter): CSV rows are routed to shard files based on `slot >> SHARD_SHIFT`.
//! Phase 2 (gather): shard files are read, sorted by slot, and processed in parallel.
//!
//! Shard shift of 16 gives 65536 slots/shard ≈ 2048 shards for 107M records.
//! Each shard file is 20-40 MB — small enough to sort in memory, large enough
//! for efficient I/O.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write as IoWrite};
use std::path::{Path, PathBuf};

/// Default shard shift: slot >> 16 = 65536 slots per shard.
pub const DEFAULT_SHARD_SHIFT: u32 = 16;

// ---------------------------------------------------------------------------
// Field tags — identify the source table for each scratch tuple
// ---------------------------------------------------------------------------

pub const TAG_IMAGE_SCALARS: u8 = 0x01;
pub const TAG_TAG_ID: u8 = 0x02;
pub const TAG_TOOL_ID: u8 = 0x03;
pub const TAG_TECHNIQUE_ID: u8 = 0x04;
pub const TAG_MODEL_VERSION_ID: u8 = 0x05;
pub const TAG_BASE_MODEL: u8 = 0x06;
pub const TAG_RESOURCE_POI: u8 = 0x07;

// ---------------------------------------------------------------------------
// ScratchWriter — append-only binary writer with lazy file handle creation
// ---------------------------------------------------------------------------

/// Append-only writer that routes tuples to shard files by slot ID.
///
/// Files are opened lazily on first write and kept open for the duration.
/// BufWriter handles write buffering (64 KB per handle).
pub struct ScratchWriter {
    dir: PathBuf,
    shard_shift: u32,
    writers: HashMap<u32, BufWriter<File>>,
    tuples_written: u64,
    bytes_written: u64,
}

impl ScratchWriter {
    /// Create a new ScratchWriter that writes to `dir`.
    /// Creates the directory if it doesn't exist.
    pub fn new(dir: &Path, shard_shift: u32) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(ScratchWriter {
            dir: dir.to_path_buf(),
            shard_shift,
            writers: HashMap::new(),
            tuples_written: 0,
            bytes_written: 0,
        })
    }

    /// Get (or create) the BufWriter for a shard.
    fn writer_for(&mut self, shard_id: u32) -> io::Result<&mut BufWriter<File>> {
        if !self.writers.contains_key(&shard_id) {
            let path = self.dir.join(format!("{:06x}.bin", shard_id));
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            self.writers
                .insert(shard_id, BufWriter::with_capacity(64 * 1024, file));
        }
        Ok(self.writers.get_mut(&shard_id).unwrap())
    }

    /// Write a raw tuple: [field_tag, slot_id LE, payload...] to the correct shard.
    #[inline]
    pub fn write_tuple(&mut self, slot: u32, field_tag: u8, payload: &[u8]) -> io::Result<()> {
        let shard_id = slot >> self.shard_shift;
        let w = self.writer_for(shard_id)?;
        w.write_all(&[field_tag])?;
        w.write_all(&slot.to_le_bytes())?;
        w.write_all(payload)?;
        self.tuples_written += 1;
        self.bytes_written += 5 + payload.len() as u64;
        Ok(())
    }

    /// Write an ImageScalars tuple.
    pub fn write_image_scalars(
        &mut self,
        slot: u32,
        nsfw_level: u8,
        user_id: u64,
        image_type: u8,
        sort_at: u64,
        flags: u8, // poi|minor|has_meta|on_site packed into bits 0-3
        post_id: u64,
        posted_to_id: u64,
        availability: u8,
        blocked_for: u8,
        published_at_ms: u64,
        url: Option<&[u8]>,
        hash: Option<&[u8]>,
    ) -> io::Result<()> {
        // Fixed part: 1 + 8 + 1 + 8 + 1 + 8 + 8 + 1 + 1 + 8 = 45 bytes
        // Variable: 1 + url_len + 1 + hash_len
        let url_bytes = url.unwrap_or(b"");
        let hash_bytes = hash.unwrap_or(b"");
        let url_len = url_bytes.len().min(255) as u8;
        let hash_len = hash_bytes.len().min(255) as u8;

        let payload_len = 45 + 1 + url_len as usize + 1 + hash_len as usize;
        let mut buf = Vec::with_capacity(payload_len);
        buf.push(nsfw_level);
        buf.extend_from_slice(&user_id.to_le_bytes());
        buf.push(image_type);
        buf.extend_from_slice(&sort_at.to_le_bytes());
        buf.push(flags);
        buf.extend_from_slice(&post_id.to_le_bytes());
        buf.extend_from_slice(&posted_to_id.to_le_bytes());
        buf.push(availability);
        buf.push(blocked_for);
        buf.extend_from_slice(&published_at_ms.to_le_bytes());
        buf.push(url_len);
        buf.extend_from_slice(&url_bytes[..url_len as usize]);
        buf.push(hash_len);
        buf.extend_from_slice(&hash_bytes[..hash_len as usize]);

        self.write_tuple(slot, TAG_IMAGE_SCALARS, &buf)
    }

    /// Write a tag ID tuple (9 bytes total: 1 tag + 4 slot + 4 value).
    #[inline]
    pub fn write_tag(&mut self, slot: u32, tag_id: u32) -> io::Result<()> {
        self.write_tuple(slot, TAG_TAG_ID, &tag_id.to_le_bytes())
    }

    /// Write a pre-assembled 9-byte tag tuple directly to the shard writer.
    /// Avoids the per-field write_all overhead of write_tuple.
    #[inline]
    pub fn write_tag_raw(&mut self, slot: u32, tag_id: u32) -> io::Result<()> {
        let shard_id = slot >> self.shard_shift;
        let w = self.writer_for(shard_id)?;
        let buf: [u8; 9] = [
            TAG_TAG_ID,
            (slot) as u8, (slot >> 8) as u8, (slot >> 16) as u8, (slot >> 24) as u8,
            (tag_id) as u8, (tag_id >> 8) as u8, (tag_id >> 16) as u8, (tag_id >> 24) as u8,
        ];
        w.write_all(&buf)?;
        self.tuples_written += 1;
        self.bytes_written += 9;
        Ok(())
    }

    /// Write a tool ID tuple.
    #[inline]
    pub fn write_tool(&mut self, slot: u32, tool_id: u32) -> io::Result<()> {
        self.write_tuple(slot, TAG_TOOL_ID, &tool_id.to_le_bytes())
    }

    /// Write a technique ID tuple.
    #[inline]
    pub fn write_technique(&mut self, slot: u32, technique_id: u32) -> io::Result<()> {
        self.write_tuple(slot, TAG_TECHNIQUE_ID, &technique_id.to_le_bytes())
    }

    /// Write a model version ID tuple (5 bytes: u32 mv_id + u8 detected).
    #[inline]
    pub fn write_model_version(&mut self, slot: u32, mv_id: u32, detected: bool) -> io::Result<()> {
        let mut payload = [0u8; 5];
        payload[..4].copy_from_slice(&mv_id.to_le_bytes());
        payload[4] = detected as u8;
        self.write_tuple(slot, TAG_MODEL_VERSION_ID, &payload)
    }

    /// Write a base model tuple (1 byte enum).
    #[inline]
    pub fn write_base_model(&mut self, slot: u32, base_model_enum: u8) -> io::Result<()> {
        self.write_tuple(slot, TAG_BASE_MODEL, &[base_model_enum])
    }

    /// Write a resource POI marker (0 bytes payload).
    #[inline]
    pub fn write_resource_poi(&mut self, slot: u32) -> io::Result<()> {
        self.write_tuple(slot, TAG_RESOURCE_POI, &[])
    }

    /// Flush all writers and report stats.
    pub fn flush_all(&mut self) -> io::Result<()> {
        for w in self.writers.values_mut() {
            w.flush()?;
        }
        Ok(())
    }

    /// Number of shard files created.
    pub fn shard_count(&self) -> usize {
        self.writers.len()
    }

    /// Total tuples written across all shards.
    pub fn tuples_written(&self) -> u64 {
        self.tuples_written
    }

    /// Total bytes written across all shards.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// List all shard file paths (sorted by shard ID).
    pub fn shard_paths(&self) -> Vec<PathBuf> {
        let mut ids: Vec<u32> = self.writers.keys().copied().collect();
        ids.sort();
        ids.into_iter()
            .map(|id| self.dir.join(format!("{:06x}.bin", id)))
            .collect()
    }

    /// The scratch directory path.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

impl Drop for ScratchWriter {
    fn drop(&mut self) {
        let _ = self.flush_all();
    }
}

// ---------------------------------------------------------------------------
// ScratchReader — read and parse a shard file
// ---------------------------------------------------------------------------

/// Parsed tuple from a scratch shard file.
#[derive(Debug)]
pub enum ScratchTuple {
    ImageScalars {
        slot: u32,
        nsfw_level: u8,
        user_id: u64,
        image_type: u8,
        sort_at: u64,
        flags: u8,
        post_id: u64,
        posted_to_id: u64,
        availability: u8,
        blocked_for: u8,
        published_at_ms: u64,
        url: Option<String>,
        hash: Option<String>,
    },
    TagId {
        slot: u32,
        tag_id: u32,
    },
    ToolId {
        slot: u32,
        tool_id: u32,
    },
    TechniqueId {
        slot: u32,
        technique_id: u32,
    },
    ModelVersionId {
        slot: u32,
        mv_id: u32,
        detected: bool,
    },
    BaseModel {
        slot: u32,
        base_model_enum: u8,
    },
    ResourcePoi {
        slot: u32,
    },
}

impl ScratchTuple {
    /// Get the slot ID for any tuple variant.
    pub fn slot(&self) -> u32 {
        match self {
            ScratchTuple::ImageScalars { slot, .. } => *slot,
            ScratchTuple::TagId { slot, .. } => *slot,
            ScratchTuple::ToolId { slot, .. } => *slot,
            ScratchTuple::TechniqueId { slot, .. } => *slot,
            ScratchTuple::ModelVersionId { slot, .. } => *slot,
            ScratchTuple::BaseModel { slot, .. } => *slot,
            ScratchTuple::ResourcePoi { slot } => *slot,
        }
    }
}

/// Assembled per-slot data from scratch tuples, ready for docstore encoding.
#[derive(Debug)]
pub struct SlotDoc {
    pub slot: u32,
    pub nsfw_level: u8,
    pub user_id: u64,
    pub image_type: u8,
    pub sort_at: u64,
    pub poi: bool,
    pub minor: bool,
    pub has_meta: bool,
    pub on_site: bool,
    pub post_id: u64,
    pub posted_to_id: u64,
    pub availability: u8,
    pub blocked_for: u8,
    pub published_at_ms: u64,
    pub url: Option<String>,
    pub hash: Option<String>,
    pub tag_ids: Vec<u32>,
    pub tool_ids: Vec<u32>,
    pub technique_ids: Vec<u32>,
    pub model_version_ids: Vec<u32>,
    pub base_model: u8,
    pub resource_poi: bool,
}

/// Read a shard file, parse all tuples, sort by slot, and group into SlotDocs.
pub fn read_shard(path: &Path) -> io::Result<Vec<SlotDoc>> {
    let data = fs::read(path)?;
    let tuples = parse_tuples(&data);

    // Group tuples by slot
    let mut slot_map: HashMap<u32, Vec<ScratchTuple>> = HashMap::new();
    for t in tuples {
        let s = t.slot();
        slot_map.entry(s).or_default().push(t);
    }

    // Build SlotDocs sorted by slot
    let mut slots: Vec<u32> = slot_map.keys().copied().collect();
    slots.sort_unstable();

    let mut docs = Vec::with_capacity(slots.len());
    for slot in slots {
        let tuples = slot_map.remove(&slot).unwrap();
        docs.push(assemble_slot_doc(slot, tuples));
    }

    Ok(docs)
}

/// Parse raw bytes into ScratchTuples.
fn parse_tuples(data: &[u8]) -> Vec<ScratchTuple> {
    let mut tuples = Vec::new();
    let mut pos = 0;
    let len = data.len();

    while pos < len {
        if pos + 5 > len {
            break; // need at least tag + slot
        }
        let tag = data[pos];
        let slot = u32::from_le_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]);
        pos += 5;

        match tag {
            TAG_IMAGE_SCALARS => {
                // Fixed part: 45 bytes
                if pos + 45 > len {
                    break;
                }
                let nsfw_level = data[pos];
                let user_id = u64::from_le_bytes(data[pos + 1..pos + 9].try_into().unwrap());
                let image_type = data[pos + 9];
                let sort_at = u64::from_le_bytes(data[pos + 10..pos + 18].try_into().unwrap());
                let flags = data[pos + 18];
                let post_id = u64::from_le_bytes(data[pos + 19..pos + 27].try_into().unwrap());
                let posted_to_id = u64::from_le_bytes(data[pos + 27..pos + 35].try_into().unwrap());
                let availability = data[pos + 35];
                let blocked_for = data[pos + 36];
                let published_at_ms = u64::from_le_bytes(data[pos + 37..pos + 45].try_into().unwrap());
                pos += 45;

                // Variable: url
                if pos >= len {
                    break;
                }
                let url_len = data[pos] as usize;
                pos += 1;
                if pos + url_len > len {
                    break;
                }
                let url = if url_len > 0 {
                    Some(String::from_utf8_lossy(&data[pos..pos + url_len]).into_owned())
                } else {
                    None
                };
                pos += url_len;

                // Variable: hash
                if pos >= len {
                    break;
                }
                let hash_len = data[pos] as usize;
                pos += 1;
                if pos + hash_len > len {
                    break;
                }
                let hash = if hash_len > 0 {
                    Some(String::from_utf8_lossy(&data[pos..pos + hash_len]).into_owned())
                } else {
                    None
                };
                pos += hash_len;

                tuples.push(ScratchTuple::ImageScalars {
                    slot,
                    nsfw_level,
                    user_id,
                    image_type,
                    sort_at,
                    flags,
                    post_id,
                    posted_to_id,
                    availability,
                    blocked_for,
                    published_at_ms,
                    url,
                    hash,
                });
            }
            TAG_TAG_ID => {
                if pos + 4 > len {
                    break;
                }
                let tag_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                tuples.push(ScratchTuple::TagId { slot, tag_id });
            }
            TAG_TOOL_ID => {
                if pos + 4 > len {
                    break;
                }
                let tool_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                tuples.push(ScratchTuple::ToolId { slot, tool_id });
            }
            TAG_TECHNIQUE_ID => {
                if pos + 4 > len {
                    break;
                }
                let technique_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                pos += 4;
                tuples.push(ScratchTuple::TechniqueId { slot, technique_id });
            }
            TAG_MODEL_VERSION_ID => {
                if pos + 5 > len {
                    break;
                }
                let mv_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                let detected = data[pos + 4] != 0;
                pos += 5;
                tuples.push(ScratchTuple::ModelVersionId { slot, mv_id, detected });
            }
            TAG_BASE_MODEL => {
                if pos + 1 > len {
                    break;
                }
                let base_model_enum = data[pos];
                pos += 1;
                tuples.push(ScratchTuple::BaseModel { slot, base_model_enum });
            }
            TAG_RESOURCE_POI => {
                tuples.push(ScratchTuple::ResourcePoi { slot });
            }
            _ => {
                // Unknown tag — can't recover position, stop parsing
                break;
            }
        }
    }

    tuples
}

/// Assemble a SlotDoc from a group of tuples belonging to the same slot.
fn assemble_slot_doc(slot: u32, tuples: Vec<ScratchTuple>) -> SlotDoc {
    let mut doc = SlotDoc {
        slot,
        nsfw_level: 0,
        user_id: 0,
        image_type: 0,
        sort_at: 0,
        poi: false,
        minor: false,
        has_meta: false,
        on_site: false,
        post_id: 0,
        posted_to_id: 0,
        availability: 0,
        blocked_for: 0,
        published_at_ms: 0,
        url: None,
        hash: None,
        tag_ids: Vec::new(),
        tool_ids: Vec::new(),
        technique_ids: Vec::new(),
        model_version_ids: Vec::new(),
        base_model: 0,
        resource_poi: false,
    };

    for t in tuples {
        match t {
            ScratchTuple::ImageScalars {
                nsfw_level,
                user_id,
                image_type,
                sort_at,
                flags,
                post_id,
                posted_to_id,
                availability,
                blocked_for,
                published_at_ms,
                url,
                hash,
                ..
            } => {
                doc.nsfw_level = nsfw_level;
                doc.user_id = user_id;
                doc.image_type = image_type;
                doc.sort_at = sort_at;
                doc.poi = (flags & 0x01) != 0;
                doc.minor = (flags & 0x02) != 0;
                doc.has_meta = (flags & 0x04) != 0;
                doc.on_site = (flags & 0x08) != 0;
                doc.post_id = post_id;
                doc.posted_to_id = posted_to_id;
                doc.availability = availability;
                doc.blocked_for = blocked_for;
                doc.published_at_ms = published_at_ms;
                doc.url = url;
                doc.hash = hash;
            }
            ScratchTuple::TagId { tag_id, .. } => {
                doc.tag_ids.push(tag_id);
            }
            ScratchTuple::ToolId { tool_id, .. } => {
                doc.tool_ids.push(tool_id);
            }
            ScratchTuple::TechniqueId { technique_id, .. } => {
                doc.technique_ids.push(technique_id);
            }
            ScratchTuple::ModelVersionId { mv_id, .. } => {
                doc.model_version_ids.push(mv_id);
            }
            ScratchTuple::BaseModel { base_model_enum, .. } => {
                doc.base_model = base_model_enum;
            }
            ScratchTuple::ResourcePoi { .. } => {
                doc.resource_poi = true;
            }
        }
    }

    // OR resource_poi into poi
    if doc.resource_poi {
        doc.poi = true;
    }

    doc
}

/// Convert a SlotDoc to a serde_json::Value for docstore encoding.
/// `metrics` is (reactionCount, commentCount, collectedCount) from ClickHouse.
pub fn slot_doc_to_json(doc: &SlotDoc, metrics: Option<(u32, u32, u32)>) -> serde_json::Value {
    use super::slot_arena::{decode_availability, decode_base_model, decode_image_type};

    let mut json = serde_json::json!({
        "id": doc.slot as i64,
        "nsfwLevel": doc.nsfw_level as i64,
        "userId": doc.user_id as i64,
        "postId": doc.post_id as i64,
        "postedToId": doc.posted_to_id as i64,
        "type": decode_image_type(doc.image_type),
        "baseModel": decode_base_model(doc.base_model),
        "availability": decode_availability(doc.availability),
        "tagIds": doc.tag_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "modelVersionIds": doc.model_version_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "modelVersionIdsManual": serde_json::json!([]),
        "toolIds": doc.tool_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "techniqueIds": doc.technique_ids.iter().map(|&t| t as i64).collect::<Vec<i64>>(),
        "reactionCount": metrics.map_or(0i64, |m| m.0 as i64),
        "commentCount": metrics.map_or(0i64, |m| m.1 as i64),
        "collectedCount": metrics.map_or(0i64, |m| m.2 as i64),
        "sortAt": doc.sort_at as i64,
        "sortAtUnix": doc.sort_at as i64 * 1000,
        "publishedAtUnix": doc.published_at_ms as i64,
        "existedAtUnix": 0i64,
    });

    if let Some(obj) = json.as_object_mut() {
        if doc.has_meta {
            obj.insert("hasMeta".into(), serde_json::json!(true));
        }
        if doc.on_site {
            obj.insert("onSite".into(), serde_json::json!(true));
        }
        if doc.poi {
            obj.insert("poi".into(), serde_json::json!(true));
        }
        if doc.minor {
            obj.insert("minor".into(), serde_json::json!(true));
        }
        if let Some(ref url) = doc.url {
            obj.insert("url".into(), serde_json::json!(url));
        }
        if let Some(ref hash) = doc.hash {
            obj.insert("hash".into(), serde_json::json!(hash));
        }
        if doc.blocked_for > 0 {
            obj.insert("blockedFor".into(), serde_json::json!("blocked"));
        }
    }

    json
}

/// List all .bin shard files in a directory, sorted by name.
pub fn list_shard_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("bin"))
        .collect();
    files.sort();
    Ok(files)
}

// ---------------------------------------------------------------------------
// Fast single-pass shard processing — no intermediate ScratchTuple allocation
// ---------------------------------------------------------------------------

/// Read a shard file and parse directly into SlotDocs in a single pass.
/// No intermediate Vec<ScratchTuple>, no HashMap grouping.
/// Uses a HashMap<u32, SlotDoc> addressed by slot ID — each tuple updates
/// the SlotDoc in-place as it's encountered.
pub fn read_shard_fast(path: &Path) -> io::Result<Vec<SlotDoc>> {
    let data = fs::read(path)?;
    let mut docs: HashMap<u32, SlotDoc> = HashMap::new();

    let mut pos = 0;
    let len = data.len();

    while pos < len {
        if pos + 5 > len { break; }
        let tag = data[pos];
        let slot = u32::from_le_bytes([data[pos+1], data[pos+2], data[pos+3], data[pos+4]]);
        pos += 5;

        // Get or create the SlotDoc for this slot
        let doc = docs.entry(slot).or_insert_with(|| SlotDoc {
            slot, nsfw_level: 0, user_id: 0, image_type: 0, sort_at: 0,
            poi: false, minor: false, has_meta: false, on_site: false,
            post_id: 0, posted_to_id: 0, availability: 0, blocked_for: 0,
            published_at_ms: 0, url: None, hash: None,
            tag_ids: Vec::new(), tool_ids: Vec::new(),
            technique_ids: Vec::new(), model_version_ids: Vec::new(),
            base_model: 0, resource_poi: false,
        });

        match tag {
            TAG_IMAGE_SCALARS => {
                if pos + 45 > len { break; }
                doc.nsfw_level = data[pos];
                doc.user_id = u64::from_le_bytes(data[pos+1..pos+9].try_into().unwrap());
                doc.image_type = data[pos+9];
                doc.sort_at = u64::from_le_bytes(data[pos+10..pos+18].try_into().unwrap());
                let flags = data[pos+18];
                doc.poi = (flags & 0x01) != 0;
                doc.minor = (flags & 0x02) != 0;
                doc.has_meta = (flags & 0x04) != 0;
                doc.on_site = (flags & 0x08) != 0;
                doc.post_id = u64::from_le_bytes(data[pos+19..pos+27].try_into().unwrap());
                doc.posted_to_id = u64::from_le_bytes(data[pos+27..pos+35].try_into().unwrap());
                doc.availability = data[pos+35];
                doc.blocked_for = data[pos+36];
                doc.published_at_ms = u64::from_le_bytes(data[pos+37..pos+45].try_into().unwrap());
                pos += 45;
                // url
                if pos >= len { break; }
                let url_len = data[pos] as usize; pos += 1;
                if pos + url_len > len { break; }
                if url_len > 0 {
                    doc.url = Some(String::from_utf8_lossy(&data[pos..pos+url_len]).into_owned());
                }
                pos += url_len;
                // hash
                if pos >= len { break; }
                let hash_len = data[pos] as usize; pos += 1;
                if pos + hash_len > len { break; }
                if hash_len > 0 {
                    doc.hash = Some(String::from_utf8_lossy(&data[pos..pos+hash_len]).into_owned());
                }
                pos += hash_len;
            }
            TAG_TAG_ID => {
                if pos + 4 > len { break; }
                let v = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                pos += 4;
                doc.tag_ids.push(v);
            }
            TAG_TOOL_ID => {
                if pos + 4 > len { break; }
                let v = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                pos += 4;
                doc.tool_ids.push(v);
            }
            TAG_TECHNIQUE_ID => {
                if pos + 4 > len { break; }
                let v = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                pos += 4;
                doc.technique_ids.push(v);
            }
            TAG_MODEL_VERSION_ID => {
                if pos + 5 > len { break; }
                let mv_id = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap());
                // skip detected byte
                pos += 5;
                doc.model_version_ids.push(mv_id);
            }
            TAG_BASE_MODEL => {
                if pos + 1 > len { break; }
                doc.base_model = data[pos];
                pos += 1;
            }
            TAG_RESOURCE_POI => {
                doc.resource_poi = true;
                doc.poi = true;
            }
            _ => break,
        }
    }

    // Collect and sort by slot
    let mut result: Vec<SlotDoc> = docs.into_values().collect();
    result.sort_unstable_by_key(|d| d.slot);
    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let scratch_dir = dir.path().join("scratch");
        let mut writer = ScratchWriter::new(&scratch_dir, DEFAULT_SHARD_SHIFT).unwrap();

        // Write a few tuples for slot 100
        writer.write_image_scalars(
            100, 8, 9999, 0, 1700000000,
            0x05, // poi=1, minor=0, has_meta=1, on_site=0
            777, 888, 0, 0, 1700000000000,
            Some(b"https://example.com/img.jpg"),
            Some(b"abc123hash"),
        ).unwrap();
        writer.write_tag(100, 42).unwrap();
        writer.write_tag(100, 99).unwrap();
        writer.write_tool(100, 7).unwrap();
        writer.write_model_version(100, 555, true).unwrap();
        writer.write_base_model(100, 3).unwrap(); // SDXL 1.0
        writer.write_resource_poi(100).unwrap();

        // Write a second slot
        writer.write_image_scalars(
            200, 16, 1234, 1, 1700000001,
            0x00, 0, 0, 1, 0, 0, None, None,
        ).unwrap();

        assert_eq!(writer.tuples_written(), 8);
        writer.flush_all().unwrap();

        // Read back
        let shard_files = list_shard_files(&scratch_dir).unwrap();
        assert_eq!(shard_files.len(), 1); // both slots in same shard (100>>16 == 200>>16 == 0)

        let docs = read_shard(&shard_files[0]).unwrap();
        assert_eq!(docs.len(), 2);

        let doc100 = &docs[0];
        assert_eq!(doc100.slot, 100);
        assert_eq!(doc100.nsfw_level, 8);
        assert_eq!(doc100.user_id, 9999);
        assert_eq!(doc100.sort_at, 1700000000);
        assert!(doc100.poi); // flags bit 0 + resource_poi
        assert!(doc100.has_meta); // flags bit 2
        assert_eq!(doc100.url.as_deref(), Some("https://example.com/img.jpg"));
        assert_eq!(doc100.hash.as_deref(), Some("abc123hash"));
        assert_eq!(doc100.tag_ids, vec![42, 99]);
        assert_eq!(doc100.tool_ids, vec![7]);
        assert_eq!(doc100.model_version_ids, vec![555]);
        assert_eq!(doc100.base_model, 3);
        assert!(doc100.resource_poi);

        let doc200 = &docs[1];
        assert_eq!(doc200.slot, 200);
        assert_eq!(doc200.nsfw_level, 16);
        assert_eq!(doc200.image_type, 1);
        assert_eq!(doc200.availability, 1);
        assert!(doc200.url.is_none());
    }

    #[test]
    fn test_multiple_shards() {
        let dir = tempdir().unwrap();
        let scratch_dir = dir.path().join("scratch");
        // Shift of 4 = 16 slots per shard, for easier testing
        let mut writer = ScratchWriter::new(&scratch_dir, 4).unwrap();

        writer.write_tag(5, 100).unwrap();   // shard 0
        writer.write_tag(20, 200).unwrap();  // shard 1
        writer.write_tag(40, 300).unwrap();  // shard 2
        writer.flush_all().unwrap();

        assert_eq!(writer.shard_count(), 3);
        let files = list_shard_files(&scratch_dir).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn test_slot_doc_to_json() {
        let doc = SlotDoc {
            slot: 42,
            nsfw_level: 8,
            user_id: 9999,
            image_type: 0,
            sort_at: 1700000000,
            poi: true,
            minor: false,
            has_meta: true,
            on_site: false,
            post_id: 777,
            posted_to_id: 888,
            availability: 0,
            blocked_for: 0,
            published_at_ms: 1700000000000,
            url: Some("https://example.com/img.jpg".to_string()),
            hash: Some("abc123".to_string()),
            tag_ids: vec![100, 200],
            tool_ids: vec![7],
            technique_ids: vec![],
            model_version_ids: vec![555],
            base_model: 3,
            resource_poi: false,
        };

        let json = slot_doc_to_json(&doc, None);
        let obj = json.as_object().unwrap();
        assert_eq!(obj["id"], 42);
        assert_eq!(obj["nsfwLevel"], 8);
        assert_eq!(obj["userId"], 9999);
        assert_eq!(obj["poi"], true);
        assert_eq!(obj["hasMeta"], true);
        assert_eq!(obj["url"], "https://example.com/img.jpg");
        let tags: Vec<i64> = obj["tagIds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect();
        assert_eq!(tags, vec![100, 200]);
    }

    #[test]
    fn test_slot_doc_to_json_with_metrics() {
        let doc = SlotDoc {
            slot: 42,
            nsfw_level: 1,
            user_id: 100,
            image_type: 0,
            sort_at: 1700000000,
            poi: false,
            minor: false,
            has_meta: false,
            on_site: false,
            post_id: 1,
            posted_to_id: 0,
            availability: 0,
            blocked_for: 0,
            published_at_ms: 1700000000000,
            url: None,
            hash: None,
            tag_ids: vec![],
            tool_ids: vec![],
            technique_ids: vec![],
            model_version_ids: vec![],
            base_model: 0,
            resource_poi: false,
        };

        // Without metrics
        let json_no_metrics = slot_doc_to_json(&doc, None);
        assert_eq!(json_no_metrics["reactionCount"], 0);
        assert_eq!(json_no_metrics["commentCount"], 0);
        assert_eq!(json_no_metrics["collectedCount"], 0);

        // With metrics
        let json_with_metrics = slot_doc_to_json(&doc, Some((500, 10, 5)));
        assert_eq!(json_with_metrics["reactionCount"], 500);
        assert_eq!(json_with_metrics["commentCount"], 10);
        assert_eq!(json_with_metrics["collectedCount"], 5);
    }
}
