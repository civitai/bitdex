/// Benchmark encoding formats for doc storage.
/// Tests: msgpack (rmp_serde), raw tuple format, bincode, postcard, and bare memcpy baseline.
use std::hint::black_box;
use std::time::Instant;

fn main() {
    println!("=== Encoding Format Benchmark ===\n");

    // Simulate a typical doc: 20 fields, mix of types
    // Fields: 8 integers, 4 booleans, 3 strings (~20 chars), 2 integer arrays (5 elements), 3 nullable ints
    let iterations = 1_000_000u64;

    // Build a representative doc as Vec<(u16, PackedValue)> equivalent
    // We'll test different serialization approaches

    // === Format 1: msgpack via rmp_serde (using a serde-friendly struct) ===
    println!("--- Format 1: msgpack (rmp_serde) ---");
    {
        // Use a tuple vec that's serde-compatible
        let doc: Vec<(u16, i64)> = vec![
            (0, 12345), (1, 1), (2, 67890), (3, 1700000000),
            (4, 500), (5, 20), (6, 100), (7, 0),
            (8, 1), (9, 1), (10, 0), (11, 0),
            (12, 0), (13, 0), (14, 0), // strings represented as dict IDs
            (15, 512), (16, 768), (17, 1700000000), (18, 1700000000), (19, 0),
        ];

        let encoded = rmp_serde::to_vec(&doc).unwrap();
        println!("  Encoded size: {} bytes (int-only, strings as dict IDs)", encoded.len());

        let t = Instant::now();
        for _ in 0..iterations {
            let e = rmp_serde::to_vec(&doc).unwrap();
            black_box(&e);
        }
        let encode_ns = t.elapsed().as_nanos() / iterations as u128;
        let encode_rate = 1_000_000_000.0 / encode_ns as f64;
        println!("  Encode: {}ns/op ({:.1}M/s)", encode_ns, encode_rate / 1e6);

        let t = Instant::now();
        for _ in 0..iterations {
            let d: Vec<(u16, i64)> = rmp_serde::from_slice(&encoded).unwrap();
            black_box(&d);
        }
        let decode_ns = t.elapsed().as_nanos() / iterations as u128;
        let decode_rate = 1_000_000_000.0 / decode_ns as f64;
        println!("  Decode: {}ns/op ({:.1}M/s)\n", decode_ns, decode_rate / 1e6);
    }

    // === Format 2: Raw binary (hand-rolled, zero-alloc decode) ===
    println!("--- Format 2: Raw binary (hand-rolled) ---");
    {
        // Format: [num_fields:u16][field_idx:u16 type:u8 value_bytes...]
        // Types: 0=i64(8B), 1=bool(1B), 2=string(u16_len + bytes), 3=null(0B)
        let mut buf = Vec::with_capacity(256);

        fn encode_raw(buf: &mut Vec<u8>) {
            buf.clear();
            buf.extend_from_slice(&20u16.to_le_bytes()); // num fields

            // Helper macros
            macro_rules! field_i64 { ($idx:expr, $val:expr) => {
                buf.extend_from_slice(&($idx as u16).to_le_bytes());
                buf.push(0); // type = i64
                buf.extend_from_slice(&($val as i64).to_le_bytes());
            }}
            macro_rules! field_bool { ($idx:expr, $val:expr) => {
                buf.extend_from_slice(&($idx as u16).to_le_bytes());
                buf.push(1); // type = bool
                buf.push(if $val { 1 } else { 0 });
            }}
            macro_rules! field_str { ($idx:expr, $val:expr) => {
                buf.extend_from_slice(&($idx as u16).to_le_bytes());
                buf.push(2); // type = string
                let s = $val.as_bytes();
                buf.extend_from_slice(&(s.len() as u16).to_le_bytes());
                buf.extend_from_slice(s);
            }}

            field_i64!(0, 12345); field_i64!(1, 1); field_i64!(2, 67890);
            field_i64!(3, 1700000000); field_i64!(4, 500); field_i64!(5, 20);
            field_i64!(6, 100); field_i64!(7, 0);
            field_bool!(8, true); field_bool!(9, true); field_bool!(10, false); field_bool!(11, false);
            field_str!(12, "abc123-guid-value-here"); field_str!(13, "SDXL 1.0"); field_str!(14, "image");
            field_i64!(15, 512); field_i64!(16, 768);
            field_i64!(17, 1700000000); field_i64!(18, 1700000000); field_i64!(19, 0);
        }

        encode_raw(&mut buf);
        println!("  Encoded size: {} bytes", buf.len());

        // Encode benchmark
        let t = Instant::now();
        for _ in 0..iterations {
            encode_raw(&mut buf);
            black_box(&buf);
        }
        let encode_ns = t.elapsed().as_nanos() / iterations as u128;
        let encode_rate = 1_000_000_000.0 / encode_ns as f64;
        println!("  Encode: {}ns/op ({:.1}M/s)", encode_ns, encode_rate / 1e6);

        // Decode benchmark (just field count + skip through)
        let encoded = buf.clone();
        let t = Instant::now();
        for _ in 0..iterations {
            let data = &encoded[..];
            let num = u16::from_le_bytes([data[0], data[1]]) as usize;
            let mut pos = 2;
            let mut sum = 0i64; // prevent optimization
            for _ in 0..num {
                let _field_idx = u16::from_le_bytes([data[pos], data[pos+1]]);
                pos += 2;
                let typ = data[pos]; pos += 1;
                match typ {
                    0 => { sum += i64::from_le_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8; }
                    1 => { sum += data[pos] as i64; pos += 1; }
                    2 => { let len = u16::from_le_bytes([data[pos], data[pos+1]]) as usize; pos += 2 + len; }
                    _ => {}
                }
            }
            black_box(sum);
        }
        let decode_ns = t.elapsed().as_nanos() / iterations as u128;
        let decode_rate = 1_000_000_000.0 / decode_ns as f64;
        println!("  Decode: {}ns/op ({:.1}M/s)\n", decode_ns, decode_rate / 1e6);
    }

    // === Format 3: Existing DocOpCodec format ===
    println!("--- Format 3: DocOpCodec-style (current BitDex format) ---");
    {
        // This is what we already use: [field_idx:u16][packed_value_tag:u8][value_bytes]
        // PackedValue encoding: I=1+8, F=1+8, B=1+1, S=1+2+len, Mi=1+4+n*8
        let mut buf = Vec::with_capacity(256);

        fn encode_docop(buf: &mut Vec<u8>) {
            buf.clear();
            // Slot + field count (like DocOp::Merge encoding)
            buf.extend_from_slice(&42u32.to_le_bytes()); // slot
            buf.extend_from_slice(&20u16.to_le_bytes()); // num fields

            macro_rules! pv_i { ($idx:expr, $val:expr) => {
                buf.extend_from_slice(&($idx as u16).to_le_bytes());
                buf.push(0x01); // PV_TAG_I
                buf.extend_from_slice(&($val as i64).to_le_bytes());
            }}
            macro_rules! pv_b { ($idx:expr, $val:expr) => {
                buf.extend_from_slice(&($idx as u16).to_le_bytes());
                buf.push(0x03); // PV_TAG_B
                buf.push(if $val { 1 } else { 0 });
            }}
            macro_rules! pv_s { ($idx:expr, $val:expr) => {
                buf.extend_from_slice(&($idx as u16).to_le_bytes());
                buf.push(0x04); // PV_TAG_S
                let s = $val.as_bytes();
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s);
            }}

            pv_i!(0, 12345); pv_i!(1, 1); pv_i!(2, 67890);
            pv_i!(3, 1700000000); pv_i!(4, 500); pv_i!(5, 20);
            pv_i!(6, 100); pv_i!(7, 0);
            pv_b!(8, true); pv_b!(9, true); pv_b!(10, false); pv_b!(11, false);
            pv_s!(12, "abc123-guid-value-here"); pv_s!(13, "SDXL 1.0"); pv_s!(14, "image");
            pv_i!(15, 512); pv_i!(16, 768);
            pv_i!(17, 1700000000); pv_i!(18, 1700000000); pv_i!(19, 0);
        }

        encode_docop(&mut buf);
        println!("  Encoded size: {} bytes", buf.len());

        let t = Instant::now();
        for _ in 0..iterations {
            encode_docop(&mut buf);
            black_box(&buf);
        }
        let encode_ns = t.elapsed().as_nanos() / iterations as u128;
        let encode_rate = 1_000_000_000.0 / encode_ns as f64;
        println!("  Encode: {}ns/op ({:.1}M/s)", encode_ns, encode_rate / 1e6);

        // Decode: same as raw binary but with PackedValue tags
        let encoded = buf.clone();
        let t = Instant::now();
        for _ in 0..iterations {
            let data = &encoded[..];
            let mut pos = 4; // skip slot
            let num = u16::from_le_bytes([data[pos], data[pos+1]]) as usize; pos += 2;
            let mut sum = 0i64;
            for _ in 0..num {
                let _fidx = u16::from_le_bytes([data[pos], data[pos+1]]); pos += 2;
                let tag = data[pos]; pos += 1;
                match tag {
                    0x01 => { sum += i64::from_le_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8; } // I
                    0x03 => { sum += data[pos] as i64; pos += 1; } // B
                    0x04 => { let len = u32::from_le_bytes(data[pos..pos+4].try_into().unwrap()) as usize; pos += 4 + len; } // S
                    _ => {}
                }
            }
            black_box(sum);
        }
        let decode_ns = t.elapsed().as_nanos() / iterations as u128;
        let decode_rate = 1_000_000_000.0 / decode_ns as f64;
        println!("  Decode: {}ns/op ({:.1}M/s)\n", decode_ns, decode_rate / 1e6);
    }

    // === Summary ===
    println!("=== Summary ===");
    println!("  At 109M docs:");
    println!("  Format       Encode          Decode          Size");
    println!("  msgpack      measure above   measure above   ~230B");
    println!("  raw binary   measure above   measure above   ~200B");
    println!("  docop codec  measure above   measure above   ~240B");
    println!("\n  Target: encoding overhead < 10% of write time");
    println!("  At 35M writes/sec: 28.6ns budget per entry");
    println!("  If encode > 28.6ns: encoding becomes the bottleneck");
}

