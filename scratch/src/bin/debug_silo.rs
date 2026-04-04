use datasilo::{DataSilo, SiloConfig};
use roaring::RoaringBitmap;

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut silo = DataSilo::open(dir.path(), SiloConfig::default()).unwrap();

    let mut bm = RoaringBitmap::new();
    bm.insert_range(0..50);

    let mut original = Vec::new();
    bm.serialize_into(&mut original).unwrap();
    println!("Original size: {} bytes", original.len());
    println!("Original first 16: {:02x?}", &original[..16.min(original.len())]);
    println!("Original last 8: {:02x?}", &original[original.len().saturating_sub(8)..]);

    silo.append_op(0, &original).unwrap();
    println!("\nBefore compact - ops size: {}", silo.ops_size());

    silo.compact().unwrap();
    println!("After compact - data bytes: {}", silo.data_bytes());

    match silo.get(0) {
        Some(loaded) => {
            println!("\nLoaded size: {} bytes", loaded.len());
            println!("Loaded first 16: {:02x?}", &loaded[..16.min(loaded.len())]);
            println!("Loaded last 8: {:02x?}", &loaded[loaded.len().saturating_sub(8)..]);
            println!("Bytes match: {}", original.as_slice() == loaded);

            if original.as_slice() != loaded {
                // Find first difference
                for (i, (a, b)) in original.iter().zip(loaded.iter()).enumerate() {
                    if a != b {
                        println!("First diff at byte {}: original={:02x} loaded={:02x}", i, a, b);
                        break;
                    }
                }
                if original.len() != loaded.len() {
                    println!("Length diff: original={} loaded={}", original.len(), loaded.len());
                }
            }

            match RoaringBitmap::deserialize_from(loaded) {
                Ok(bm2) => println!("Deserialized OK: {} entries", bm2.len()),
                Err(e) => println!("Deserialize FAILED: {e}"),
            }
        }
        None => println!("ERROR: get(0) returned None!"),
    }
}
