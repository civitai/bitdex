use roaring::RoaringBitmap;

fn main() {
    let mut bm = RoaringBitmap::new();
    bm.insert_range(0..50);

    let frozen_size = bm.frozen_serialized_size();
    println!("Frozen size: {}", frozen_size);

    let mut buf = vec![0u8; frozen_size];
    let written = bm.serialize_frozen_into(&mut buf).unwrap();
    println!("Written: {} bytes", written);
    println!("Last 8 bytes: {:02x?}", &buf[buf.len().saturating_sub(8)..]);

    // Try view
    match roaring::FrozenRoaringBitmap::view(&buf) {
        Ok(frozen) => {
            println!("View OK: {} entries", frozen.len());
            let owned = frozen.to_owned();
            println!("To owned: {} entries", owned.len());
        }
        Err(e) => println!("View FAILED: {e:?}"),
    }

    // Now test: write to DataSilo, compact, read back, view
    let dir = tempfile::tempdir().unwrap();
    let mut silo = datasilo::DataSilo::open(dir.path(), datasilo::SiloConfig {
        alignment: 32,
        buffer_ratio: 1.2,
        min_entry_size: 64,
    }).unwrap();

    silo.append_op(0, &buf).unwrap();
    silo.compact().unwrap();

    match silo.get(0) {
        Some(loaded) => {
            println!("\nLoaded from silo: {} bytes", loaded.len());
            println!("Pointer aligned: {}", loaded.as_ptr() as usize % 32 == 0);
            println!("Bytes match: {}", buf[..written] == *loaded);
            println!("Loaded last 8: {:02x?}", &loaded[loaded.len().saturating_sub(8)..]);

            match roaring::FrozenRoaringBitmap::view(loaded) {
                Ok(frozen) => println!("View from silo OK: {} entries", frozen.len()),
                Err(e) => println!("View from silo FAILED: {e:?}"),
            }
        }
        None => println!("ERROR: get(0) returned None"),
    }
}
