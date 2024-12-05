//! All fields are made public for the purposes of simplicity.
use std::{collections::HashMap, sync::Arc};

use blake3::{Hash, Hasher};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use tokio::sync::{Mutex, RwLock};

const AVAILABLE_DATES: [NaiveDate; 30] = [
    NaiveDate::from_ymd_opt(2024, 12, 1).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 2).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 3).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 4).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 5).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 6).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 7).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 8).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 9).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 10).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 11).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 12).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 13).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 14).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 15).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 16).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 17).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 18).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 19).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 20).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 21).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 22).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 23).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 24).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 25).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 26).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 27).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 28).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 29).unwrap(),
    NaiveDate::from_ymd_opt(2024, 12, 30).unwrap(),
];

const AVAILABLE_TIMES: [NaiveTime; 6] = [
    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    NaiveTime::from_hms_opt(4, 0, 0).unwrap(),
    NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
    NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
    NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
    NaiveTime::from_hms_opt(20, 0, 0).unwrap(),
];

fn gen_date_distribution() -> Vec<NaiveDate> {
    use rand::seq::SliceRandom;
    let mut dates: Vec<NaiveDate> = AVAILABLE_DATES
        .choose_multiple(&mut rand::thread_rng(), 20)
        .cloned()
        .collect();
    dates.sort();
    dates
}

fn gen_image_times(date: NaiveDate) -> Vec<NaiveDateTime> {
    use rand::seq::SliceRandom;
    let mut times: Vec<NaiveDateTime> = AVAILABLE_TIMES
        .choose_multiple(&mut rand::thread_rng(), 3)
        .cloned()
        .map(|t| NaiveDateTime::new(date, t))
        .collect();
    times.sort();
    times
}

struct Image {
    /// the image itself, for the purposes of test task is just random bytes
    pub data: Vec<u8>,
    /// the actual date & time value we care about for ordering
    pub created_at: NaiveDateTime,
}

impl std::fmt::Debug for Image {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|{}| {:x?}...", self.created_at, &self.data[..5])
    }
}

impl Image {
    fn random(created_at: NaiveDateTime) -> Self {
        let rand_slice: [u8; 32] = rand::random();
        Self {
            created_at,
            data: rand_slice.to_vec(),
        }
    }

    fn hash(&self) -> Hash {
        #[cfg(debug_assertions)]
        tracing::trace!(img_name = %self.created_at, "Hashing self");

        Hasher::new().update_rayon(&self.data).finalize()
    }
}

#[derive(Debug)]
struct Directory {
    /// actual value we care about for the sync
    date: NaiveDate,
    /// images inside of the directory, assuming sorted by creation
    images: Vec<Image>,
}

/// This struct holds the metadata of the directory.
/// Primary sync primitive.
#[derive(Debug, Clone, Copy)]
struct DirectoryMetadata {
    dir_hash: Hash,
    fs_index: usize,
}

impl Directory {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();
        for image in self.images.iter() {
            hasher.update_rayon(&image.data);
        }

        hasher.finalize()
    }
}

#[derive(Debug)]
struct NaiveFs {
    /// Root hash of the filesystem. Once all the root hashes
    /// of the peers become equal, we know the sync is finished.
    root_hash: Arc<RwLock<Hash>>,
    /// Assuming that we only have directories in the fs
    /// The obv choice for such task is an MPT, but i want to rely
    /// on libraries as least as possible.
    ///
    /// the RwLock is used for easy concurrent accesses
    dir_metadatas: Arc<RwLock<HashMap<NaiveDate, DirectoryMetadata>>>,
    dirs: Arc<Mutex<Vec<Directory>>>,
}

impl NaiveFs {
    fn random() -> Self {
        let available_dates = gen_date_distribution();
        let mut root_hasher = Hasher::new();
        let dirs: Vec<(NaiveDate, Directory, DirectoryMetadata)> = available_dates
            .into_iter()
            .enumerate()
            .map(|(idx, d)| {
                let dir = Directory {
                    date: d,
                    images: gen_image_times(d)
                        .into_iter()
                        .map(|t| Image::random(t))
                        .collect(),
                };
                let dir_hash = dir.hash();
                root_hasher.update_rayon(dir_hash.as_bytes());
                let dir_metadata = DirectoryMetadata {
                    dir_hash,
                    fs_index: idx,
                };
                (d, dir, dir_metadata)
            }) // return both the date and the dir to build
            // the hashmap
            .collect();
        let directory_metadatas = 
                dirs.iter().map(|(date, _, meta)| (*date, *meta)).collect::<Vec<(NaiveDate, DirectoryMetadata)>>();
        let dirs = dirs.into_iter().map(|(_, dir, _)| dir).collect();

        Self {
            root_hash: Arc::new(RwLock::new(root_hasher.finalize())),
            dir_metadatas: Arc::new(RwLock::new(HashMap::from_iter(
                directory_metadatas
            ))),
            dirs: Arc::new(Mutex::new(dirs)),
        }
    }
}

#[tokio::main]
async fn main() {
    let fs = NaiveFs::random();
    dbg!(fs);
}
