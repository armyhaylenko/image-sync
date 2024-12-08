use std::collections::HashMap;

use blake3::{Hash, Hasher};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use crate::error::Error;

pub const DATE_FORMAT_DIR: &str = "%Y-%m-%d";
pub const DATE_FORMAT_IMG: &str = "%Y-%m-%d:%h-%M-%s";
pub const AVAILABLE_DATES: [NaiveDate; 30] = [
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

#[derive(Clone)]
pub struct Image {
    /// the actual date & time value we care about for ordering
    pub created_at: NaiveDateTime,
}

impl std::fmt::Debug for Image {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|{}| {:?}...", self.created_at, self.hash().to_hex())
    }
}

impl Image {
    fn new(created_at: NaiveDateTime) -> Self {
        Self { created_at }
    }

    fn hash(&self) -> Hash {
        #[cfg(debug_assertions)]
        tracing::trace!(img_name = %self.created_at, "Hashing self");

        Hasher::new()
            .update(
                &self
                    .created_at
                    .format("%Y-%m-%d:%h-%M-%s")
                    .to_string()
                    .as_bytes(),
            )
            .finalize()
    }
}

#[derive(Debug)]
pub struct Directory {
    /// actual value we care about for the sync
    #[allow(dead_code)]
    pub date: NaiveDate,
    /// images inside of the directory, assuming sorted by creation
    pub images: Vec<(Hash, Image)>,
}

/// This struct holds the metadata of the directory.
/// Primary sync primitive.
#[derive(Debug, Clone, Copy)]
pub struct DirectoryMetadata {
    pub dir_hash: Hash,
    pub fs_index: usize,
}

impl Directory {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();
        for (image_hash, _) in self.images.iter() {
            hasher.update(image_hash.as_bytes());
        }

        hasher.finalize()
    }
}

#[derive(Debug)]
pub struct NaiveFs {
    /// Assuming that we only have directories in the fs
    /// The obv choice for such task is an MPT, but i want to rely
    /// on libraries as least as possible.
    pub dir_metadatas: HashMap<NaiveDate, DirectoryMetadata>,
    pub dirs: Vec<Directory>,
}

impl NaiveFs {
    /// Creates a random fs.
    pub fn random() -> Self {
        let mut root_hasher = Hasher::new();
        let gen_directory_images = |date: NaiveDate| -> Vec<(Hash, Image)> {
            gen_image_times(date)
                .into_iter()
                .map(|t| {
                    let i = Image::new(t);
                    let hash = i.hash();
                    (hash, i)
                })
                .collect()
        };
        let dirs: Vec<(NaiveDate, Directory, DirectoryMetadata)> = AVAILABLE_DATES
            .into_iter()
            .enumerate()
            .map(|(idx, d)| {
                let images = gen_directory_images(d);
                let dir = Directory {
                    date: d,
                    // TODO: remove the clone
                    images: images.clone(),
                };
                let dir_hash = dir.hash();
                tracing::debug!(dir = d.format(DATE_FORMAT_DIR).to_string(), ?images, dir_hash = %dir_hash.to_hex(), "images");
                root_hasher.update(dir_hash.as_bytes());
                let dir_metadata = DirectoryMetadata {
                    dir_hash,
                    fs_index: idx,
                };
                (d, dir, dir_metadata)
            }) // return both the date and the dir to build
            // the hashmap
            .collect();

        let directory_metadatas = dirs
            .iter()
            .map(|(date, _, meta)| (*date, *meta))
            .collect::<Vec<(NaiveDate, DirectoryMetadata)>>();
        let dirs = dirs.into_iter().map(|(_, dir, _)| dir).collect();

        Self {
            dir_metadatas: HashMap::from_iter(directory_metadatas),
            dirs,
        }
    }

    /// Creates a fully filled fs.
    pub fn full() -> Self {
        let mut root_hasher = Hasher::new();
        let gen_directory_images = |date: NaiveDate| -> Vec<(Hash, Image)> {
            AVAILABLE_TIMES
                .into_iter()
                .map(|t| {
                    let i = Image::new(NaiveDateTime::new(date, t));
                    let hash = i.hash();
                    (hash, i)
                })
                .collect()
        };
        let dirs: Vec<(NaiveDate, Directory, DirectoryMetadata)> = AVAILABLE_DATES
            .into_iter()
            .enumerate()
            .map(|(idx, d)| {
                let images = gen_directory_images(d);
                let dir = Directory {
                    date: d,
                    // TODO: remove the clone
                    images: images.clone(),
                };
                let dir_hash = dir.hash();
                root_hasher.update(dir_hash.as_bytes());
                let dir_metadata = DirectoryMetadata {
                    dir_hash,
                    fs_index: idx,
                };
                (d, dir, dir_metadata)
            })
            // the hashmap
            .collect();

        let directory_metadatas = dirs
            .iter()
            .map(|(date, _, meta)| (*date, *meta))
            .collect::<Vec<(NaiveDate, DirectoryMetadata)>>();
        let dirs = dirs.into_iter().map(|(_, dir, _)| dir).collect();

        Self {
            dir_metadatas: HashMap::from_iter(directory_metadatas),
            dirs,
        }
    }

    pub fn dir_state(&self, date: &NaiveDate) -> Result<Hash, Error> {
        Ok(self
            .dir_metadatas
            .get(date)
            .ok_or(Error::BadDate(*date))?
            .dir_hash)
    }

    pub async fn get_images_for_date(&self, date: &NaiveDate) -> Result<Vec<(Hash, Image)>, Error> {
        let DirectoryMetadata { fs_index, .. } = self
            .dir_metadatas
            .get(date)
            .cloned()
            .ok_or(Error::BadDate(*date))?;
        let dir = self.dirs.get(fs_index).ok_or(Error::BadDate(*date))?;

        Ok(dir.images.clone())
    }
}
