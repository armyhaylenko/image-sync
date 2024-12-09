use blake3::{Hash, Hasher};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

use crate::error::Error;

pub const DATE_FORMAT_DIR: &str = "%Y-%m-%d";
pub const DATE_FORMAT_IMG: &str = "%Y-%m-%d:%H-%M-%S";
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
                    .format(DATE_FORMAT_IMG)
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
    pub root_hash: Option<Hash>,
    pub dirs: Vec<Directory>,
}

impl NaiveFs {
    /// Creates a random fs.
    pub fn random() -> Self {
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
        let dirs: Vec<Directory> = AVAILABLE_DATES
            .into_iter()
            .map(|d| {
                let images = gen_directory_images(d);
                let dir = Directory {
                    date: d,
                    // TODO: remove the clone
                    images: images.clone(),
                };
                dir
            }) // return both the date and the dir to build
            // the hashmap
            .collect();

        Self {
            dirs,
            root_hash: None,
        }
    }

    /// Creates a fully filled fs.
    pub fn full() -> Self {
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
        let dirs: Vec<Directory> = AVAILABLE_DATES
            .into_iter()
            .map(|d| {
                let images = gen_directory_images(d);
                let dir = Directory {
                    date: d,
                    // TODO: remove the clone
                    images: images.clone(),
                };
                dir
            })
            // the hashmap
            .collect();

        Self {
            root_hash: None,
            dirs,
        }
    }

    pub fn add_image(&mut self, date: NaiveDate, image: Image) -> Result<(), Error> {
        let dir = self
            .dirs
            .iter_mut()
            .find(|d| d.date == date)
            .ok_or(Error::BadDate(date))?;
        if dir
            .images
            .iter()
            .any(|(h, i)| h == &image.hash() || i.created_at == image.created_at)
        {
            // do not insert duplicates
            return Ok(());
        }
        dir.images.push((image.hash(), image));
        dir.images.sort_by_key(|(_hash, image)| image.created_at);

        Ok(())
    }

    pub fn update_root_hash(&mut self) {
        let mut root_hasher = Hasher::new();
        self.dirs.sort_by_key(|d| d.date);
        self.dirs.iter_mut().for_each(|d| {
            d.images.sort_by_key(|(_h, i)| i.created_at);
        });

        for d in self.dirs.iter() {
            root_hasher.update(d.hash().as_bytes());
        }

        self.root_hash = Some(root_hasher.finalize());
    }

    pub fn dir_state(&self, date: &NaiveDate) -> Result<Hash, Error> {
        Ok(self
            .dirs
            .iter()
            .find(|d| &d.date == date)
            .ok_or(Error::BadDate(*date))?
            .hash())
    }

    pub async fn get_images_for_date(&self, date: &NaiveDate) -> Result<Vec<(Hash, Image)>, Error> {
        let dir = self
            .dirs
            .iter()
            .find(|d| &d.date == date)
            .ok_or(Error::BadDate(*date))?;

        Ok(dir.images.clone())
    }
}
