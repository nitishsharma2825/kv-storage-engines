use std::{collections::HashMap, fs::{File, OpenOptions}, io::{self, BufReader, BufWriter, Read, Seek, Write}, path::PathBuf, sync::{Mutex, PoisonError, RwLock}};

const LOG_FILE: &str = "log.data";
const COMPACTION_THRESHOLD_BYTES: u32 = 1024000;

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum EngineCommand {
    SetCommand { key: String, value: String},
    RmCommand { key: String },
}

#[derive(Debug, Clone, Copy)]
pub struct LogPointer {
    offset: u32,
}

pub struct KvStore {
    log_path: PathBuf,
    index: RwLock<HashMap<String, LogPointer>>,
    cur_offset: Mutex<u32>,
    reader: Mutex<BufReader<File>>, // works only with single thread code
    writer: Mutex<BufWriter<File>>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let log_path = PathBuf::from(path.into()).join(LOG_FILE);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&log_path)?;

        let writer = BufWriter::new(log_file.try_clone()?); // independent file offsets
        let reader_lock = Mutex::new(BufReader::new(log_file));
        let mut index = HashMap::new();
        let mut cur_offset = 0;

        loop {
            let mut len_buf = [0u8; 4];
            let mut reader_mut = reader_lock.lock()?;
            match reader_mut.read_exact(&mut len_buf) {
                Ok(()) => (),
                Err(_) => break, // EOF
            }

            let len = u32::from_le_bytes(len_buf);
            let mut buf = vec![0u8; len as usize];
            reader_mut.read_exact(&mut buf)?;

            let entry: EngineCommand = bincode::deserialize(&buf)?;
            match entry {
                EngineCommand::SetCommand { key, .. } => {
                    index.insert(key, LogPointer { offset: cur_offset });
                    cur_offset = cur_offset + 4 + len;
                },
                EngineCommand::RmCommand { key } => {
                    index.remove(&key);
                    cur_offset = cur_offset + 4 + len;
                },
            };
        }

        Ok(KvStore {
            log_path: log_path,
            index: RwLock::new(index),
            cur_offset: Mutex::new(cur_offset),
            reader: reader_lock,
            writer: Mutex::new(writer),
        })
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        // acquire read lock on index, blocks thread
        let index_lock = self.index.read()?;
        let mut reader = self.reader.lock()?;

        match index_lock.get(&key).cloned() {
            Some(log_pointer) => {
                reader.seek(io::SeekFrom::Start(log_pointer.offset as u64))?;

                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf)?;

                let len = u32::from_le_bytes(len_buf);

                let mut cmd = vec![0u8; len as usize];
                reader.read_exact(&mut cmd)?;

                let log_entry: EngineCommand = bincode::deserialize(&cmd)?;
                match log_entry {
                    EngineCommand::SetCommand { value, .. } => return Ok(Some(value.clone())),
                    EngineCommand::RmCommand { .. } => return Err(EngineError::UnexpectedResult)
                }
            },
            None => return Ok(None)
        }
    }

    pub fn set(&self, key: String, value: String) -> Result<()>{
        // acquire locks
        let mut index_lock = self.index.write()?;
        let mut writer_lock = self.writer.lock()?;
        let mut offset_lock = self.cur_offset.lock()?;

        let set_command = EngineCommand::SetCommand { key: key.clone(), value: value.clone() };
        let bytes = bincode::serialize(&set_command)?;

        let length = bytes.len() as u32;
        writer_lock.write_all(&length.to_le_bytes())?;
        writer_lock.write_all(&bytes)?;
        writer_lock.flush()?;

        index_lock.insert(key, LogPointer { offset: *offset_lock });

        *offset_lock = *offset_lock + 4 + length;

        let should_compact = true;

        drop(index_lock);
        drop(writer_lock);
        drop(offset_lock);

        if should_compact {
            self.compact()?;
        }

        Ok(())
    }

    pub fn remove(&self, key: String) -> Result<()>{
        // acquire locks
        let mut index_lock = self.index.write()?;
        let mut writer_lock = self.writer.lock()?;
        let mut offset_lock = self.cur_offset.lock()?;

        if index_lock.get(&key).is_none() {
            println!("Key not found");
            return Err(EngineError::KeyNotFound);
        }

        let rm_command = EngineCommand::RmCommand { key: key.clone() };
        let bytes = bincode::serialize(&rm_command)?;

        let length = bytes.len() as u32;
        writer_lock.write_all(&length.to_le_bytes())?;
        writer_lock.write_all(&bytes)?;
        writer_lock.flush()?;

        index_lock.remove(&key);
        *offset_lock = *offset_lock + 4 + length;

        let should_compact = *offset_lock >= COMPACTION_THRESHOLD_BYTES;

        drop(index_lock);
        drop(writer_lock);
        drop(offset_lock);

        if should_compact {
            self.compact()?;
        }

        Ok(())
    }

    fn compact(&self) -> Result<()> {
        let temp_log_path = self.log_path.parent().unwrap().join(PathBuf::from("temp_log.data"));
        let temp_log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&temp_log_path)?;

        let mut index_lock = self.index.write()?;
        let mut _writer_lock = self.writer.lock()?;
        let mut reader = self.reader.lock()?;
        let mut offset_lock = self.cur_offset.lock()?;

        let mut writer = BufWriter::new(temp_log_file.try_clone()?);
        let mut new_offset = 0;

        let entries: Vec<(String, LogPointer)> = index_lock
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        for (key, log_pointer) in entries {
            reader.seek(io::SeekFrom::Start(log_pointer.offset as u64))?;

            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;

            let len = u32::from_le_bytes(len_buf);

            let mut cmd = vec![0u8; len as usize];
            reader.read_exact(&mut cmd)?;

            let log_entry: EngineCommand = bincode::deserialize(&cmd)?;
            match log_entry {
                EngineCommand::SetCommand { value, .. } => {
                    let set_command = EngineCommand::SetCommand { key: key.clone(), value: value };
                    let bytes = bincode::serialize(&set_command)?;

                    let length = bytes.len() as u32;
                    writer.write_all(&length.to_le_bytes())?;
                    writer.write_all(&bytes)?;

                    let new_log_pointer = LogPointer { offset: new_offset };
                    index_lock.insert(key, new_log_pointer);
                    new_offset += 4 + length;
                },
                EngineCommand::RmCommand { .. } => return Err(EngineError::UnexpectedResult)
            };
        }

        writer.flush()?;
        drop(writer);
        temp_log_file.sync_all()?;

        std::fs::rename(&temp_log_path, &self.log_path)?;

        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&self.log_path)?;

        // update data structures
        *offset_lock = new_offset;
        *_writer_lock = BufWriter::new(new_file.try_clone()?);
        *reader = BufReader::new(new_file);

        Ok(())
    }
}

#[derive(Debug)]
pub enum EngineError {
    IoError(io::Error),
    KeyNotFound,
    BincodeError(String),
    LockPoisoned(String),
    UnexpectedResult,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::IoError(err) => write!(f, "IO error: {}", err),
            EngineError::KeyNotFound => write!(f, "Key not found"),
            EngineError::BincodeError(err) => write!(f, "Serde error: {}", err),
            EngineError::LockPoisoned(err) => write!(f, "Lock poisoned: {}", err),
            EngineError::UnexpectedResult => write!(f, "Unexpected result"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<io::Error> for EngineError {
    fn from(value: io::Error) -> Self {
        EngineError::IoError(value)
    }
}

impl From<bincode::Error> for EngineError {
    fn from(value: bincode::Error) -> Self {
        EngineError::BincodeError(value.to_string())
    }
}

impl<T> From<PoisonError<T>> for EngineError {
    fn from(value: PoisonError<T>) -> Self {
        EngineError::LockPoisoned(value.to_string())
    }
}