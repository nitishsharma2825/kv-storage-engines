use std::{collections::HashMap, fs::{File, OpenOptions}, io::{self, BufReader, BufWriter, Read, Seek, Write}, path::PathBuf, sync::PoisonError};

const LOG_FILE: &str = "log.data";
const COMPACTION_THRESHOLD_BYTES: u64 = 100;

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum EngineCommand {
    SetCommand { key: String, value: String},
    RmCommand { key: String },
}

#[derive(Debug, Clone, Copy)]
struct LogPointer {
    offset: u64,
}

pub struct KvStore {
    path: PathBuf,
    index: HashMap<String, LogPointer>,
    cur_offset: u64,
    garbage: u64,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = PathBuf::from(path.into()).join(LOG_FILE);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&path)?;

        let writer = BufWriter::new(log_file.try_clone()?); // independent file offsets
        let mut reader = BufReader::new(File::open(&path)?);
        let mut index = HashMap::new();
        let mut cur_offset = 0;
        let mut garbage = 0;

        loop {
            let mut len_buf = [0u8; 8];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => (),
                Err(_) => break, // EOF
            }

            let len = u64::from_le_bytes(len_buf);
            let mut buf = vec![0u8; len as usize];
            reader.read_exact(&mut buf)?;

            let entry: EngineCommand = bincode::deserialize(&buf)?;
            match entry {
                EngineCommand::SetCommand { key, .. } => {
                    if index.insert(key, LogPointer { offset: cur_offset }).is_some() {
                        garbage = garbage + len + 8; 
                    }
                    cur_offset = cur_offset + 8 + len;
                },
                EngineCommand::RmCommand { key } => {
                    if index.remove(&key).is_some() {
                        garbage = garbage + len + 8;
                    }
                    cur_offset = cur_offset + 8 + len;
                },
            };
        }

        Ok(KvStore {
            path: path,
            index: index,
            cur_offset: cur_offset,
            garbage: garbage,
            reader: reader,
            writer: writer,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key).cloned() {
            Some(log_pointer) => {
                self.reader.seek(io::SeekFrom::Start(log_pointer.offset as u64))?;

                let mut len_buf = [0u8; 8];
                self.reader.read_exact(&mut len_buf)?;

                let len = u64::from_le_bytes(len_buf);

                let mut cmd = vec![0u8; len as usize];
                self.reader.read_exact(&mut cmd)?;

                let log_entry: EngineCommand = bincode::deserialize(&cmd)?;
                match log_entry {
                    EngineCommand::SetCommand { value, .. } => return Ok(Some(value.clone())),
                    EngineCommand::RmCommand { .. } => return Err(EngineError::UnexpectedResult)
                }
            },
            None => return Ok(None)
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()>{
        let set_command = EngineCommand::SetCommand { key: key.clone(), value: value.clone() };
        let bytes = bincode::serialize(&set_command)?;

        let length = bytes.len() as u64;
        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        if self.index.insert(key, LogPointer { offset: self.cur_offset }).is_some() {
            self.garbage += 8 + length;
        }
        self.cur_offset = self.cur_offset + 8 + length;

        if self.garbage > COMPACTION_THRESHOLD_BYTES {
            self.compact()?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()>{
        if self.index.get(&key).is_none() {
            return Err(EngineError::KeyNotFound);
        }

        let rm_command = EngineCommand::RmCommand { key: key.clone() };
        let bytes = bincode::serialize(&rm_command)?;

        let length = bytes.len() as u64;
        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        if self.index.remove(&key).is_some() {
            self.garbage += 8 + length;
        }
        self.cur_offset = self.cur_offset + 8 + length;

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let temp_log_path = self.path.parent().unwrap().join(PathBuf::from("temp_log.data"));
        let temp_log_file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&temp_log_path)?;

        let mut writer = BufWriter::new(temp_log_file.try_clone()?);
        let mut new_offset = 0;

        for cmd in self.index.values_mut() {
            self.reader.seek(io::SeekFrom::Start(cmd.offset))?;

            // read 8 byte length
            let mut len_buf = [0u8; 8];
            self.reader.read_exact(&mut len_buf)?;
            let len = u64::from_le_bytes(len_buf) as usize;
            
            let mut payload = vec![0u8; len];
            self.reader.read_exact(&mut payload)?;

            writer.write_all(&len_buf)?;
            writer.write_all(&payload)?;

            cmd.offset = new_offset;

            new_offset = new_offset + 8 + len as u64;
        }

        writer.flush()?;
        temp_log_file.sync_all()?;

        std::fs::rename(&temp_log_path, &self.path)?;

        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&self.path)?;

        // update data structures
        self.cur_offset = new_offset;
        self.writer = BufWriter::new(new_file.try_clone()?);
        self.reader = BufReader::new(File::open(&self.path)?);
        self.garbage = 0;

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