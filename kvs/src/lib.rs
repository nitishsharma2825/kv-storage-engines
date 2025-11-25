use std::{collections::HashMap, fs::{File, OpenOptions}, io::{self, BufReader, BufWriter, Read, Seek, Write}, path::PathBuf};
use std::cell::RefCell;

const LOG_FILE: &str = "log.data";

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
    index: HashMap<String, LogPointer>,
    cur_offset: u32,
    reader: RefCell<BufReader<File>>, // works only with single thread code
    writer: BufWriter<File>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let log_path = PathBuf::from(path.into()).join(LOG_FILE);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(log_path)?;

        let writer = BufWriter::new(log_file.try_clone()?); // independent file offsets
        let reader = RefCell::new(BufReader::new(log_file));
        let mut index = HashMap::new();
        let mut cur_offset = 0;

        loop {
            let mut len_buf = [0u8; 4];
            let mut reader_mut = reader.borrow_mut();
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
                },
            };
        }

        Ok(KvStore {
            index: index,
            cur_offset: cur_offset,
            reader: reader,
            writer: writer,
        })
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.index.get(&key).cloned() {
            Some(log_pointer) => {
                let mut reader = self.reader.borrow_mut();
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

    pub fn set(&mut self, key: String, value: String) -> Result<()>{
        let set_command = EngineCommand::SetCommand { key: key.clone(), value: value.clone() };
        let bytes = bincode::serialize(&set_command)?;

        let length = bytes.len() as u32;
        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        self.index.insert(key, LogPointer { offset: self.cur_offset });
        self.cur_offset = self.cur_offset + 4 + length;
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()>{
        if self.index.get(&key).is_none() {
            println!("Key not found");
            return Err(EngineError::KeyNotFound);
        }

        let rm_command = EngineCommand::RmCommand { key: key.clone() };
        let bytes = bincode::serialize(&rm_command)?;

        let length = bytes.len() as u32;
        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;

        self.index.remove(&key);
        self.cur_offset = self.cur_offset + 4 + length;
        Ok(())
    }
}

#[derive(Debug)]
pub enum EngineError {
    IoError(io::Error),
    KeyNotFound,
    BincodeError(String),
    UnexpectedResult,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::IoError(err) => write!(f, "IO error: {}", err),
            EngineError::KeyNotFound => write!(f, "Key not found"),
            EngineError::BincodeError(err) => write!(f, "Serde error: {}", err),
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
    fn from(value: Box<bincode::ErrorKind>) -> Self {
        EngineError::BincodeError(value.to_string())
    }
}