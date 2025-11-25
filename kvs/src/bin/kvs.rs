use clap::{Parser, Subcommand};
use kvs::{KvStore, Result};

#[derive(Parser, Debug)]
#[command(
    name = env!("CARGO_PKG_NAME"),
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
    about = env!("CARGO_PKG_DESCRIPTION"),
)]
struct Cli {
    #[command(subcommand)]
    commands: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Get key from store
    Get {
        key: String,
    },
    /// Set key to value in store
    Set {
        key: String,
        value: String,
    },
    /// Remove key from store
    RM {
        key: String,
    },
}

fn main() -> Result<()> {
    let store = KvStore::open(std::env::current_dir()?)?;

    let cli = Cli::parse();
    match &cli.commands {
        Commands::Get { key } => {
            match store.get(key.to_string()) {
                Ok(Some(value)) => println!("{value}"),
                Ok(None) => println!("Key not found"),
                Err(e) => eprintln!("{e}"),
            }
        },
        Commands::Set { key, value } => {
            store.set(key.to_string(), value.to_string())?;
        }
        Commands::RM { key } => {
            store.remove(key.to_string())?
        },
    }

    Ok(())
}
