use clap::{Parser, Subcommand};
use kvs::KvStore;

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

fn main() {
    let mut kv_store = KvStore::new();
    let cli = Cli::parse();

    match &cli.commands {
        Commands::Get { key } => {
            let result = kv_store.get(key.to_string());
            match result {
                Some(val) => println!("{}", val),
                None => println!("Value does not exist"),
            }
        }
        Commands::Set { key, value } => {
            kv_store.set(key.to_string(), value.to_string());
        }
        Commands::RM { key } => {
            kv_store.remove(key.to_string());
        }
    }
}
