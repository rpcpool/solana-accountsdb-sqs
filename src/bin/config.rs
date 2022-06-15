use {
    anyhow::Result,
    clap::{Parser, Subcommand},
    solana_geyser_sqs::{admin::ConfigMgmt, config::Config},
};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    /// Path to geyser plugin config
    #[clap(short, long)]
    config: String,

    /// Filter for change
    #[clap(subcommand)]
    filter_type: ArgsFilterType,
}

#[derive(Debug, Subcommand)]
enum ArgsFilterType {
    /// Change accounts filters
    Accounts,
    /// Change transactions filters
    #[clap(subcommand)]
    Transactions(ArgsFilterTransactions),
}

#[derive(Debug, Subcommand)]
enum ArgsFilterTransactions {
    Add {
        /// Filter name
        #[clap(short, long)]
        name: String,
        /// Config
        #[clap(short, long)]
        config: String, // Parse to `ConfigTransactionsFilter`?
    },
    Remove {
        /// Filter name
        #[clap(short, long)]
        name: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load_from_file(&args.config)?;

    let admin = ConfigMgmt::new(config.filters.admin.expect("defined admin config")).await?;

    Ok(())
}
