#[macro_use]
extern crate diesel;

use clap::{Parser, Subcommand};

mod blocks_collector;
mod db;
mod logs_collector;
mod logs_processor;
mod pools_collector;
mod raw_csv_processor;
mod utils;

use db::db::establish_connection;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(short, long)]
    db_url: String,
}

#[derive(Subcommand)]
enum Commands {
    LogsCollector(LogsCollectorArgs),
    LogsProcessor(LogsProcessorArgs),
    RawCSVProcessor(RawCSVsProcessorArgs),
    PoolsCollector(PoolsCollectorArgs),
    BlocksCollector(BlocksCollectorArgs),
}

#[derive(Parser)]
struct LogsCollectorArgs {
    #[arg(short, long)]
    from_block: u64,

    #[arg(short, long)]
    to_block: u64,

    #[arg(short, long)]
    path: String,

    #[arg(short, long)]
    rpc: String,
}

#[derive(Parser)]
struct LogsProcessorArgs {
    #[arg(short, long)]
    rpc: String,

    #[arg(short, long)]
    cex_data_path: String,

    #[arg(short, long)]
    logs_path: String,

    #[arg(short, long)]
    pools_path: String,

    #[arg(short, long)]
    output_dir: String,
}

#[derive(Parser)]
struct RawCSVsProcessorArgs {
    #[arg(short, long)]
    blocks_window_len: u64,

    #[arg(short, long)]
    candlestick_len: u64,

    #[arg(short, long)]
    swaps_path: String,

    #[arg(short, long)]
    output_dir: String,
}

#[derive(Parser)]
struct PoolsCollectorArgs {
    #[arg(short, long)]
    rpc: String,

    #[arg(short, long)]
    output_filepath: String,
}

#[derive(Parser)]
struct BlocksCollectorArgs {
    #[arg(short, long)]
    rpc: String,

    #[arg(short, long)]
    start_block: u64,

    #[arg(short, long)]
    end_block: u64,

    #[arg(short, long)]
    output_filepath: String,
}

#[derive(Parser)]
struct BinanceCollectorArgs {
    #[arg(short, long)]
    output_filepath: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let conn = establish_connection(&cli.db_url);

    match cli.command {
        Commands::LogsCollector(args) => {
            let opts = logs_collector::Opts {
                from_block: args.from_block,
                to_block: args.to_block,
                path: args.path,
                rpc: args.rpc,
            };

            logs_collector::collect(&conn, opts).await;
        }

        Commands::LogsProcessor(args) => {
            let output_dir = args.output_dir.clone();
            let processor = logs_processor::LogsProcessor::new(&conn, args);
            processor.save_to_db(&conn).await;
        }

        Commands::RawCSVProcessor(args) => {
            let processor = raw_csv_processor::RawCSVProcessor::new(args);
            processor.save_tokens_db(conn);
        }

        Commands::PoolsCollector(args) => {
            let pools_collector = pools_collector::PoolCollector::new(args);
            pools_collector.collect(conn).await;
        }

        Commands::BlocksCollector(args) => {
            blocks_collector::collect(&conn, args).await;
        }
    };
}
