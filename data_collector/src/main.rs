use clap::{Parser, Subcommand};

mod binance_collector;
mod blocks_collector;
mod logs_collector;
mod logs_processor;
mod pools_collector;
mod raw_csv_processor;
mod utils;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    LogsCollector(LogsCollectorArgs),
    LogsProcessor(LogsProcessorArgs),
    RawCSVProcessor(RawCSVsProcessorArgs),
    PoolsCollector(PoolsCollectorArgs),
    BlocksCollector(BlocksCollectorArgs),
    BinanceCollector(BinanceCollectorArgs),
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

    match cli.command {
        Commands::LogsCollector(args) => {
            let opts = logs_collector::Opts {
                from_block: args.from_block,
                to_block: args.to_block,
                path: args.path,
                rpc: args.rpc,
            };

            logs_collector::collect(opts).await;
        }

        Commands::LogsProcessor(args) => {
            let output_dir = args.output_dir.clone();
            let processor = logs_processor::LogsProcessor::new(args);
            processor.write_raw_csvs(&output_dir).await;
        }

        Commands::RawCSVProcessor(args) => {
            let processor = raw_csv_processor::RawCSVProcessor::new(args);
            processor.write_tokens_csv();
        }

        Commands::PoolsCollector(args) => {
            let pools_collector = pools_collector::PoolCollector::new(args);
            pools_collector.collect().await;
        }

        Commands::BlocksCollector(args) => {
            blocks_collector::collect(args).await;
        }

        Commands::BinanceCollector(args) => {
            binance_collector::fetch_all_trades(&args.output_filepath, "WIFUSDT").await;
        }
    };
}
