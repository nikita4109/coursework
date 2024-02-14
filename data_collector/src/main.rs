use clap::{Parser, Subcommand};

mod logs_collector;
mod logs_processor;
mod pools_collector;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    LogsCollector(LogsCollectorArgs),
    LogsProcessor(LogsProcessorArgs),
    PoolsCollector(PoolsCollectorArgs),
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
    cex_data_path: String,

    #[arg(short, long)]
    logs_path: String,

    #[arg(short, long)]
    pools_path: String,

    #[arg(short, long)]
    output_filepath: String,
}

#[derive(Parser)]
struct PoolsCollectorArgs {
    #[arg(short, long)]
    rpc: String,

    #[arg(short, long)]
    abi_path: String,

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
            let output_filepath = args.output_filepath.clone();
            let processor = logs_processor::LogsProcessor::new(args);
            processor.write_csv(&output_filepath);
        }

        Commands::PoolsCollector(args) => {
            let pools_collector = pools_collector::PoolCollector::new(args);
            pools_collector.collect().await;
        }
    };
}
