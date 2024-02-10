use clap::Parser;

mod collector;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    from_block: u64,

    #[arg(short, long)]
    to_block: u64,

    #[arg(short, long)]
    path: String,

    #[arg(short, long)]
    rpc: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let opts = collector::Opts {
        from_block: args.from_block,
        to_block: args.to_block,
        path: args.path,
        rpc: args.rpc,
    };

    collector::collect(opts).await;
}
