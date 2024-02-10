mod collector;

use std::env;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 5 {
        println!("Error, specify from_block, to_block, path to file and rpc");
        return;
    }

    let from_block = match args[1].parse::<u64>() {
        Ok(x) => x,
        Err(x) => {
            println!("Can not parse from_block parameter: {:?}", x);
            return;
        }
    };

    let to_block = match args[2].parse::<u64>()  {
        Ok(x) => x,
        Err(x) => {
            println!("Can not parse to_block parameter: {:?}", x);
            return;
        }
    };

    let path = args[3].clone();
    let rpc = args[4].clone();

    let opts = collector::Opts {
        from_block: from_block,
        to_block: to_block,
        path: path,
        rpc: rpc,
    };

    collector::collect(opts).await;
}
