use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Config {
    /// Name of this node
    #[arg(short, long, required = true)]
    pub name: String,

    /// Names of sunlab nodes to connect to
    #[arg(short, long, required = true, value_delimiter = ',')]
    pub connections: Vec<String>,

    /// Number of keys to test on
    #[arg(short, long, default_value = "1000000")]
    pub num_keys: usize,

    /// Range of keys
    #[arg(short, long, default_value = "1000")]
    pub key_range: u64,

    /// how many nodes hold each key
    #[arg(short, long, default_value = "2")]
    pub repication_degree: usize,

    /// How many locked sections in the database
    #[arg(short, long, default_value = "100")]
    pub stripes: usize,
}
