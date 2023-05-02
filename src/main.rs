use std::error::Error;

use synth_table::prompts::run_workflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run_workflow().await?;
    Ok(())
}
