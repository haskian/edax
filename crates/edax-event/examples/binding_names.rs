//! Export the event kernel's closed names for binding generation without parsing Rust source.

use std::io::{self, Write};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let names = serde_json::json!({ "command_fence": edax_event::CommandFence::NAMES });
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, &names)?;
    writeln!(output)?;
    Ok(())
}
