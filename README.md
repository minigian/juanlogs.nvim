# juan-log.nvim

A high-performance log viewer for Neovim, powered by Rust and Piece Tables.
It can open and search through 50GB+ files instantly without freezing Neovim.

## Requirements
- Neovim >= 0.9.0
- Rust / Cargo (to compile the core engine)

## Installation

Using **lazy.nvim**:

```lua
{
    "minigian/juan-log.nvim",
    build = "cargo build --release",
    config = function()
        require("juan_log").setup({
            threshold_size = 1024 * 1024 * 100, -- 100MB
            mode = "dynamic",
            patterns = { "*.log", "*.csv" }
        })
    end
}
