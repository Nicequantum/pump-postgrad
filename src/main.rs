//! Standalone post-graduation Raydium sniper — Shredstream gRPC + Jupiter execution.

fn main() -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(64 * 1024 * 1024)
        .build()
        .expect("tokio runtime");

    rt.block_on(pump_postgrad::run())
}