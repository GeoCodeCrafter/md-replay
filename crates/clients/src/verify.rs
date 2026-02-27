use crate::feature::{run_feature, FeatureConfig};
use md_replay_engine::read_events;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VerifyError {
    #[error("replay error: {0}")]
    Replay(#[from] md_replay_engine::ReplayError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("determinism check failed")]
    NonDeterministic,
}

pub fn verify_feature_determinism(
    log_path: &Path,
    index_path: Option<&Path>,
    seed: u64,
    out_path: &Path,
) -> Result<(), VerifyError> {
    let events = read_events(log_path, index_path, None, None)?;
    let cfg = seeded_feature_config(seed);

    let run1 = run_feature(&events, &cfg).join("\n");
    let run2 = run_feature(&events, &cfg).join("\n");

    if run1.as_bytes() != run2.as_bytes() {
        return Err(VerifyError::NonDeterministic);
    }

    let mut bytes = run1.into_bytes();
    bytes.push(b'\n');
    std::fs::write(out_path, bytes)?;
    Ok(())
}

fn seeded_feature_config(seed: u64) -> FeatureConfig {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    FeatureConfig {
        mid_window: 8,
        ewma_alpha: 0.1 + rng.gen_range(0.0..0.25),
        spread_threshold: 20 + rng.gen_range(0..10),
        imbalance_threshold: 0.6 + rng.gen_range(0.0..0.2),
        vol_threshold: 0.02 + rng.gen_range(0.0..0.02),
    }
}
