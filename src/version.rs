use {serde::Serialize, std::env};

#[derive(Debug, Serialize)]
pub struct Version {
    pub buildts: &'static str,
    pub git: &'static str,
    pub rustc: &'static str,
    pub solana: &'static str,
    pub version: &'static str,
}

pub const VERSION: Version = Version {
    buildts: env!("VERGEN_BUILD_TIMESTAMP"),
    git: env!("GIT_VERSION"),
    rustc: env!("VERGEN_RUSTC_SEMVER"),
    solana: env!("SOLANA_SDK_VERSION"),
    version: env!("VERGEN_BUILD_SEMVER"),
};
