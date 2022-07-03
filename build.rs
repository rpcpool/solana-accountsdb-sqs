use vergen::{vergen, Config};

fn main() -> anyhow::Result<()> {
    vergen(Config::default())?;

    // vergen git version does not looks cool
    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!()
    );

    Ok(())
}
