node {
   // Clean old workspace
   stage 'Setup Workspace'
   deleteDir()

   sh 'pwd > pwd.current'
   env.WORKSPACE = readFile('pwd.current').trim()
   echo "${env.WORKSPACE}"

   // Mark the code checkout stage....
   stage 'Checkout'
   sh "mkdir cernan"
   sh "cd cernan/"
   checkout scm

   sh "git rev-parse HEAD | cut -b1-8 > ver.current"
   env.VERSION = readFile('ver.current').trim()
   echo "${env.VERSION}"

   sh "awk -F'\"' '/version/ {print \$2}' Cargo.toml > semvar.current"
   env.SEMVAR = readFile('semvar.current').trim()
   echo "${env.SEMVAR}"


   stage 'Install Deps'
   env.LD_LIBRARY_PATH = "${env.WORKSPACE}/rust/rustc/lib:${env.LD_LIBRARY_PATH}"
   env.PATH = "${env.WORKSPACE}/rust/bin:${env.PATH}"
   env.RUSTUP_HOME = "${env.WORKSPACE}/.rustup"

   sh "curl -sSf https://static.rust-lang.org/rustup.sh > rustup.sh"
   sh "chmod +x rustup.sh"

   stage 'Stable Tests'
   sh "./rustup.sh --yes --disable-sudo --prefix=${env.WORKSPACE}/rust default stable"
   sh "cargo clean"
   sh "cargo test"

   stage 'Beta Tests'
   sh "./rustup.sh --yes --disable-sudo --prefix=${env.WORKSPACE}/rust default beta"
   sh "cargo clean"
   sh "cargo test"

    stage 'Build Artifact'
    sh "./rustup.sh --yes --disable-sudo --prefix=${env.WORKSPACE}/rust default stable"
    sh "cargo build --release"

    sh "aws s3 cp target/release/cernan s3://artifacts.postmates.com/binaries/cernan/cernan-${env.VERSION}"
    if (env.BRANCH_NAME == "stable") {
       sh "aws s3 cp target/release/cernan s3://artifacts.postmates.com/binaries/cernan/cernan"
       sh "aws s3 cp target/release/cernan s3://artifacts.postmates.com/binaries/cernan/cernan-${env.SEMVAR}"
    }
}
