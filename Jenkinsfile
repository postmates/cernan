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

   stage 'Install Deps'
   env.LD_LIBRARY_PATH = "${env.WORKSPACE}/rust/rustc/lib:${env.LD_LIBRARY_PATH}"
   env.PATH = "${env.WORKSPACE}/rust/bin:${env.PATH}"

   echo "${env.LD_LIBRARY_PATH}"
   echo "${env.PATH}"
   echo "${env.WORKSPACE}/rust"

   sh "curl -sSf https://static.rust-lang.org/rustup.sh > rustup.sh"
   sh "chmod +x rustup.sh"

   // "Run tests"
   stage 'Stable Tests'
   sh "./rustup.sh --yes --disable-sudo --prefix=${env.WORKSPACE}/rust default stable"
   sh "cargo clean"
   sh "cargo test"

   stage 'Beta Tests'
   sh "./rustup.sh --yes --disable-sudo --prefix=${env.WORKSPACE}/rust default beta"
   sh "cargo clean"
   sh "cargo test"

   stage 'Nightly Tests'
   sh "./rustup.sh --yes --disable-sudo --prefix=${env.WORKSPACE}/rust default nightly"
   sh "cargo clean"
   sh "cargo test"


}
