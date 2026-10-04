//! Runtime controls shared by the comparison launchers, for scheduler measurements.
use super::application::Result;
use std::future::Future;

pub(crate) fn run<F: Future<Output = Result<()>>>(
    application: impl FnOnce(Vec<String>) -> F,
) -> Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let mut builder = match args.as_slice() {
        [benchmark, runtime, mode] if benchmark == "--benchmark" && runtime == "--runtime" => {
            let builder = match mode.as_str() {
                "current" => tokio::runtime::Builder::new_current_thread(),
                "two" => {
                    let mut builder = tokio::runtime::Builder::new_multi_thread();
                    builder.worker_threads(2);
                    builder
                }
                "default" => tokio::runtime::Builder::new_multi_thread(),
                _ => return Err("benchmark runtime must be current, two, or default".into()),
            };
            args.truncate(1);
            builder
        }
        _ => tokio::runtime::Builder::new_multi_thread(),
    };
    builder.enable_all().build()?.block_on(application(args))
}
