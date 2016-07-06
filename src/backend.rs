use backends::console;
use backends::wavefront;
use backends::librato;
use metric::Metric;

use regex::Regex;

/// A 'backend' is a sink for metrics.
pub trait Backend {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Metric) -> ();
}

/// Creates the collection of backends based on the paraemeters
///
pub fn factory(console: &bool,
               wavefront: &bool,
               librato: &bool,
               tags: &str,
               wavefront_host: &str,
               wavefront_port: &u16,
               librato_username: &str,
               librato_token: &str,
               librato_host: &str)
               -> Box<[Box<Backend>]> {
    let mut backends: Vec<Box<Backend>> = Vec::with_capacity(3);
    if *console {
        backends.push(Box::new(console::Console::new()));
    }
    if *wavefront {
        let wf_tags: String = tags.replace(",", " ");
        backends.push(Box::new(wavefront::Wavefront::new(wavefront_host,
                                                         *wavefront_port,
                                                         wf_tags)));
    }
    if *librato {
        let re = Regex::new(r"(?x)(source=(?P<source>.*),+)?").unwrap();
        let metric_source = re.captures(tags).unwrap().name("source").unwrap_or("cernan");
        // librato does not support arbitrary tags, only a 'source' tag. We have
        // to parse the source tag--if it exists--out and ship only that.
        backends.push(Box::new(librato::Librato::new(librato_username,
                                                     librato_token,
                                                     metric_source,
                                                     librato_host)));
    }
    backends.into_boxed_slice()
}


#[cfg(test)]
mod test {
    use super::*;
    use regex::Regex;

    #[test]
    fn wavefront_tag_munging() {
        let tags = "source=s,host=h,service=srv";
        let re = Regex::new(r",").unwrap();
        let wf_tags: String = re.replace_all(tags, " ");

        assert_eq!("source=s host=h service=srv", wf_tags);
    }

    #[test]
    fn factory_makes_wavefront() {
        let backends = factory(&false,
                               &true,
                               &false,
                               "source=src",
                               "127.0.0.1",
                               &2878,
                               "username",
                               "token",
                               "http://librato.example.com/");
        assert_eq!(1, backends.len());
    }

    #[test]
    fn factory_makes_librato() {
        let backends = factory(&false,
                               &false,
                               &true,
                               "source=src,host=h,service=s",
                               "127.0.0.1",
                               &2878,
                               "username",
                               "token",
                               "http://librato.example.com/");
        assert_eq!(1, backends.len());
    }

    #[test]
    fn factory_makes_console() {
        let backends = factory(&true,
                               &false,
                               &false,
                               "source=src",
                               "127.0.0.1",
                               &2878,
                               "username",
                               "token",
                               "http://librato.example.com/");
        assert_eq!(1, backends.len());
    }

    #[test]
    fn factory_makes_all() {
        let backends = factory(&true,
                               &true,
                               &true,
                               "source=src",
                               "127.0.0.1",
                               &2878,
                               "username",
                               "token",
                               "http://librato.example.com/");
        assert_eq!(3, backends.len());
    }
}
