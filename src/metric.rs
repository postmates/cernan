use metrics::statsd;
use chrono::{UTC, DateTime};

#[derive(PartialEq, Debug, Clone)]
pub enum MetricKind {
    Counter(f64),
    Gauge,
    Timer,
    Histogram,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Metric {
    pub kind: MetricKind,
    pub name: String,
    pub value: f64,
    pub time: DateTime<UTC>,
    pub source: Option<String>,
}

impl Metric {
    /// Create a new metric
    ///
    /// Uses the Into trait to allow both str and String types.
    pub fn new<S: Into<String>>(name: S, value: f64, kind: MetricKind) -> Metric {
        Metric::new_with_source(name, value, None, kind, None)
    }
    pub fn new_with_source<S: Into<String>>(name: S,
                                            value: f64,
                                            time: Option<DateTime<UTC>>,
                                            kind: MetricKind,
                                            source: Option<S>)
                                            -> Metric {
        Metric {
            name: name.into(),
            value: value,
            kind: kind,
            time: time.unwrap_or(UTC::now()),
            source: source.map(|x| x.into()),
        }
    }
    /// Valid message formats are:
    ///
    /// - `<str:metric_name>:<f64:value>|<str:type>`
    /// - `<str:metric_name>:<f64:value>|c|@<f64:sample_rate>`
    ///
    /// Multiple metrics can be sent in a single UDP packet
    /// separated by newlines.
    pub fn parse(source: &str) -> Option<Vec<Metric>> {
        statsd::parse_MetricPayload(source).ok()
    }
}

#[cfg(test)]
mod tests {
    use metric::{Metric, MetricKind};
    //    use test::Bencher; // see bench_prs

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "fst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c@2";
        let prs = Metric::parse(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[0].name, "fst");
        assert_eq!(prs_pyld[0].value, -1.1);

        assert_eq!(prs_pyld[1].kind, MetricKind::Gauge);
        assert_eq!(prs_pyld[1].name, "snd");
        assert_eq!(prs_pyld[1].value, 2.2);

        assert_eq!(prs_pyld[2].kind, MetricKind::Histogram);
        assert_eq!(prs_pyld[2].name, "thd");
        assert_eq!(prs_pyld[2].value, 3.3);

        assert_eq!(prs_pyld[3].kind, MetricKind::Counter(1.0));
        assert_eq!(prs_pyld[3].name, "fth");
        assert_eq!(prs_pyld[3].value, 4.0);

        assert_eq!(prs_pyld[4].kind, MetricKind::Counter(2.0));
        assert_eq!(prs_pyld[4].name, "fvth");
        assert_eq!(prs_pyld[4].value, 5.5);
    }

    #[test]
    fn test_metric_equal_in_name() {
        let res = Metric::parse("A=:1|ms\n").unwrap();

        assert_eq!("A=", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_slash_in_name() {
        let res = Metric::parse("A/:1|ms\n").unwrap();

        assert_eq!("A/", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        assert_eq!(None, Metric::parse(""));
    }

    #[test]
    fn test_metric_parse_invalid_no_value() {
        assert_eq!(None, Metric::parse("foo:"));
    }

    #[test]
    fn test_metric_multiple() {
        let res = Metric::parse("a.b:12.1|g\nb_c:13.2|c").unwrap();
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(12.1, res[0].value);

        assert_eq!("b_c", res[1].name);
        assert_eq!(13.2, res[1].value);
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec!["", "metric", "metric|11:", "metric|12", "metric:13|", ":|@", ":1.0|c"];
        for input in invalid.iter() {
            let result = Metric::parse(*input);
            assert!(result.is_none());
        }
    }

    // Benchmarking requires a nightly rust compiler as the 'test' crate is not
    // enabled by default. Dunno how to use feature flags yet so... gonna just
    // comment this hot mess out.
    //
    // My machine reports that bench_prs is done in 92ns (+/- 5ns). Not bad.

    // #[bench]
    // fn bench_prs(b: &mut Bencher) {
    //     let pyld = "zx4HyAkdvG7huGSnzo6Q8zTOvXIknvpamlhnvHsMMbk=:1|c\n\
    //                 Y4Bdiu/QXHxtIojiX9BoqkgYRJBb3XSjm+J3MBOIgrs=:1|ms\n\
    //                 RUiqIW2JBfcJ2GJtmk8IIomPfS3J6cnEvzywCM8cZe4=:1|g\n\
    //                 llWoPejpG64f92NiWPmfJAbWBhcmpO3XreJ6wpMqF0c=:1|ms\n\
    //                 P711QRWxqae6YFfLGJJSaoL0nQki7Lqw7+C7PZDrbDk=:1|c\n\
    //                 kI+3B99qj5v8qlgIxiGA3MbSzwIIZWHIjho1Pf4wfN0=:1|ms\n\
    //                 auaVrFU0SxVv3krrFcnFkO0HLA2fIrOGghISUuAYEI8=:1|ms\n\
    //                 oE8bmBQVneQ+EYQ1eENvhXpR7gVcIxEZALh44B/JTb0=:1|g\n\
    //                 PmvFGAaAzVeVSQOZ36yY2Z9PjuGoLzp87Ws1GbLMWUk=:1|h\n\
    //                 gDan61yoL3Y2iClroFOgtrCoykMeWSxyzCgpleRXWf4=:1|g\n\
    //                 8MhQyM7iQe2M+oOKHQDUoxemDHpt/oxU9AA1XIhDMaQ=:1|h\n\
    //                 awMrx0B3xGUtjCokvuflg+vX7uKtEenx0FN1jNTYevQ=:1|g\n\
    //                 6SChxeCuYHI6zKaOwa5FZ65nxY+MwVMl4PaxEsVabrw=:1|ms\n\
    //                 h75DU0EjnCoqlfI4wOfPMFohol9WjxK7oJWQLMEoPfE=:1|c\n\
    //                 O+daVVqZG2PoJVULhNGZDqbT9wGHwWkjM/JEDrek+fs=:1|g\n\
    //                 8ugoT+52Mpi2Gai/rfJFJdkxsQ6r6Vc4PUQRCd5hwXo=:1|ms\n\
    //                 1Z2nFxByzEdQWRUFzO0AqyGicLJ9VhjGw5suKYAwX14=:1|g\n\
    //                 s6p3H6tycu373vqGQUznWcYqxPvSnydUKnFO5FpFpw0=:1|g\n\
    //                 Y91xSYI5PbOnHW47UQd2zp5nQoVXmyXnmUjeEoenQuM=:1|ms\n\
    //                 wUDIBy7yHaY/483FumSTJgy56oRkfK1jUQLV1ZY08YY=:1|g\n\
    //                 0uNciXHoQQNSxFdkTLmD/ur1S++xIqTZihUgaN6fXF4=:1|c\n\
    //                 X5UoRnAP+4mJu3d5ewZrIPIc1kSUJiuyjS11tiZ+W1M=:1|h\n\
    //                 0weP/1Jps89hWXQblUCXrox/QEKrC79o7Ev1HOQf/Co=:1|ms\n\
    //                 354hZbr2jbbEhrszmhZkw0SRZQjOVM/XSRZX5roUcco=:1|g\n\
    //                 +e1LX1c7TW0WyhZi5nxMXUdxHTKV7Yo5JtL0iKVXYic=:1|ms\n\
    //                 jHTgSscT9oHbVTdDkZ0vEb6oKYlx6WLRdWYexhbqqJs=:1|c\n\
    //                 AkPhryt82Bmv0mgJeAX4+woajfoSWXpg6AnnwN5MyjA=:1|g\n\
    //                 wK6tk2hQR7TAwzwg7tQtfnMkhvribiFmpLriFL3M4X8=:1|c\n\
    //                 VJQ5NIZff5hRR1KGPmxWl43zT7ma6pVHN5U7/nSRGLM=:1|h\n\
    //                 JmSjP/7xdCe7PHOevRBqGV0Dwn7wzjcd66rAAA586Js=:1|h\n\
    //                 D+3Kl+Wqqm1GFceIfhwE+ZZKmxSRF43Sf0cy9LS56Wo=:1|h\n\
    //                 EKMY1NJN8SEtarCNkU1/i4BUndegetyP1mCi/6tnkYw=:1|h\n\
    //                 yP82/VEGAvWv3YAW2l6Y4H/3ldSeHDM85rRnE0SwCK8=:1|h\n\
    //                 uSMgywUHDPLXE8QETushJgX/+H6f6+65NJiJeyQ+YjI=:1|ms\n\
    //                 3k5T7LBioOj4nV73e/bOp/fKMcjNGBRjyWoE2H6x9Ow=:1|c\n\
    //                 D0t7MH6/SmB7qZqP5gziDEjYHs67xhH/BgIWMPNHfDE=:1|g\n\
    //                 robrcyfL3UizvdZDhKi3zSpz/DywA8FOP+zASCK+6Dk=:1|h\n\
    //                 s+pDfjNbOUsy0o/TBsPgHCE/E2mmQsHK8Y3sWxgAa+Y=:1|ms\n\
    //                 h9rwJsIbH2GdOT6Xu9GUw21VwS1gkc8ImVH49QlMA/w=:1|h\n\
    //                 qTAgmPEycTwfxNLWNqfMgxhntmSah8zZbWPU2/JJ6Hk=:1|ms\n\
    //                 BkMY8URiNnve9bAFW2shRrcnhHCRdiSoQm+hXhiTVvw=:1|g\n\
    //                 wYvy59tY6U6mIxbF/qXDJcrF5xFC2qoYpsgG5W7qlrs=:1|g\n\
    //                 cRQ6pQiTTzXSUSCrUsDrQsRObJB3yHnUYjukS+3IeYk=:1|g\n\
    //                 TN9Hdl6DVWwB16EsD7ISLtLFlyriyDJoSWIRVQpPVeo=:1|ms\n\
    //                 4xDNemA8wpV/KKSsGLtyu2fPSnRduMqGxjC96/B5maM=:1|ms\n\
    //                 PhRyO0jH1bIwmc41dyxR4+JWUTM/UOTRIG1W1teaxCo=:1|c\n\
    //                 +RKV6Sop3WWT4CdX9zjqGuX8aR+cQvOaqp+MsQzeWKc=:1|c\n\
    //                 o7YLSwIjozy0Sbc2eWLvxtWHFcdgQo95dk9FPYxc5ec=:1|ms\n\
    //                 t8eUeerzyR8NdAZCgaXoLW7iSGTQW5n6IYn19/ygQlk=:1|h\n\
    //                 Jiv4SlitOgtMZO8trj9Z+ISK/9eomrLEy8A0/+RzNO8=:1|h\n\
    //                 vzKzIbVNa+JAniJ0VzmfdhH2sb7hig8+bkYs81aEj8k=:1|c\n\
    //                 DKi4McWiyBlt4BaGNaM6aE1NDk1F6S65I/9RfjXk5v0=:1|ms\n\
    //                 vgqITgM+DHJmtBAtzQCSsgk7Ls8lIQMMa3f4Xz+udTw=:1|c\n\
    //                 7rREqAsT2sEnS52hBA7Q/2UOyCX/6VS7SmILgGwT5sk=:1|c\n\
    //                 zh0d1MbgzTRPD7Kuqp5ooLfjdteGs4KsZdqWPNvA4+Y=:1|c\n\
    //                 fpPAvxVD35KoaW2GYqn1KMvwUJlta979vZl35NXEfgk=:1|h\n\
    //                 ZO6SosbTOG0p4AmABKgUBj0RCfLLhgQTUpBbfbIPpmE=:1|c\n\
    //                 1kNmuWejzBKSBg7HXMPwRCpQmm07BgvcUn+D5iixbtY=:1|h\n\
    //                 55ehcp2TCN/kqDm+fgyp1+KqxxAAXyEOMn7p0w3XsbY=:1|h\n\
    //                 vZtRxtrJ0sqzwTgSFIQezMtGy793kl35dsZPSXL0y8k=:1|c\n\
    //                 I2mkKXd1wD+wDHFU8EjoVOAQtJxGPKyerpJfu0/QqBg=:1|h\n\
    //                 q0SJKKp5AOh75hit5h1S+mqt8igVw41HgEDVQ2BpPFs=:1|ms\n\
    //                 2L8IU/jsCpRz2m+oTdzLTl8BNhYty0/kIZaCdGKQUgs=:1|c\n\
    //                 LFH8cHHyQACeHlWeqqEmZSW6f71IDu2Vp6k7oAga+R8=:1|h\n\
    //                 MnvnNCF2Ue+NEyM/KGuSkQUcZimqafzIQUJ8hq7P2gA=:1|g\n\
    //                 nxmLv87CwglPXxYZY8/BWMedQvpu4zdGcdgd0iiLIcc=:1|h\n\
    //                 WR6SG0WXJ7Viz00u7ABuqqFZQC9xw0qGqkE6IBKxcmY=:1|c\n\
    //                 XRzDqE3gS/DnYjShhj+j8PNVm/awAXrchgDJt4jMBmk=:1|g\n\
    //                 RGsDIhnYn0L5VXY4RvamIR3lBJhuKaaMteMmEpPEHJM=:1|ms\n\
    //                 2KvUfKtOb2N+9309VIvDYGau5wm6lCj8rlInEQLYloM=:1|c\n\
    //                 0xzfkjzDSdq27Wf8/BjphGQSpewXCLdehAkyl/EiZNU=:1|c\n\
    //                 aG/MehOevcIbW0JEmuCFV24TJXG6ig7rkOOMEXbaZaQ=:1|c\n\
    //                 kxGitiVkhY6COUoHkVJcPdp0kXeIriHOY/G3yGhAx+8=:1|ms\n\
    //                 BLkmbzEPpcm+q5HOwPfBP5DbcegKIn/TtkJ7r0tiMts=:1|ms\n";
    //     b.iter(|| Metric::parse(pyld));
    // }
}
