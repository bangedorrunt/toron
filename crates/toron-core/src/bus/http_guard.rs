// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SSRF guard + feed caps (ADR-0003 security hygiene, Buzz `is_private_ip`
//! pattern). Blocks RFC1918, loopback, link-local, CGNAT, and IPv4-mapped-
//! IPv6 addresses so `call_webhook` / any HTTP fetch in toron cannot be
//! redirected at internal infrastructure. Feed queries are capped.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Maximum feed query limit (Buzz `FEED_MAX_LIMIT`).
pub const FEED_MAX_LIMIT: usize = 100;

/// Whether an IP address is RFC1918 private, loopback, link-local, CGNAT,
/// or an IPv4-mapped-IPv6 form of those. SSRF-unfriendly.
pub fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_private_v4(v4),
        IpAddr::V6(v6) => {
            // IPv4-mapped-IPv6 (::ffff:a.b.c.d) — check the embedded IPv4.
            if let Some(v4) = ipv4_mapped_to_v4(v6) {
                return is_private_v4(v4);
            }
            is_private_v6(v6)
        }
    }
}

/// Resolve `::ffff:a.b.c.d` (IPv4-mapped-IPv6) to the embedded IPv4.
fn ipv4_mapped_to_v4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    match v6.octets() {
        // ::ffff:0:0/96
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, a, b, c, d] => Some(Ipv4Addr::new(a, b, c, d)),
        _ => None,
    }
}

/// RFC1918 (10/8, 172.16/12, 192.168/16), loopback (127/8), link-local
/// (169.254/16), and CGNAT (100.64/10, RFC 6598).
fn is_private_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    match octets {
        // Loopback 127.0.0.0/8
        [127, ..] => true,
        // RFC1918 10.0.0.0/8
        [10, ..] => true,
        // RFC1918 172.16.0.0/12
        [172, b, ..] if (16..=31).contains(&b) => true,
        // RFC1918 192.168.0.0/16
        [192, 168, ..] => true,
        // Link-local 169.254.0.0/16
        [169, 254, ..] => true,
        // CGNAT 100.64.0.0/10 (RFC 6598)
        [100, b, ..] if (64..=127).contains(&b) => true,
        _ => false,
    }
}

/// Link-local IPv6 (fe80::/10) and unique-local (fc00::/7) + loopback (::1).
fn is_private_v6(ip: Ipv6Addr) -> bool {
    let octets = ip.octets();
    // Loopback ::1
    if ip == Ipv6Addr::LOCALHOST {
        return true;
    }
    // Link-local fe80::/10 → first byte fe or ff? fe80::/10 = 1111111010
    // first 10 bits: 0xfe80/16 covers it (0xFE80..0xFEBF).
    let first16 = u16::from_be_bytes([octets[0], octets[1]]);
    if (0xfe80..=0xfebf).contains(&first16) {
        return true;
    }
    // Unique-local fc00::/7 → first byte fc or fd.
    if octets[0] == 0xfc || octets[0] == 0xfd {
        return true;
    }
    false
}

/// Clamp a requested feed limit to [`FEED_MAX_LIMIT`].
pub fn clamp_feed_limit(limit: Option<usize>) -> usize {
    limit
        .map(|l| l.min(FEED_MAX_LIMIT))
        .unwrap_or(FEED_MAX_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    fn parse(s: &str) -> IpAddr {
        s.parse().expect("valid ip")
    }

    #[test]
    fn test_rfc1918_private() {
        for addr in [
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.0.1",
            "192.168.255.255",
        ] {
            assert!(is_private_ip(parse(addr)), "{addr} must be private");
        }
    }

    #[test]
    fn test_rfc1918_public_edges() {
        for addr in ["11.0.0.1", "172.15.0.1", "172.32.0.1", "192.169.0.1"] {
            assert!(!is_private_ip(parse(addr)), "{addr} must be public");
        }
    }

    #[test]
    fn test_loopback_private() {
        assert!(is_private_ip(parse("127.0.0.1")));
        assert!(is_private_ip(parse("127.0.0.0")));
    }

    #[test]
    fn test_link_local_private() {
        assert!(is_private_ip(parse("169.254.0.1")));
        assert!(is_private_ip(parse("169.254.255.254")));
    }

    #[test]
    fn test_cgnat_private() {
        // 100.64.0.0/10 (RFC 6598)
        assert!(is_private_ip(parse("100.64.0.1")));
        assert!(is_private_ip(parse("100.127.255.254")));
        // 100.63.x is NOT CGNAT.
        assert!(!is_private_ip(parse("100.63.0.1")));
        assert!(!is_private_ip(parse("100.128.0.1")));
    }

    #[test]
    fn test_public_v4() {
        for addr in ["8.8.8.8", "1.1.1.1", "93.184.216.34", "172.15.255.255"] {
            assert!(!is_private_ip(parse(addr)), "{addr} must be public");
        }
    }

    #[test]
    fn test_ipv4_mapped_ipv6_private() {
        // ::ffff:10.0.0.1 → RFC1918 private.
        assert!(is_private_ip(parse("::ffff:10.0.0.1")));
        assert!(is_private_ip(parse("::ffff:127.0.0.1")));
        assert!(is_private_ip(parse("::ffff:192.168.1.1")));
        // ::ffff:8.8.8.8 → public.
        assert!(!is_private_ip(parse("::ffff:8.8.8.8")));
    }

    #[test]
    fn test_ipv6_private_loopback_linklocal_ula() {
        assert!(is_private_ip(parse("::1")));
        assert!(is_private_ip(parse("fe80::1")));
        assert!(is_private_ip(parse("fe81::dead:beef")));
        assert!(is_private_ip(parse("fc00::1")));
        assert!(is_private_ip(parse("fd12:3456::1")));
    }

    #[test]
    fn test_ipv6_public() {
        for addr in [
            "2001:4860:4860::8888",
            "2606:4700:4700::1111",
            "2001:db8::1",
        ] {
            assert!(!is_private_ip(parse(addr)), "{addr} must be public");
        }
    }

    #[test]
    fn test_feed_limit_cap() {
        assert_eq!(clamp_feed_limit(Some(10)), 10);
        assert_eq!(clamp_feed_limit(Some(100)), 100);
        assert_eq!(clamp_feed_limit(Some(10_000)), FEED_MAX_LIMIT);
        assert_eq!(clamp_feed_limit(None), FEED_MAX_LIMIT);
        assert_eq!(FEED_MAX_LIMIT, 100);
    }

    /// Table-driven: blocked vs allowed URLs (acceptance criterion).
    #[test]
    fn test_blocked_allowed_table() {
        let blocked = [
            "http://10.0.0.1/x",
            "http://127.0.0.1:8080/x",
            "http://169.254.169.254/latest/meta-data/",
            "http://100.64.0.1/",
            "http://172.16.0.1/",
            "http://192.168.1.1/",
        ];
        for url in blocked {
            let host = url
                .split("://")
                .nth(1)
                .and_then(|rest| rest.split('/').next())
                .and_then(|h| h.split(':').next())
                .expect("host");
            let ip: IpAddr = host.parse().expect("valid host");
            assert!(is_private_ip(ip), "{url} host {host} must be blocked");
        }

        // Allowed literals (public IPv4 / IPv6; bracket the IPv6).
        let allowed = ["http://8.8.8.8/x", "http://[2001:4860:4860::8888]/x"];
        for url in allowed {
            let host = url
                .split("://")
                .nth(1)
                .and_then(|rest| rest.split('/').next())
                .and_then(|h| h.trim_start_matches('[').split(']').next())
                .expect("host");
            let ip: IpAddr = host.parse().expect("valid host");
            assert!(!is_private_ip(ip), "{url} host {host} must be allowed");
        }
    }
}
