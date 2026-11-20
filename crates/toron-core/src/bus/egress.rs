// governed-by: ADR-0039 U4 (KTD5)
#![forbid(unsafe_code)]

//! One SSRF-safe egress decision for every toron HTTP callback (U4).
//!
//! Pure: URL validation, host allowlist, resolved-address check with an
//! allow-CIDR carve-out, and a body cap. The actual socket connect is a port:
//! the caller MUST connect to the `SocketAddr` returned by `check_resolved`,
//! with SNI/Host from the original name and redirects disabled (KTD5 steps
//! 5–7). No new dependency: CIDR matching is a plain prefix compare.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::http_guard::is_private_ip;

/// An allowlisted CIDR (IPv4 or IPv6), matched by prefix bits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cidr {
    base: IpAddr,
    prefix: u8,
}

impl Cidr {
    pub fn parse(s: &str) -> Result<Self, String> {
        let (addr, bits) = s
            .split_once('/')
            .ok_or_else(|| format!("CIDR without prefix: {s}"))?;
        let base: IpAddr = addr
            .parse()
            .map_err(|e| format!("bad CIDR address {addr}: {e}"))?;
        let max = match base {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        let prefix: u8 = bits
            .parse()
            .map_err(|e| format!("bad CIDR prefix {bits}: {e}"))?;
        if prefix > max {
            return Err(format!("CIDR prefix {prefix} exceeds {max}"));
        }
        Ok(Self { base, prefix })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.base, ip) {
            (IpAddr::V4(a), IpAddr::V4(b)) => {
                let mask = mask_v4(self.prefix);
                (u32::from(a) & mask) == (u32::from(b) & mask)
            }
            (IpAddr::V6(a), IpAddr::V6(b)) => {
                let mask = mask_v6(self.prefix);
                let (aa, bb) = (a.octets(), b.octets());
                for i in 0..16 {
                    if (aa[i] & mask[i]) != (bb[i] & mask[i]) {
                        return false;
                    }
                }
                true
            }
            // An IPv4-mapped IPv6 address is compared as the embedded v4 so a
            // 100.64/10 carve-out cannot be slipped through a mapped form.
            (IpAddr::V4(_), IpAddr::V6(b)) => {
                mapped_v4(b).is_some_and(|m| self.contains(IpAddr::V4(m)))
            }
            (IpAddr::V6(_), IpAddr::V4(_)) => false,
        }
    }
}

fn mask_v4(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn mask_v6(prefix: u8) -> [u8; 16] {
    let mut out = [0u8; 16];
    let whole = (prefix / 8) as usize;
    for i in 0..whole {
        out[i] = 0xff;
    }
    if whole < 16 && prefix % 8 != 0 {
        out[whole] = 0xffu8.wrapping_shl(8 - u32::from(prefix % 8));
    }
    out
}

fn mapped_v4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    match v6.octets() {
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, a, b, c, d] => Some(Ipv4Addr::new(a, b, c, d)),
        _ => None,
    }
}

/// The one egress policy. Defaults allow only the operator's tailnet, per KTD5.
#[derive(Clone, Debug)]
pub struct EgressPolicy {
    /// Hostname suffixes allowed to receive callbacks (leading dot = suffix).
    pub allow_hosts: Vec<String>,
    /// Private ranges that are explicitly allowed despite `is_private_ip`.
    pub allow_cidrs: Vec<Cidr>,
    pub max_body_bytes: usize,
    pub connect_timeout_ms: u64,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self {
            allow_hosts: vec![".example-tailnet.ts.net".to_string()],
            allow_cidrs: vec![
                Cidr::parse("100.64.0.0/10").expect("constant CIDR"),
                Cidr::parse("fd7a:115c:a1e0::/48").expect("constant CIDR"),
            ],
            max_body_bytes: 1_048_576,
            connect_timeout_ms: 3_000,
        }
    }
}

/// A validated callback target. `host` is the ORIGINAL name (for SNI/Host);
/// the caller connects only to the `SocketAddr` chosen by `check_resolved`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub host: String,
    pub port: u16,
}

fn host_allowed(host: &str, policy: &EgressPolicy) -> bool {
    let host = host.to_ascii_lowercase();
    policy.allow_hosts.iter().any(|suffix| {
        let s = suffix.to_ascii_lowercase();
        if let Some(base) = s.strip_prefix('.') {
            host == base || host.ends_with(&format!(".{base}"))
        } else {
            host == s
        }
    })
}

/// Validate a callback URL: https only, no userinfo, allowlisted host, no
/// fragment. Returns the target the caller must resolve.
pub fn validate_target(url: &str, policy: &EgressPolicy) -> Result<Target, String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or("callback must be https")?;
    if rest.contains('#') {
        return Err("callback URL must not carry a fragment".into());
    }
    let authority = rest.split('/').next().unwrap_or("");
    if authority.contains('@') {
        return Err("callback URL must not carry userinfo".into());
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            let port: u16 = p.parse().map_err(|_| "callback port out of range")?;
            (h, port)
        }
        _ => (authority, 443),
    };
    if host.is_empty() || !host_allowed(host, policy) {
        return Err(format!("callback host {host:?} is not allowlisted"));
    }
    Ok(Target {
        host: host.to_string(),
        port,
    })
}

/// Check every resolved address. A private address is refused UNLESS an
/// allow-CIDR explicitly carves it out; the first allowed address is returned
/// as the sole socket the caller may connect to.
pub fn check_resolved(addrs: &[IpAddr], policy: &EgressPolicy) -> Result<IpAddr, String> {
    let allowed: Vec<IpAddr> = addrs
        .iter()
        .copied()
        .filter(|ip| !is_private_ip(*ip) || policy.allow_cidrs.iter().any(|c| c.contains(*ip)))
        .collect();
    allowed
        .first()
        .copied()
        .ok_or_else(|| "no resolved callback address passes the egress guard".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ip(s: &str) -> IpAddr {
        s.parse().expect("ip")
    }

    #[test]
    fn ae3_a_host_outside_the_allowlist_is_refused() {
        let p = EgressPolicy::default();
        assert!(validate_target("https://evil.example/hook", &p).is_err());
        assert!(validate_target("https://foo.example-tailnet.ts.net/hook", &p).is_ok());
    }

    #[test]
    fn tailnet_cidr_is_allowed_but_metadata_and_loopback_are_not() {
        let p = EgressPolicy::default();
        assert_eq!(
            check_resolved(&[ip("100.64.2.3")], &p).unwrap(),
            ip("100.64.2.3")
        );
        assert!(check_resolved(&[ip("169.254.169.254")], &p).is_err());
        assert!(check_resolved(&[ip("127.0.0.1")], &p).is_err());
        assert!(check_resolved(&[ip("192.168.1.5")], &p).is_err());
    }

    #[test]
    fn ipv4_mapped_loopback_is_refused() {
        let p = EgressPolicy::default();
        assert!(check_resolved(&[ip("::ffff:127.0.0.1")], &p).is_err());
    }

    #[test]
    fn http_userinfo_and_fragment_are_refused() {
        let p = EgressPolicy::default();
        assert!(validate_target("http://foo.example-tailnet.ts.net/h", &p).is_err());
        assert!(validate_target("https://u:p@foo.example-tailnet.ts.net/h", &p).is_err());
        assert!(validate_target("https://foo.example-tailnet.ts.net/h#x", &p).is_err());
    }

    #[test]
    fn check_picks_only_the_checked_address_rebinding_still_safe() {
        let p = EgressPolicy::default();
        // A resolver that returns an allowed and a disallowed address: the
        // guard returns exactly the allowed one; the caller connects there.
        // A later re-resolve is irrelevant because the caller never repeats it.
        let chosen = check_resolved(&[ip("169.254.169.254"), ip("100.64.0.9")], &p).unwrap();
        assert_eq!(chosen, ip("100.64.0.9"));
    }

    #[test]
    fn a_private_host_needing_an_allow_cidr_without_one_is_refused() {
        let mut p = EgressPolicy::default();
        p.allow_cidrs.clear();
        assert!(check_resolved(&[ip("100.64.2.3")], &p).is_err());
    }
}
