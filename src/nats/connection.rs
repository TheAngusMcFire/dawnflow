//! Parsing of NATS connection strings into `async_nats::ConnectOptions`.
//!
//! async-nats accepts credentials embedded in the server URL but silently
//! ignores them during the handshake, and it has no way to configure TLS
//! material via the URL. This module closes that gap so a single connection
//! string is enough to reach an authenticated and/or TLS-protected server:
//!
//! ```text
//! nats://host:4222                         plain
//! nats://user:password@host:4222           username / password
//! nats://token@host:4222                   token (user without password)
//! tls://user:password@host:4222            TLS (system root certificates)
//! tls://host:4222?tls_ca=/etc/nats/ca.pem  TLS with a custom CA
//! tls://host:4222?tls_ca=ca.pem&tls_cert=client.pem&tls_key=client-key.pem
//!                                          mutual TLS
//! nats://host:4222?creds=/etc/nats/user.creds
//!                                          JWT + NKey credentials file
//! ```
//!
//! Query parameters:
//! - `tls_ca`: PEM file with root certificates to trust (replaces system roots)
//! - `tls_cert` / `tls_key`: PEM client certificate and key for mutual TLS
//! - `tls_first`: `true` to do the TLS handshake before the NATS INFO exchange
//! - `creds`: path to a NATS `.creds` file
//!
//! Any `tls_*` parameter forces TLS even with the `nats://` scheme. Special
//! characters in user names and passwords must be percent-encoded.
//!
//! Several servers of one cluster are separated by commas
//! (`nats://u:p@a:4222,nats://b:4222`); credentials and parameters go on the
//! first entry, later entries carrying any are refused.

use std::path::PathBuf;

use async_nats::{ConnectOptions, ServerAddr};
use percent_encoding::percent_decode_str;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionStringError {
    // `url::ParseError` never echoes the input, so credentials don't leak
    // into logs through this error
    #[error("invalid NATS connection string: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("unsupported NATS connection string scheme `{0}`, expected `nats` or `tls`")]
    UnsupportedScheme(String),
    #[error("NATS connection string has no host")]
    MissingHost,
    #[error("credentials in the NATS connection string are not valid percent-encoded UTF-8")]
    InvalidCredentials,
    #[error("unknown NATS connection string parameter `{0}`")]
    UnknownParameter(String),
    #[error("invalid value `{value}` for NATS connection string parameter `{param}`")]
    InvalidValue { param: String, value: String },
    #[error("NATS connection string parameters `tls_cert` and `tls_key` must be given together")]
    IncompleteClientCertificate,
    #[error("could not load NATS credentials file: {0}")]
    CredentialsFile(#[from] std::io::Error),
    #[error("credentials and parameters of a NATS server list belong on its first entry")]
    SettingsOnLaterServer,
}

/// Authentication taken from the user info part of the URL.
#[derive(Debug, PartialEq, Eq)]
enum Auth {
    None,
    UserPassword { user: String, password: String },
    Token(String),
}

/// A parsed connection string; the server address carries neither
/// credentials nor query parameters.
#[derive(Debug)]
struct ConnectionString {
    servers: Vec<Url>,
    auth: Auth,
    tls_required: bool,
    tls_first: bool,
    tls_ca: Option<PathBuf>,
    tls_client_cert: Option<(PathBuf, PathBuf)>,
    creds: Option<PathBuf>,
}

fn decode(value: &str) -> Result<String, ConnectionStringError> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|x| x.into_owned())
        .map_err(|_| ConnectionStringError::InvalidCredentials)
}

impl ConnectionString {
    fn parse(input: &str) -> Result<Self, ConnectionStringError> {
        let mut entries = input.split(',').map(str::trim).filter(|e| !e.is_empty());
        let first = entries.next().ok_or(ConnectionStringError::MissingHost)?;
        let mut parsed = Self::parse_one(first)?;
        for entry in entries {
            let later = Self::parse_one(entry)?;
            let plain = later.auth == Auth::None
                && !later.tls_first
                && later.tls_ca.is_none()
                && later.tls_client_cert.is_none()
                && later.creds.is_none();
            if !plain {
                return Err(ConnectionStringError::SettingsOnLaterServer);
            }
            parsed.servers.extend(later.servers);
        }
        Ok(parsed)
    }

    fn parse_one(input: &str) -> Result<Self, ConnectionStringError> {
        // keep accepting bare `host:port`, like async-nats does
        let mut url: Url = if input.contains("://") {
            input.parse()?
        } else {
            format!("nats://{input}").parse()?
        };

        let tls_scheme = match url.scheme() {
            "nats" => false,
            "tls" => true,
            other => return Err(ConnectionStringError::UnsupportedScheme(other.to_string())),
        };
        if url.host_str().is_none_or(str::is_empty) {
            return Err(ConnectionStringError::MissingHost);
        }

        let auth = match (url.username(), url.password()) {
            ("", None) => Auth::None,
            (token, None) => Auth::Token(decode(token)?),
            (user, Some(password)) => Auth::UserPassword {
                user: decode(user)?,
                password: decode(password)?,
            },
        };

        let mut tls_first = false;
        let mut tls_ca = None;
        let mut tls_cert = None;
        let mut tls_key = None;
        let mut creds = None;
        for (param, value) in url.query_pairs() {
            match param.as_ref() {
                "tls_ca" => tls_ca = Some(PathBuf::from(value.as_ref())),
                "tls_cert" => tls_cert = Some(PathBuf::from(value.as_ref())),
                "tls_key" => tls_key = Some(PathBuf::from(value.as_ref())),
                "tls_first" => {
                    tls_first =
                        value
                            .parse()
                            .map_err(|_| ConnectionStringError::InvalidValue {
                                param: param.to_string(),
                                value: value.to_string(),
                            })?
                }
                "creds" => creds = Some(PathBuf::from(value.as_ref())),
                other => return Err(ConnectionStringError::UnknownParameter(other.to_string())),
            }
        }
        let tls_client_cert = match (tls_cert, tls_key) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => return Err(ConnectionStringError::IncompleteClientCertificate),
        };
        let tls_required =
            tls_scheme || tls_first || tls_ca.is_some() || tls_client_cert.is_some();

        // strip everything async-nats doesn't need, so credentials never end
        // up in its server list (which it logs and reports in errors)
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        url.set_path("");

        Ok(Self {
            servers: vec![url],
            auth,
            tls_required,
            tls_first,
            tls_ca,
            tls_client_cert,
            creds,
        })
    }

    async fn apply(
        self,
        mut options: ConnectOptions,
    ) -> Result<(Vec<ServerAddr>, ConnectOptions), ConnectionStringError> {
        options = match self.auth {
            Auth::None => options,
            Auth::UserPassword { user, password } => options.user_and_password(user, password),
            Auth::Token(token) => options.token(token),
        };
        if let Some(creds) = self.creds {
            options = options.credentials_file(creds).await?;
        }
        if self.tls_required {
            install_crypto_provider();
            options = options.require_tls(true);
        }
        if self.tls_first {
            options = options.tls_first();
        }
        if let Some(ca) = self.tls_ca {
            options = options.add_root_certificates(ca);
        }
        if let Some((cert, key)) = self.tls_client_cert {
            options = options.add_client_certificate(cert, key);
        }
        // the scheme was validated in `parse`, so this cannot fail
        let servers = self
            .servers
            .into_iter()
            .map(ServerAddr::from_url)
            .collect::<Result<_, _>>()?;
        Ok((servers, options))
    }
}

/// rustls refuses to build a TLS config when several crypto providers are
/// compiled into the binary (e.g. another dependency enables `aws-lc-rs`)
/// and none was picked. Pick ring, the one async-nats is built with, unless
/// the application already chose one.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Parses `connection_string` and applies its credentials and TLS settings on
/// top of `options`. Returns the server addresses to pass to
/// [`ConnectOptions::connect`]. See the module docs for the accepted format.
pub async fn configure(
    connection_string: &str,
    options: ConnectOptions,
) -> Result<(Vec<ServerAddr>, ConnectOptions), ConnectionStringError> {
    ConnectionString::parse(connection_string)?
        .apply(options)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain() {
        let cs = ConnectionString::parse("nats://localhost:4222").unwrap();
        assert_eq!(cs.servers[0].as_str(), "nats://localhost:4222");
        assert_eq!(cs.auth, Auth::None);
        assert!(!cs.tls_required);
    }

    #[test]
    fn bare_host_port() {
        let cs = ConnectionString::parse("localhost:4222").unwrap();
        assert_eq!(cs.servers[0].as_str(), "nats://localhost:4222");
    }

    #[test]
    fn user_password_is_decoded_and_stripped() {
        let cs = ConnectionString::parse("nats://dawn%40store:p%3Ass%2Fw@nats.example:4222").unwrap();
        assert_eq!(
            cs.auth,
            Auth::UserPassword {
                user: "dawn@store".into(),
                password: "p:ss/w".into()
            }
        );
        assert_eq!(cs.servers[0].as_str(), "nats://nats.example:4222");
    }

    #[test]
    fn token() {
        let cs = ConnectionString::parse("nats://s3cr3t@localhost").unwrap();
        assert_eq!(cs.auth, Auth::Token("s3cr3t".into()));
    }

    #[test]
    fn tls_scheme() {
        let cs = ConnectionString::parse("tls://u:p@nats.example:4222").unwrap();
        assert!(cs.tls_required);
        assert_eq!(cs.servers[0].as_str(), "tls://nats.example:4222");
    }

    #[test]
    fn tls_params() {
        let cs = ConnectionString::parse(
            "nats://nats.example?tls_ca=/ca.pem&tls_cert=/c.pem&tls_key=/k.pem&tls_first=true",
        )
        .unwrap();
        assert!(cs.tls_required);
        assert!(cs.tls_first);
        assert_eq!(cs.tls_ca, Some("/ca.pem".into()));
        assert_eq!(cs.tls_client_cert, Some(("/c.pem".into(), "/k.pem".into())));
        assert_eq!(cs.servers[0].as_str(), "nats://nats.example");
    }

    #[test]
    fn server_lists_take_the_settings_of_the_first() {
        let cs = ConnectionString::parse("tls://u:p@a:4222, nats://b:4222,").unwrap();
        let servers: Vec<_> = cs.servers.iter().map(Url::as_str).collect();
        assert_eq!(servers, ["tls://a:4222", "nats://b:4222"]);
        assert!(cs.tls_required);
        assert!(matches!(
            ConnectionString::parse("nats://a:4222,http://b"),
            Err(ConnectionStringError::UnsupportedScheme(_))
        ));
        assert!(matches!(ConnectionString::parse(" , "), Err(ConnectionStringError::MissingHost)));
        assert!(matches!(
            ConnectionString::parse("nats://a:4222,nats://u:p@b:4222"),
            Err(ConnectionStringError::SettingsOnLaterServer)
        ));
    }

    #[test]
    fn rejects_bad_input() {
        assert!(matches!(
            ConnectionString::parse("http://localhost"),
            Err(ConnectionStringError::UnsupportedScheme(_))
        ));
        assert!(matches!(
            ConnectionString::parse("nats://localhost?tls_ca_file=x"),
            Err(ConnectionStringError::UnknownParameter(_))
        ));
        assert!(matches!(
            ConnectionString::parse("nats://localhost?tls_cert=x"),
            Err(ConnectionStringError::IncompleteClientCertificate)
        ));
        assert!(matches!(
            ConnectionString::parse("nats://localhost?tls_first=yes"),
            Err(ConnectionStringError::InvalidValue { .. })
        ));
    }
}
