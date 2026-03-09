// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Ed25519 key management and event signing for stophammer nodes.
//!
//! [`NodeSigner`] loads a 32-byte ed25519 signing key from disk, or generates
//! and persists one on first run (Unix: mode 0o600). Signing computes
//! SHA-256 over the canonical JSON serialisation of [`EventSigningPayload`]
//! and produces an ed25519 signature stored as a hex string.
//!
//! [`verify_event_signature`] reconstructs the same payload from an [`Event`]
//! and verifies the signature without requiring access to a `NodeSigner`.

use std::fmt;
use ed25519_dalek::{Signer, Verifier, SigningKey, VerifyingKey, Signature};
use rand_core::OsRng;
use sha2::{Sha256, Digest};
use crate::event::{EventSigningPayload, EventType, Event};

/// Holds the node's ed25519 signing key and its hex-encoded public key.
pub struct NodeSigner {
    signing_key: SigningKey,
    pubkey_hex:  String,
}

/// Errors returned by key-management and signature-verification operations.
pub enum SigningError {
    /// An I/O error occurred reading or writing the key file, or decoding
    /// a hex-encoded public key or signature from an event.
    Io(std::io::Error),
    /// The ed25519 key material is structurally invalid, or the signature
    /// does not verify against the reconstructed signing payload.
    InvalidKey(ed25519_dalek::SignatureError),
}

impl fmt::Debug for SigningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for SigningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SigningError::Io(e)         => write!(f, "IO error: {e}"),
            SigningError::InvalidKey(e) => write!(f, "Invalid key: {e}"),
        }
    }
}

impl From<std::io::Error> for SigningError {
    fn from(e: std::io::Error) -> Self {
        SigningError::Io(e)
    }
}

impl From<ed25519_dalek::SignatureError> for SigningError {
    fn from(e: ed25519_dalek::SignatureError) -> Self {
        SigningError::InvalidKey(e)
    }
}

impl std::error::Error for SigningError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SigningError::Io(e)         => Some(e),
            SigningError::InvalidKey(e) => Some(e),
        }
    }
}

impl NodeSigner {
    /// Loads a 32-byte ed25519 signing key from `path`, or generates one on first run.
    ///
    /// A newly generated key is written to `path` with Unix permissions 0o600.
    ///
    /// # Errors
    ///
    /// Returns [`SigningError::Io`] if the key file cannot be read, written,
    /// or if the file does not contain exactly 32 bytes (the raw bytes are
    /// `try_into`-converted and the length mismatch is mapped to an
    /// `InvalidData` I/O error).
    pub fn load_or_create(path: &str) -> Result<Self, SigningError> {
        let signing_key = if std::path::Path::new(path).exists() {
            let bytes = std::fs::read(path)?;
            let arr: [u8; 32] = bytes.try_into()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "key file must be 32 bytes"))?;
            SigningKey::from_bytes(&arr)
        } else {
            let key = SigningKey::generate(&mut OsRng);
            std::fs::write(path, key.to_bytes())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o600);
                std::fs::set_permissions(path, perms)?;
            }
            key
        };

        // VerifyingKey can be derived from SigningKey at any point via
        // signing_key.verifying_key(), so there is no need to store it.
        let pubkey_hex = hex::encode(signing_key.verifying_key().to_bytes());

        Ok(Self { signing_key, pubkey_hex })
    }

    /// Returns the hex-encoded ed25519 public key for this node.
    pub fn pubkey_hex(&self) -> &str {
        &self.pubkey_hex
    }

    /// Signs the event fields and returns `(pubkey_hex, signature_hex)`.
    ///
    /// The signature covers SHA-256 of the canonical JSON serialisation of
    /// [`EventSigningPayload`] built from the supplied fields.
    ///
    /// # Panics
    ///
    /// Panics if [`EventSigningPayload`] cannot be serialised to JSON. This
    /// is a programming error — the type is always serialisable.
    pub fn sign_event(
        &self,
        event_id:     &str,
        event_type:   &EventType,
        payload_json: &str,
        subject_guid: &str,
        created_at:   i64,
    ) -> (String, String) {
        let payload = EventSigningPayload {
            event_id,
            event_type,
            payload_json,
            subject_guid,
            created_at,
        };
        let serialized = serde_json::to_string(&payload).expect("EventSigningPayload serialization failed");
        let digest = Sha256::digest(serialized.as_bytes());
        let sig: Signature = self.signing_key.sign(&digest);
        (self.pubkey_hex.clone(), hex::encode(sig.to_bytes()))
    }
}

/// Verifies the ed25519 signature on `event` using the pubkey in `event.signed_by`.
///
/// This is a free function rather than a `NodeSigner` method because it
/// verifies events signed by *any* node — it does not require access to
/// this node's private key.
///
/// # Errors
///
/// Returns [`SigningError::Io`] if `event.signed_by` or `event.signature`
/// are not valid hex, or if either decodes to the wrong byte length.
/// Returns [`SigningError::InvalidKey`] if the public key bytes are
/// structurally invalid or if the signature does not verify.
pub fn verify_event_signature(event: &Event) -> Result<(), SigningError> {
    let pubkey_bytes = hex::decode(&event.signed_by)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let pubkey_arr: [u8; 32] = pubkey_bytes.try_into()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "pubkey must be 32 bytes"))?;
    let verifying_key = VerifyingKey::from_bytes(&pubkey_arr)?;

    // Use the canonical payload_json string set at sign time.
    // Re-serializing through EventPayload (an internally-tagged enum) would
    // round-trip through serde_json::Value, which sorts object keys
    // alphabetically — producing a different digest than the original struct
    // serialisation that preserves declaration order.
    if event.payload_json.is_empty() {
        return Err(SigningError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "event.payload_json is empty — populate it before calling verify_event_signature",
        )));
    }

    let payload = EventSigningPayload {
        event_id:     &event.event_id,
        event_type:   &event.event_type,
        payload_json: &event.payload_json,
        subject_guid: &event.subject_guid,
        created_at:   event.created_at,
    };
    let serialized = serde_json::to_string(&payload)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let digest = Sha256::digest(serialized.as_bytes());

    let sig_bytes = hex::decode(&event.signature)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    let sig_arr: [u8; 64] = sig_bytes.try_into()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "signature must be 64 bytes"))?;
    let sig = Signature::from_bytes(&sig_arr);

    verifying_key.verify(&digest, &sig)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventPayload, EventType, ArtistUpsertedPayload};
    use crate::model::Artist;

    #[test]
    fn sign_verify_roundtrip() {
        let signer = NodeSigner::load_or_create("/tmp/sign-roundtrip.key").unwrap();

        let artist = Artist {
            artist_id:  "artist-1".into(),
            name:       "Test Artist".into(),
            name_lower: "test artist".into(),
            created_at: 1_000_000,
        };

        let inner = ArtistUpsertedPayload { artist };
        let payload_json = serde_json::to_string(&inner).unwrap();

        let (signed_by, signature) = signer.sign_event(
            "evt-1", &EventType::ArtistUpserted, &payload_json, "subj-1", 9999,
        );

        let event = crate::event::Event {
            event_id:     "evt-1".into(),
            event_type:   EventType::ArtistUpserted,
            payload:      EventPayload::ArtistUpserted(inner),
            payload_json: payload_json.clone(),
            subject_guid: "subj-1".into(),
            signed_by,
            signature,
            seq:        1,
            created_at: 9999,
            warnings:   vec![],
        };

        verify_event_signature(&event).expect("signature verification failed");
    }
}
