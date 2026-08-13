"""
polarysdb.modules.crypto
AES-256-GCM encryption and decryption utilities compatible with Go crypto package.
Format: [12 bytes nonce][N bytes ciphertext + 16 bytes tag]
"""

from __future__ import annotations

import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_SIZE = 12


def encrypt(payload: bytes, key: bytes | None) -> bytes:
    """
    Encrypts payload using AES-256-GCM.
    If key is None or empty/zero-value, returns payload unencrypted.
    """
    if not key or not payload:
        return payload

    aesgcm = AESGCM(key)
    nonce: bytes = os.urandom(NONCE_SIZE)
    ciphertext = aesgcm.encrypt(nonce, payload, None)
    return nonce + ciphertext


def decrypt(data: bytes, key: bytes | None) -> bytes:
    """
    Decrypts AES-256-GCM data (nonce || ciphertext).
    If key is None or empty/zero-value, returns data as-is.
    """
    if not key or not data:
        return data

    if len(data) < NONCE_SIZE + 16:
        raise ValueError("data too short for AES-GCM decryption")

    nonce: bytes = data[:NONCE_SIZE]
    ciphertext: bytes = data[NONCE_SIZE:]

    aesgcm = AESGCM(key)
    try:
        return aesgcm.decrypt(nonce, ciphertext, None)
    except Exception as exc:
        raise ValueError("decryption failed — wrong key or corrupted data") from exc