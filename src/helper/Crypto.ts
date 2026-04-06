import { createHmac, Hmac } from "crypto";
import { ensureStringLength } from "./utils.js";

/**
 * Derives a 32-byte AES-256 key from password.
 */
export function deriveKey(password: string): Buffer {
    let key = Buffer.from(password, 'utf8');
    if (key.length !== 16 && key.length !== 24 && key.length !== 32) {
        key = Buffer.from(ensureStringLength(password, 32), 'utf8');
    }
    return key;
}

/**
 * Creates HMAC-SHA256 instance for integrity verification.
 */
export function createHmacSha256(key: Buffer): Hmac {
    return createHmac('sha256', key as any);
}
