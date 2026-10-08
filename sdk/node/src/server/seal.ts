// Запечатанные значения состояния: {"$sealed": "v1.<eph>.<nonce>.<ct>"} (base64url без паддинга).
// eph — одноразовый открытый X25519; ключ = HKDF-SHA256(ECDH(eph, ключ агента), salt пусто,
// info "agent sealed v1"); шифр AES-256-GCM (nonce 12 байт, AAD пусто), ct — с тегом.
// Раскрывает только агент (своим закрытым ключом) перед передачей воркеру.
import {
  createCipheriv,
  createDecipheriv,
  createPrivateKey,
  createPublicKey,
  diffieHellman,
  generateKeyPairSync,
  hkdfSync,
  randomBytes,
  type KeyObject,
} from "node:crypto";

/** Запечатанное значение в снимке состояния. */
export interface Sealed {
  $sealed: string;
}

const PREFIX = "v1";
const INFO = "agent sealed v1";
const KEY_LEN = 32;
const NONCE_LEN = 12;
const TAG_LEN = 16;

const PKCS8_X25519 = Buffer.from("302e020100300506032b656e04220420", "hex");

const b64url = (b: Buffer) => b.toString("base64url");

/** Открытый ключ X25519 из сырых 32 байт. */
function publicKeyFromRaw(raw: Buffer): KeyObject {
  if (raw.length !== KEY_LEN) throw new Error(`ключ X25519 — ${KEY_LEN} байта, получено ${raw.length}`);
  return createPublicKey({ key: { kty: "OKP", crv: "X25519", x: b64url(raw) }, format: "jwk" });
}

/** Закрытый ключ X25519 из сырых 32 байт (PKCS#8: префикс X25519 + ключ). */
function privateKeyFromRaw(raw: Buffer): KeyObject {
  if (raw.length !== KEY_LEN) throw new Error(`закрытый ключ X25519 — ${KEY_LEN} байта`);
  return createPrivateKey({ key: Buffer.concat([PKCS8_X25519, raw]), format: "der", type: "pkcs8" });
}

/** Ключ шифра из общего секрета ECDH. */
function deriveKey(shared: Buffer): Buffer {
  return Buffer.from(hkdfSync("sha256", shared, Buffer.alloc(0), INFO, KEY_LEN));
}

/**
 * Запечатать value (любой JSON) открытым ключом агента (base64, 32 байта —
 * hello.agent.encryptionKey). Ключ некорректен — ошибка.
 */
export function seal(encryptionKey: string, value: unknown): Sealed {
  const agentKey = publicKeyFromRaw(Buffer.from(encryptionKey, "base64"));
  const eph = generateKeyPairSync("x25519");
  const ephRaw = Buffer.from(eph.publicKey.export({ format: "jwk" }).x!, "base64url");
  const key = deriveKey(diffieHellman({ privateKey: eph.privateKey, publicKey: agentKey }));
  const nonce = randomBytes(NONCE_LEN);
  const cipher = createCipheriv("aes-256-gcm", key, nonce);
  const plain = Buffer.from(JSON.stringify(value === undefined ? null : value), "utf8");
  const ct = Buffer.concat([cipher.update(plain), cipher.final(), cipher.getAuthTag()]);
  return { $sealed: [PREFIX, b64url(ephRaw), b64url(nonce), b64url(ct)].join(".") };
}

/**
 * @internal Раскрыть запечатанное значение закрытым ключом агента (сырые 32 байта, base64) —
 * для проверки совместимости; в работе раскрывает агент.
 */
export function unseal(privateKey: string, sealed: string): unknown {
  const parts = sealed.split(".");
  if (parts.length !== 4 || parts[0] !== PREFIX) throw new Error("не запечатанное значение v1");
  const [, ephS, nonceS, ctS] = parts;
  const eph = publicKeyFromRaw(Buffer.from(ephS, "base64url"));
  const priv = privateKeyFromRaw(Buffer.from(privateKey, "base64"));
  const key = deriveKey(diffieHellman({ privateKey: priv, publicKey: eph }));
  const nonce = Buffer.from(nonceS, "base64url");
  const ct = Buffer.from(ctS, "base64url");
  if (nonce.length !== NONCE_LEN || ct.length < TAG_LEN) throw new Error("повреждённое запечатанное значение");
  const decipher = createDecipheriv("aes-256-gcm", key, nonce);
  decipher.setAuthTag(ct.subarray(ct.length - TAG_LEN));
  const plain = Buffer.concat([decipher.update(ct.subarray(0, ct.length - TAG_LEN)), decipher.final()]);
  return JSON.parse(plain.toString("utf8"));
}

/** @internal Открытый ключ (base64, 32 байта) по закрытому (base64, 32 байта) — для тестов. */
export function publicKeyOf(privateKey: string): string {
  const priv = privateKeyFromRaw(Buffer.from(privateKey, "base64"));
  return Buffer.from(createPublicKey(priv).export({ format: "jwk" }).x!, "base64url").toString("base64");
}
