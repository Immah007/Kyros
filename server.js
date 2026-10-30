const express = require("express");
const { Pool } = require("pg");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs");
const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");
const { OAuth2Client } = require("google-auth-library");
const nodemailer = require("nodemailer");

const app = express();
app.set("trust proxy", true);
app.disable("x-powered-by");
app.use(express.json({ limit: "128kb" }));

const PORT = process.env.PORT || 3000;

/* ============================================================
   DATABASE
   ============================================================ */

const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const IS_KNOWN_PRODUCTION_DATABASE = DATABASE_URL.includes(
  "ep-weathered-rice-ax710zy0-pooler.c-4.us-east-2.aws.neon.tech"
);
const pool = new Pool({
  ...(DATABASE_URL ? { connectionString: DATABASE_URL } : {
    host: "ep-weathered-rice-ax710zy0-pooler.c-4.us-east-2.aws.neon.tech",
    port: 5432,
    database: "neondb",
    user: "neondb_owner",
    password: "npg_oczfV0eqx6mh",
  }),
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

const PUBLIC_DIR = path.join(__dirname, "public");
const RESOURCES_DIR = path.join(__dirname, "resources");
const ACTIVE_TIMEOUT_SECONDS = 180;

/* ============================================================
   REMOTE CAMERA CONFIG
   ============================================================ */

// The user never types any URL. The apps call this backend internally and
// exchange only the short 6-digit connection code.
//
// Waiting codes expire quickly. Once a camera joins, the code is invalidated
// and the authenticated WebRTC signaling session can remain alive longer.
const REMOTE_CAMERA_CODE_TTL_MINUTES = Number(
  process.env.REMOTE_CAMERA_CODE_TTL_MINUTES || 10
);

const REMOTE_CAMERA_SESSION_TTL_HOURS = Number(
  process.env.REMOTE_CAMERA_SESSION_TTL_HOURS || 12
);

// ICE / STUN / TURN
//
// KyroS uses Google's public STUN service by default so Internet Remote Camera
// can attempt direct peer-to-peer connectivity immediately with no extra setup.
//
// Google public STUN does NOT provide a general free TURN relay.
// TURN can therefore be added later without changing the app.
//
// STUN_URLS can override the defaults.
//
// Example:
// STUN_URLS=stun:stun.l.google.com:19302,stun:stun1.l.google.com:19302

const DEFAULT_STUN_URLS = [
  "stun:stun.l.google.com:19302",
  "stun:stun1.l.google.com:19302",
  "stun:stun2.l.google.com:19302",
  "stun:stun3.l.google.com:19302",
  "stun:stun4.l.google.com:19302",
];

function csvEnv(name) {
  return String(process.env[name] || "")
    .split(",")
    .map(v => v.trim())
    .filter(Boolean);
}

const STUN_URLS = csvEnv("STUN_URLS");

const EFFECTIVE_STUN_URLS =
  STUN_URLS.length > 0
    ? STUN_URLS
    : DEFAULT_STUN_URLS;

/*
  OPTION A
  --------
  KyroS-owned coturn using TURN REST/HMAC temporary credentials.

  Example:

  TURN_HOST=turn.kyro.app
  TURN_SECRET=<same secret configured in coturn>
*/

const TURN_HOST =
  String(process.env.TURN_HOST || "").trim();

const TURN_SECRET =
  String(process.env.TURN_SECRET || "").trim();

const TURN_REALM =
  process.env.TURN_REALM ||
  TURN_HOST ||
  "kyros";

const TURN_CREDENTIAL_TTL_SECONDS =
  Number(
    process.env.TURN_CREDENTIAL_TTL_SECONDS ||
    3600
  );

/*
  OPTION B
  --------
  External TURN provider.

  Example:

  TURN_URLS=turn:relay.example.com:3478?transport=udp,turns:relay.example.com:5349?transport=tcp
  TURN_USERNAME=username
  TURN_CREDENTIAL=password
*/

const TURN_URLS =
  csvEnv("TURN_URLS");

const TURN_USERNAME =
  String(process.env.TURN_USERNAME || "").trim();

const TURN_CREDENTIAL =
  String(process.env.TURN_CREDENTIAL || "").trim();

const EXTERNAL_TURN_CONFIGURED =
  Boolean(
    TURN_URLS.length &&
    TURN_USERNAME &&
    TURN_CREDENTIAL
  );

const COTURN_CONFIGURED =
  Boolean(
    TURN_HOST &&
    TURN_SECRET
  );

const TURN_CONFIGURED =
  EXTERNAL_TURN_CONFIGURED ||
  COTURN_CONFIGURED;

/* ============================================================
   DOWNLOADS
   ============================================================ */

const DOWNLOADS = {
  android: {
    file: "kyros-android.apk",
    downloadName: "KyroS-Android.apk",
  },

  ios: {
    file: "kyros-ios.ipa",
    downloadName: "KyroS-iOS.ipa",
  },

  windows: {
    file: "kyros-windows.exe",
    downloadName: "KyroS-Windows.exe",
  },
};

/* ============================================================
   ALLOWED VALUES
   ============================================================ */

const ALLOWED_DESTINATIONS = new Set([
  "youtube",
  "facebook",
  "twitch",
  "tiktok",
  "instagram",
  "x",
  "kick",
  "custom",
]);

const ALLOWED_PLATFORMS = new Set([
  "android",
  "ios",
  "windows",
  "macos",
  "linux",
  "unknown",
]);

const ALLOWED_REMOTE_ROLES = new Set([
  "studio",
  "camera",
]);

const ALLOWED_SIGNAL_TYPES = new Set([
  "offer",
  "answer",
  "ice",
  "control",
  "camera_state",
  "studio_state",
  "hangup",
  "ping",
]);

/* ============================================================
   DATABASE INITIALIZATION
   ============================================================ */

async function initializeDatabase() {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_installations (
        installation_id UUID PRIMARY KEY,

        platform VARCHAR(20)
          NOT NULL
          DEFAULT 'unknown',

        manufacturer VARCHAR(100),
        brand VARCHAR(100),
        model VARCHAR(150),

        os_version VARCHAR(100),
        os_api INTEGER,

        app_version VARCHAR(50),
        app_build VARCHAR(50),
        app_language VARCHAR(30),

        device_language VARCHAR(30),
        device_locale VARCHAR(50),
        device_region VARCHAR(20),
        timezone VARCHAR(100),

        screen_width INTEGER,
        screen_height INTEGER,
        screen_density DOUBLE PRECISION,
        screen_refresh_rate DOUBLE PRECISION,

        cpu_cores INTEGER,
        total_memory_mb BIGINT,
        low_ram_device BOOLEAN,

        first_seen_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        last_seen_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        last_network_country VARCHAR(10),

        clustered_data JSONB,

        created_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        updated_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_sessions (
        session_id UUID PRIMARY KEY,

        installation_id UUID
          NOT NULL
          REFERENCES kyros_installations(installation_id)
          ON DELETE CASCADE,

        started_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        last_seen_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        ended_at TIMESTAMPTZ,

        app_state VARCHAR(50)
          NOT NULL
          DEFAULT 'unknown',

        is_foreground BOOLEAN
          NOT NULL
          DEFAULT TRUE,

        is_broadcasting BOOLEAN
          NOT NULL
          DEFAULT FALSE,

        is_recording BOOLEAN
          NOT NULL
          DEFAULT FALSE,

        is_screen_sharing BOOLEAN
          NOT NULL
          DEFAULT FALSE,

        is_remote_camera BOOLEAN
          NOT NULL
          DEFAULT FALSE,

        network_transport VARCHAR(30),

        internet_validated BOOLEAN,

        battery_percent INTEGER,
        charging BOOLEAN,
        power_save BOOLEAN,

        created_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        updated_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_broadcasts (
        broadcast_id UUID PRIMARY KEY,

        session_id UUID
          NOT NULL
          REFERENCES kyros_sessions(session_id)
          ON DELETE CASCADE,

        installation_id UUID
          NOT NULL
          REFERENCES kyros_installations(installation_id)
          ON DELETE CASCADE,

        destinations TEXT[]
          NOT NULL
          DEFAULT '{}',

        started_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        last_seen_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        ended_at TIMESTAMPTZ,

        created_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        updated_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_issue_reports (
        issue_id UUID PRIMARY KEY,

        installation_id UUID
          REFERENCES kyros_installations(installation_id)
          ON DELETE SET NULL,

        session_id UUID
          REFERENCES kyros_sessions(session_id)
          ON DELETE SET NULL,

        email VARCHAR(320),

        title VARCHAR(200)
          NOT NULL,

        description TEXT
          NOT NULL,

        screenshots JSONB
          NOT NULL
          DEFAULT '[]'::jsonb,

        platform VARCHAR(20),

        app_version VARCHAR(50),
        app_build VARCHAR(50),
        device_model VARCHAR(150),

        clustered_data JSONB,

        addressed BOOLEAN
          NOT NULL
          DEFAULT FALSE,

        addressed_at TIMESTAMPTZ,

        created_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        updated_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW()
      )
    `);

    /*
      One row represents one Studio <-> Remote Camera pairing.

      Security:
      - connection_code is temporary.
      - peer tokens are never stored directly.
      - only SHA-256 token hashes are stored.
      - the 6-digit code becomes NULL once Studio claims it.
      - SDP and ICE messages are not stored.
    */

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_remote_camera_sessions (
        remote_camera_id UUID PRIMARY KEY,

        connection_code VARCHAR(6) UNIQUE,

        status VARCHAR(20)
          NOT NULL
          DEFAULT 'waiting'
          CHECK (
            status IN (
              'waiting',
              'joined',
              'connected',
              'disconnected',
              'cancelled',
              'expired'
            )
          ),

        studio_installation_id UUID
          REFERENCES kyros_installations(installation_id)
          ON DELETE SET NULL,

        studio_session_id UUID
          REFERENCES kyros_sessions(session_id)
          ON DELETE SET NULL,

        camera_installation_id UUID
          REFERENCES kyros_installations(installation_id)
          ON DELETE SET NULL,

        camera_session_id UUID
          REFERENCES kyros_sessions(session_id)
          ON DELETE SET NULL,

        studio_token_hash CHAR(64),
        camera_token_hash CHAR(64),

        connection_mode VARCHAR(20)
          NOT NULL
          DEFAULT 'webrtc',

        relay_used BOOLEAN,

        selected_candidate_type VARCHAR(30),

        round_trip_ms DOUBLE PRECISION,

        packet_loss_percent DOUBLE PRECISION,

        created_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        expires_at TIMESTAMPTZ
          NOT NULL,

        joined_at TIMESTAMPTZ,

        connected_at TIMESTAMPTZ,

        disconnected_at TIMESTAMPTZ,

        last_seen_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        updated_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW(),

        CHECK (
          connection_code IS NULL
          OR connection_code ~ '^[0-9]{6}$'
        )
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_remote_camera_events (
        event_id BIGSERIAL PRIMARY KEY,

        remote_camera_id UUID
          NOT NULL
          REFERENCES kyros_remote_camera_sessions(remote_camera_id)
          ON DELETE CASCADE,

        peer_role VARCHAR(20)
          CHECK (
            peer_role IS NULL
            OR peer_role IN (
              'studio',
              'camera',
              'server'
            )
          ),

        event_type VARCHAR(50)
          NOT NULL,

        details JSONB
          NOT NULL
          DEFAULT '{}'::jsonb,

        created_at TIMESTAMPTZ
          NOT NULL
          DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_kir_created
      ON kyros_issue_reports(created_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_kir_addressed
      ON kyros_issue_reports(addressed, created_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_kir_installation
      ON kyros_issue_reports(installation_id)
    `);

    await client.query(`
      ALTER TABLE kyros_installations
      ADD COLUMN IF NOT EXISTS clustered_data JSONB
    `);

    await client.query(`
      ALTER TABLE kyros_issue_reports
      ADD COLUMN IF NOT EXISTS clustered_data JSONB
    `);

    await client.query(`
      ALTER TABLE kyros_remote_camera_sessions
      ALTER COLUMN studio_token_hash DROP NOT NULL
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_ki_last_seen
      ON kyros_installations(last_seen_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_ki_platform
      ON kyros_installations(platform)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_ks_last_seen
      ON kyros_sessions(last_seen_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_kb_started
      ON kyros_broadcasts(started_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_krcs_code_status
      ON kyros_remote_camera_sessions(
        connection_code,
        status
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_krcs_studio_installation
      ON kyros_remote_camera_sessions(
        studio_installation_id,
        created_at DESC
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_krcs_camera_installation
      ON kyros_remote_camera_sessions(
        camera_installation_id,
        created_at DESC
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_krcs_status_expiry
      ON kyros_remote_camera_sessions(
        status,
        expires_at
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_krce_session_created
      ON kyros_remote_camera_events(
        remote_camera_id,
        created_at DESC
      )
    `);

    // Authentication accounts and payment records. Existing KyroS tables remain unchanged.
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        "index" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        id VARCHAR(40) NOT NULL UNIQUE,
        name VARCHAR(160) NOT NULL,
        email VARCHAR(320) NOT NULL,
        password_hash TEXT,
        phone_number VARCHAR(40),
        sign_in_method VARCHAR(30) NOT NULL DEFAULT 'password'
          CHECK (sign_in_method IN ('password', 'google', 'password+google')),
        sign_in_id VARCHAR(255),
        role VARCHAR(30) NOT NULL DEFAULT 'user',
        account_status VARCHAR(20) NOT NULL DEFAULT 'active'
          CHECK (account_status IN ('active', 'suspended', 'disabled')),
        subscription_status VARCHAR(20) NOT NULL DEFAULT 'free'
          CHECK (subscription_status IN ('free', 'active', 'expired', 'cancelled')),
        subscription_start TIMESTAMPTZ,
        subscription_end TIMESTAMPTZ,
        auto_renew BOOLEAN NOT NULL DEFAULT FALSE,
        subscription_data JSONB NOT NULL DEFAULT '{"plan":"free"}'::jsonb,
        email_verified BOOLEAN NOT NULL DEFAULT FALSE,
        phone_verified BOOLEAN NOT NULL DEFAULT FALSE,
        country VARCHAR(2),
        profile_image TEXT,
        refresh_sessions JSONB NOT NULL DEFAULT '[]'::jsonb
          CHECK (jsonb_typeof(refresh_sessions) = 'array'),
        token_version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_login_at TIMESTAMPTZ,
        deleted_at TIMESTAMPTZ,
        CONSTRAINT users_public_id_format CHECK (id ~ '^[A-Za-z0-9]{12,40}$'),
        CONSTRAINT users_subscription_dates CHECK (
          subscription_end IS NULL OR subscription_start IS NULL
          OR subscription_end >= subscription_start
        )
      )
    `);

    // Additive account fields: NULL/{} means no free trial has ever been used.
    // Existing user rows and all pre-existing KyroS tables are preserved.
    await client.query(`
      ALTER TABLE users
      ADD COLUMN IF NOT EXISTS free_trial_used JSONB DEFAULT NULL
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_auth_otps (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_id VARCHAR(40) NOT NULL
          REFERENCES users(id) ON DELETE CASCADE,
        email VARCHAR(320) NOT NULL,
        purpose VARCHAR(32) NOT NULL
          CHECK (purpose IN ('email_verification', 'password_reset')),
        code_hash CHAR(64) NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 10),
        sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        used_at TIMESTAMPTZ,
        reset_token_hash CHAR(64),
        reset_token_expires_at TIMESTAMPTZ,
        reset_token_used_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_kyros_auth_otps_user_purpose
      ON kyros_auth_otps (user_id, purpose, created_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_kyros_auth_otps_email_purpose
      ON kyros_auth_otps (LOWER(email), purpose, created_at DESC)
    `);

    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_lower
      ON users (LOWER(email))
    `);

    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_users_sign_in_id
      ON users (sign_in_id)
      WHERE sign_in_id IS NOT NULL
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_users_account_status
      ON users (account_status, created_at DESC)
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS payments (
        "index" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        id VARCHAR(40) NOT NULL UNIQUE,
        user_id VARCHAR(40) NOT NULL
          REFERENCES users(id) ON DELETE RESTRICT,
        user_name VARCHAR(160) NOT NULL,
        user_email VARCHAR(320) NOT NULL,
        amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
        currency VARCHAR(3) NOT NULL DEFAULT 'KES'
          CHECK (currency ~ '^[A-Z]{3}$'),
        payment_method VARCHAR(40) NOT NULL,
        transaction_reference VARCHAR(200),
        payment_status VARCHAR(20) NOT NULL DEFAULT 'pending'
          CHECK (payment_status IN ('pending', 'successful', 'failed', 'refunded')),
        payment_purpose VARCHAR(120) NOT NULL,
        subscription_start TIMESTAMPTZ,
        subscription_end TIMESTAMPTZ,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        paid_at TIMESTAMPTZ,
        CONSTRAINT payments_public_id_format CHECK (id ~ '^[A-Za-z0-9]{12,40}$'),
        CONSTRAINT payments_subscription_dates CHECK (
          subscription_end IS NULL OR subscription_start IS NULL
          OR subscription_end >= subscription_start
        )
      )
    `);

    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_transaction_reference
      ON payments (transaction_reference)
      WHERE transaction_reference IS NOT NULL
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_payments_user_created
      ON payments (user_id, created_at DESC)
    `);

    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_payments_status_created
      ON payments (payment_status, created_at DESC)
    `);

    // Idempotency is additive: existing payment history remains untouched.
    await client.query(`
      ALTER TABLE payments
      ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(128)
    `);
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_user_idempotency_key
      ON payments (user_id, idempotency_key)
      WHERE idempotency_key IS NOT NULL
    `);

    await client.query("COMMIT");

    console.log(
      "KyroS database initialized."
    );

  } catch (e) {

    await client.query("ROLLBACK");

    throw e;

  } finally {

    client.release();
  }
}

/* ============================================================
   HELPERS
   ============================================================ */

function str(v, max = 200) {
  if (typeof v !== "string") {
    return null;
  }

  const s = v.trim();

  return s
    ? s.slice(0, max)
    : null;
}

function int(
  v,
  min = null,
  max = null
) {
  const n = Number(v);

  if (!Number.isInteger(n)) {
    return null;
  }

  if (
    min !== null &&
    n < min
  ) {
    return null;
  }

  if (
    max !== null &&
    n > max
  ) {
    return null;
  }

  return n;
}

function num(
  v,
  min = null,
  max = null
) {
  const n = Number(v);

  if (!Number.isFinite(n)) {
    return null;
  }

  if (
    min !== null &&
    n < min
  ) {
    return null;
  }

  if (
    max !== null &&
    n > max
  ) {
    return null;
  }

  return n;
}

function bool(v) {
  return typeof v === "boolean"
    ? v
    : null;
}

function uuid(v) {
  return (
    typeof v === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(v)
  );
}

function platform(v) {
  const p =
    (
      str(v, 20) ||
      "unknown"
    ).toLowerCase();

  return ALLOWED_PLATFORMS.has(p)
    ? p
    : "unknown";
}

function destinations(v) {
  if (!Array.isArray(v)) {
    return [];
  }

  return [
    ...new Set(
      v
        .filter(
          x =>
            typeof x === "string"
        )
        .map(
          x =>
            x.trim().toLowerCase()
        )
        .filter(
          x =>
            ALLOWED_DESTINATIONS.has(x)
        )
    ),
  ].slice(0, 20);
}

function country(req) {
  for (const h of [
    req.headers["cf-ipcountry"],
    req.headers["x-vercel-ip-country"],
    req.headers["x-country-code"],
  ]) {
    if (
      typeof h === "string" &&
      /^[A-Za-z]{2}$/.test(h.trim())
    ) {
      return h
        .trim()
        .toUpperCase();
    }
  }

  return null;
}

function screenshotList(v) {
  if (!Array.isArray(v)) {
    return [];
  }

  return v
    .filter(
      x =>
        typeof x === "string"
    )
    .map(
      x =>
        x.trim()
    )
    .filter(Boolean)
    .slice(0, 6)
    .map(
      x =>
        x.slice(0, 1000)
    );
}

function limitInt(
  v,
  fallback,
  min,
  max
) {
  const n =
    Number.parseInt(v, 10);

  if (!Number.isFinite(n)) {
    return fallback;
  }

  return Math.max(
    min,
    Math.min(
      max,
      n
    )
  );
}

/* ============================================================
   REMOTE CAMERA HELPERS
   ============================================================ */

function normalizeConnectionCode(v) {
  if (
    typeof v !== "string" &&
    typeof v !== "number"
  ) {
    return null;
  }

  const code =
    String(v)
      .replace(/\D/g, "");

  return /^[0-9]{6}$/.test(code)
    ? code
    : null;
}

function generateConnectionCode() {
  return String(
    crypto.randomInt(
      100000,
      1000000
    )
  );
}

function generatePeerToken() {
  return crypto
    .randomBytes(32)
    .toString("hex");
}

function tokenHash(token) {
  return crypto
    .createHash("sha256")
    .update(
      String(token || "")
    )
    .digest("hex");
}

function safeEqualHash(a, b) {
  if (
    typeof a !== "string" ||
    typeof b !== "string"
  ) {
    return false;
  }

  if (a.length !== b.length) {
    return false;
  }

  return crypto.timingSafeEqual(
    Buffer.from(a),
    Buffer.from(b)
  );
}

async function existingInstallationId(v) {
  if (!uuid(v)) {
    return null;
  }

  const q =
    await pool.query(
      `
      SELECT installation_id
      FROM kyros_installations
      WHERE installation_id=$1
      `,
      [v]
    );

  return q.rowCount
    ? v
    : null;
}

async function existingSessionId(v) {
  if (!uuid(v)) {
    return null;
  }

  const q =
    await pool.query(
      `
      SELECT session_id
      FROM kyros_sessions
      WHERE session_id=$1
      `,
      [v]
    );

  return q.rowCount
    ? v
    : null;
}

async function logRemoteEvent(
  remoteCameraId,
  peerRole,
  eventType,
  details = {}
) {
  try {
    await pool.query(
      `
      INSERT INTO kyros_remote_camera_events (
        remote_camera_id,
        peer_role,
        event_type,
        details
      )
      VALUES (
        $1,
        $2,
        $3,
        $4::jsonb
      )
      `,
      [
        remoteCameraId,

        ALLOWED_REMOTE_ROLES.has(
          peerRole
        )
          ? peerRole
          : "server",

        String(eventType)
          .slice(0, 50),

        JSON.stringify(
          details &&
          typeof details === "object"
            ? details
            : {}
        ),
      ]
    );

  } catch (e) {

    console.warn(
      "Remote camera event log failed:",
      e.message
    );
  }
}

async function authenticateRemotePeer(
  remoteCameraId,
  role,
  token
) {
  if (!uuid(remoteCameraId)) {
    return null;
  }

  if (!ALLOWED_REMOTE_ROLES.has(role)) {
    return null;
  }

  if (
    typeof token !== "string" ||
    token.length < 32 ||
    token.length > 256
  ) {
    return null;
  }

  const result =
    await pool.query(
      `
      SELECT *
      FROM kyros_remote_camera_sessions
      WHERE remote_camera_id=$1
      LIMIT 1
      `,
      [remoteCameraId]
    );

  if (!result.rowCount) {
    return null;
  }

  const row =
    result.rows[0];

  const expected =
    role === "studio"
      ? row.studio_token_hash
      : row.camera_token_hash;

  if (!expected) {
    return null;
  }

  const actual =
    tokenHash(token);

  if (
    !safeEqualHash(
      actual,
      expected
    )
  ) {
    return null;
  }

  if (
    [
      "cancelled",
      "expired",
    ].includes(row.status)
  ) {
    return null;
  }

  if (
    new Date(
      row.expires_at
    ).getTime() <= Date.now()
  ) {
    return null;
  }

  return row;
}

function publicRemoteSession(row) {
  return {
    remoteCameraId:
      row.remote_camera_id,

    status:
      row.status,

    createdAt:
      row.created_at,

    expiresAt:
      row.expires_at,

    joinedAt:
      row.joined_at,

    connectedAt:
      row.connected_at,

    disconnectedAt:
      row.disconnected_at,
  };
}

/* ============================================================
   ICE SERVER GENERATION
   ============================================================ */

function buildIceServers(remoteCameraId) {
  const servers = [
    {
      urls:
        EFFECTIVE_STUN_URLS,
    },
  ];

  /*
    External hosted TURN provider.
  */

  if (EXTERNAL_TURN_CONFIGURED) {
    servers.push({
      urls:
        TURN_URLS,

      username:
        TURN_USERNAME,

      credential:
        TURN_CREDENTIAL,
    });
  }

  /*
    KyroS-owned coturn.

    Temporary TURN credentials are generated
    from the shared secret.

    The master TURN secret never enters
    the Flutter or Android application.
  */

  if (COTURN_CONFIGURED) {
    const expiry =
      Math.floor(
        Date.now() / 1000
      ) +
      TURN_CREDENTIAL_TTL_SECONDS;

    const username =
      `${expiry}:${remoteCameraId}`;

    const credential =
      crypto
        .createHmac(
          "sha1",
          TURN_SECRET
        )
        .update(username)
        .digest("base64");

    servers.push({
      urls: [
        `turn:${TURN_HOST}:3478?transport=udp`,
        `turn:${TURN_HOST}:3478?transport=tcp`,
        `turns:${TURN_HOST}:5349?transport=tcp`,
      ],

      username,

      credential,
    });
  }

  return servers;
}

/* ============================================================
   WEBSITE + DOWNLOADS
   ============================================================ */

app.use(
  express.static(
    PUBLIC_DIR,
    {
      extensions: ["html"],
      maxAge: "1h",
    }
  )
);

app.get(
  "/download/:platform",
  (req, res) => {
    const item =
      DOWNLOADS[
        String(
          req.params.platform ||
          ""
        ).toLowerCase()
      ];

    if (!item) {
      return res
        .status(404)
        .json({
          ok: false,
          error:
            "unsupported_platform",
        });
    }

    const filePath =
      path.join(
        RESOURCES_DIR,
        item.file
      );

    if (
      !fs.existsSync(filePath) ||
      fs.statSync(filePath).size === 0
    ) {
      return res
        .status(503)
        .json({
          ok: false,
          error:
            "release_not_available",

          platform:
            req.params.platform,

          message:
            "This KyroS release package has not been uploaded yet.",
        });
    }

    return res.download(
      filePath,
      item.downloadName
    );
  }
);

app.get(
  "/api/downloads",
  (req, res) => {
    const releases = {};

    for (
      const [
        name,
        item,
      ] of Object.entries(
        DOWNLOADS
      )
    ) {
      const filePath =
        path.join(
          RESOURCES_DIR,
          item.file
        );

      releases[name] = {
        available:
          fs.existsSync(filePath) &&
          fs.statSync(filePath).size > 0,

        url:
          `/download/${name}`,
      };
    }

    res.json({
      ok: true,
      releases,
    });
  }
);

/* ============================================================
   TELEMETRY
   ============================================================ */

app.post(
  "/api/telemetry",
  async (req, res) => {
    const client =
      await pool.connect();

    try {
      const body =
        req.body || {};

      const installationId =
        body.installationId;

      let sessionId =
        body.sessionId;

      if (!uuid(installationId)) {
        return res
          .status(400)
          .json({
            ok: false,
            error:
              "invalid_installation_id",
          });
      }

      if (!uuid(sessionId)) {
        sessionId =
          crypto.randomUUID();
      }

      const device =
        body.device &&
        typeof body.device ===
          "object"
          ? body.device
          : {};

      const appInfo =
        body.app &&
        typeof body.app ===
          "object"
          ? body.app
          : {};

      const display =
        body.display &&
        typeof body.display ===
          "object"
          ? body.display
          : {};

      const hardware =
        body.hardware &&
        typeof body.hardware ===
          "object"
          ? body.hardware
          : {};

      const network =
        body.network &&
        typeof body.network ===
          "object"
          ? body.network
          : {};

      const power =
        body.power &&
        typeof body.power ===
          "object"
          ? body.power
          : {};

      const state =
        body.state &&
        typeof body.state ===
          "object"
          ? body.state
          : {};

      const broadcast =
        body.broadcast &&
        typeof body.broadcast ===
          "object"
          ? body.broadcast
          : {};

      const clusteredData =
        body.clusteredData &&
        typeof body.clusteredData ===
          "object"
          ? body.clusteredData
          : null;

      const p =
        platform(
          device.platform
        );

      const dests =
        destinations(
          broadcast.destinations
        );

      const broadcasting =
        bool(
          broadcast.active
        ) ??
        (
          dests.length > 0
        );

      const networkCountry =
        country(req);

      await client.query(
        "BEGIN"
      );

      await client.query(
        `
        INSERT INTO kyros_installations (
          installation_id,
          platform,
          manufacturer,
          brand,
          model,
          os_version,
          os_api,
          app_version,
          app_build,
          app_language,
          device_language,
          device_locale,
          device_region,
          timezone,
          screen_width,
          screen_height,
          screen_density,
          screen_refresh_rate,
          cpu_cores,
          total_memory_mb,
          low_ram_device,
          last_network_country,
          clustered_data,
          first_seen_at,
          last_seen_at,
          created_at,
          updated_at
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,
          $8,$9,$10,$11,$12,$13,$14,
          $15,$16,$17,$18,$19,$20,$21,
          $22,$23::jsonb,
          NOW(),NOW(),NOW(),NOW()
        )
        ON CONFLICT (installation_id)
        DO UPDATE SET
          platform=
            EXCLUDED.platform,

          manufacturer=
            COALESCE(
              EXCLUDED.manufacturer,
              kyros_installations.manufacturer
            ),

          brand=
            COALESCE(
              EXCLUDED.brand,
              kyros_installations.brand
            ),

          model=
            COALESCE(
              EXCLUDED.model,
              kyros_installations.model
            ),

          os_version=
            COALESCE(
              EXCLUDED.os_version,
              kyros_installations.os_version
            ),

          os_api=
            COALESCE(
              EXCLUDED.os_api,
              kyros_installations.os_api
            ),

          app_version=
            COALESCE(
              EXCLUDED.app_version,
              kyros_installations.app_version
            ),

          app_build=
            COALESCE(
              EXCLUDED.app_build,
              kyros_installations.app_build
            ),

          app_language=
            COALESCE(
              EXCLUDED.app_language,
              kyros_installations.app_language
            ),

          device_language=
            COALESCE(
              EXCLUDED.device_language,
              kyros_installations.device_language
            ),

          device_locale=
            COALESCE(
              EXCLUDED.device_locale,
              kyros_installations.device_locale
            ),

          device_region=
            COALESCE(
              EXCLUDED.device_region,
              kyros_installations.device_region
            ),

          timezone=
            COALESCE(
              EXCLUDED.timezone,
              kyros_installations.timezone
            ),

          screen_width=
            COALESCE(
              EXCLUDED.screen_width,
              kyros_installations.screen_width
            ),

          screen_height=
            COALESCE(
              EXCLUDED.screen_height,
              kyros_installations.screen_height
            ),

          screen_density=
            COALESCE(
              EXCLUDED.screen_density,
              kyros_installations.screen_density
            ),

          screen_refresh_rate=
            COALESCE(
              EXCLUDED.screen_refresh_rate,
              kyros_installations.screen_refresh_rate
            ),

          cpu_cores=
            COALESCE(
              EXCLUDED.cpu_cores,
              kyros_installations.cpu_cores
            ),

          total_memory_mb=
            COALESCE(
              EXCLUDED.total_memory_mb,
              kyros_installations.total_memory_mb
            ),

          low_ram_device=
            COALESCE(
              EXCLUDED.low_ram_device,
              kyros_installations.low_ram_device
            ),

          last_network_country=
            COALESCE(
              EXCLUDED.last_network_country,
              kyros_installations.last_network_country
            ),

          clustered_data=
            COALESCE(
              EXCLUDED.clustered_data,
              kyros_installations.clustered_data
            ),

          last_seen_at=
            NOW(),

          updated_at=
            NOW()
        `,
        [
          installationId,

          p,

          str(
            device.manufacturer,
            100
          ),

          str(
            device.brand,
            100
          ),

          str(
            device.model,
            150
          ),

          str(
            device.osVersion,
            100
          ),

          int(
            device.osApi,
            1,
            1000
          ),

          str(
            appInfo.version,
            50
          ),

          str(
            appInfo.build,
            50
          ),

          str(
            appInfo.language,
            30
          ),

          str(
            device.language,
            30
          ),

          str(
            device.locale,
            50
          ),

          str(
            device.region,
            20
          ),

          str(
            device.timezone,
            100
          ),

          int(
            display.width,
            1,
            50000
          ),

          int(
            display.height,
            1,
            50000
          ),

          num(
            display.density,
            0.1,
            20
          ),

          num(
            display.refreshRate,
            1,
            1000
          ),

          int(
            hardware.cpuCores,
            1,
            1024
          ),

          int(
            hardware.totalMemoryMb,
            1,
            10000000
          ),

          bool(
            hardware.lowRamDevice
          ),

          networkCountry,

          clusteredData === null
            ? null
            : JSON.stringify(
                clusteredData
              ),
        ]
      );

      await client.query(
        `
        INSERT INTO kyros_sessions (
          session_id,
          installation_id,
          started_at,
          last_seen_at,
          app_state,
          is_foreground,
          is_broadcasting,
          is_recording,
          is_screen_sharing,
          is_remote_camera,
          network_transport,
          internet_validated,
          battery_percent,
          charging,
          power_save,
          created_at,
          updated_at
        )
        VALUES (
          $1,$2,NOW(),NOW(),
          $3,$4,$5,$6,$7,$8,
          $9,$10,$11,$12,$13,
          NOW(),NOW()
        )
        ON CONFLICT (session_id)
        DO UPDATE SET
          last_seen_at=
            NOW(),

          app_state=
            EXCLUDED.app_state,

          is_foreground=
            EXCLUDED.is_foreground,

          is_broadcasting=
            EXCLUDED.is_broadcasting,

          is_recording=
            EXCLUDED.is_recording,

          is_screen_sharing=
            EXCLUDED.is_screen_sharing,

          is_remote_camera=
            EXCLUDED.is_remote_camera,

          network_transport=
            EXCLUDED.network_transport,

          internet_validated=
            EXCLUDED.internet_validated,

          battery_percent=
            EXCLUDED.battery_percent,

          charging=
            EXCLUDED.charging,

          power_save=
            EXCLUDED.power_save,

          updated_at=
            NOW()
        `,
        [
          sessionId,

          installationId,

          str(
            state.appState,
            50
          ) || "unknown",

          bool(
            state.foreground
          ) ?? true,

          broadcasting,

          bool(
            state.recording
          ) ?? false,

          bool(
            state.screenSharing
          ) ?? false,

          bool(
            state.remoteCamera
          ) ?? false,

          str(
            network.transport,
            30
          ),

          bool(
            network.validated
          ),

          int(
            power.batteryPercent,
            0,
            100
          ),

          bool(
            power.charging
          ),

          bool(
            power.powerSave
          ),
        ]
      );

      let broadcastId =
        uuid(
          broadcast.broadcastId
        )
          ? broadcast.broadcastId
          : null;

      if (broadcasting) {
        if (!broadcastId) {
          const found =
            await client.query(
              `
              SELECT broadcast_id
              FROM kyros_broadcasts
              WHERE session_id=$1
                AND ended_at IS NULL
              ORDER BY started_at DESC
              LIMIT 1
              `,
              [
                sessionId,
              ]
            );

          broadcastId =
            found.rows[0]
              ?.broadcast_id ||
            crypto.randomUUID();
        }

        await client.query(
          `
          INSERT INTO kyros_broadcasts (
            broadcast_id,
            session_id,
            installation_id,
            destinations,
            started_at,
            last_seen_at,
            created_at,
            updated_at
          )
          VALUES (
            $1,$2,$3,$4,
            NOW(),NOW(),NOW(),NOW()
          )
          ON CONFLICT (broadcast_id)
          DO UPDATE SET
            destinations=
              EXCLUDED.destinations,

            last_seen_at=
              NOW(),

            updated_at=
              NOW()
          `,
          [
            broadcastId,
            sessionId,
            installationId,
            dests,
          ]
        );
      } else {
        await client.query(
          `
          UPDATE kyros_broadcasts
          SET
            ended_at=
              COALESCE(
                ended_at,
                NOW()
              ),

            last_seen_at=
              NOW(),

            updated_at=
              NOW()

          WHERE session_id=$1
            AND ended_at IS NULL
          `,
          [
            sessionId,
          ]
        );

        broadcastId =
          null;
      }

      await client.query(
        "COMMIT"
      );

      res.json({
        ok: true,

        installationId,

        sessionId,

        broadcastId,

        serverTime:
          new Date()
            .toISOString(),

        activeTimeoutSeconds:
          ACTIVE_TIMEOUT_SECONDS,
      });

    } catch (e) {

      try {
        await client.query(
          "ROLLBACK"
        );
      } catch (_) {}

      console.error(
        "Telemetry error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,
          error:
            "telemetry_failed",
        });

    } finally {

      client.release();
    }
  }
);

/* ============================================================
   ISSUE REPORTING
   ============================================================ */

app.post(
  "/api/issues",
  async (req, res) => {
    try {
      const body =
        req.body &&
        typeof req.body === "object"
          ? req.body
          : {};

      const issueId =
        uuid(body.id)
          ? body.id
          : (
              uuid(body.issueId)
                ? body.issueId
                : crypto.randomUUID()
            );

      const installationId =
        uuid(body.installationId)
          ? body.installationId
          : null;

      const sessionId =
        uuid(body.sessionId)
          ? body.sessionId
          : null;

      const title =
        str(
          body.title,
          200
        );

      const description =
        str(
          body.description,
          12000
        );

      const email =
        str(
          body.email,
          320
        );

      const shots =
        screenshotList(
          body.screenshots
        );

      const platformValue =
        platform(
          body.platform ||
          body.device?.platform
        );

      const appVersion =
        str(
          body.appVersion ||
          body.app?.version,
          50
        );

      const appBuild =
        str(
          body.appBuild ||
          body.app?.build,
          50
        );

      const deviceModel =
        str(
          body.deviceModel ||
          body.device?.model,
          150
        );

      const clusteredData =
        body.clusteredData &&
        typeof body.clusteredData ===
          "object"
          ? body.clusteredData
          : null;

      if (
        !title ||
        !description
      ) {
        return res
          .status(400)
          .json({
            ok: false,

            error:
              "title_and_description_required",
          });
      }

      let safeInstallationId =
        installationId;

      let safeSessionId =
        sessionId;

      if (safeInstallationId) {
        const found =
          await pool.query(
            `
            SELECT 1
            FROM kyros_installations
            WHERE installation_id=$1
            `,
            [
              safeInstallationId,
            ]
          );

        if (!found.rowCount) {
          safeInstallationId =
            null;
        }
      }

      if (safeSessionId) {
        const found =
          await pool.query(
            `
            SELECT 1
            FROM kyros_sessions
            WHERE session_id=$1
            `,
            [
              safeSessionId,
            ]
          );

        if (!found.rowCount) {
          safeSessionId =
            null;
        }
      }

      const result =
        await pool.query(
          `
          INSERT INTO kyros_issue_reports (
            issue_id,
            installation_id,
            session_id,
            email,
            title,
            description,
            screenshots,
            platform,
            app_version,
            app_build,
            device_model,
            clustered_data,
            addressed,
            created_at,
            updated_at
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,
            $7::jsonb,
            $8,$9,$10,$11,
            $12::jsonb,
            FALSE,
            NOW(),
            NOW()
          )
          ON CONFLICT (issue_id)
          DO UPDATE SET
            email=
              COALESCE(
                EXCLUDED.email,
                kyros_issue_reports.email
              ),

            title=
              EXCLUDED.title,

            description=
              EXCLUDED.description,

            screenshots=
              EXCLUDED.screenshots,

            platform=
              COALESCE(
                EXCLUDED.platform,
                kyros_issue_reports.platform
              ),

            app_version=
              COALESCE(
                EXCLUDED.app_version,
                kyros_issue_reports.app_version
              ),

            app_build=
              COALESCE(
                EXCLUDED.app_build,
                kyros_issue_reports.app_build
              ),

            device_model=
              COALESCE(
                EXCLUDED.device_model,
                kyros_issue_reports.device_model
              ),

            clustered_data=
              COALESCE(
                EXCLUDED.clustered_data,
                kyros_issue_reports.clustered_data
              ),

            updated_at=
              NOW()

          RETURNING
            issue_id,
            addressed,
            created_at
          `,
          [
            issueId,

            safeInstallationId,

            safeSessionId,

            email,

            title,

            description,

            JSON.stringify(
              shots
            ),

            platformValue,

            appVersion,

            appBuild,

            deviceModel,

            clusteredData === null
              ? null
              : JSON.stringify(
                  clusteredData
                ),
          ]
        );

      res
        .status(201)
        .json({
          ok: true,

          issue:
            result.rows[0],

          message:
            "Issue report received.",
        });

    } catch (e) {

      console.error(
        "Issue submission error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "issue_submission_failed",
        });
    }
  }
);

app.get(
  "/admin-issues",
  async (req, res) => {
    try {
      const status =
        String(
          req.query.status ||
          "open"
        ).toLowerCase();

      const limit =
        limitInt(
          req.query.limit,
          100,
          1,
          250
        );

      const offset =
        limitInt(
          req.query.offset,
          0,
          0,
          1000000
        );

      const search =
        str(
          req.query.q,
          200
        );

      const where = [];
      const values = [];

      if (
        status === "open"
      ) {
        where.push(
          `r.addressed=FALSE`
        );
      } else if (
        status === "addressed"
      ) {
        where.push(
          `r.addressed=TRUE`
        );
      }

      if (search) {
        values.push(
          `%${search}%`
        );

        where.push(`
          (
            r.title ILIKE $${values.length}
            OR r.description ILIKE $${values.length}
            OR COALESCE(r.email,'') ILIKE $${values.length}
            OR COALESCE(r.device_model,'') ILIKE $${values.length}
          )
        `);
      }

      const whereSql =
        where.length
          ? `WHERE ${where.join(" AND ")}`
          : "";

      values.push(
        limit,
        offset
      );

      const [
        rows,
        counts,
      ] =
        await Promise.all([
          pool.query(
            `
            SELECT
              r.issue_id,
              r.installation_id,
              r.session_id,
              r.email,
              r.title,
              r.description,
              r.screenshots,
              r.platform,
              r.app_version,
              r.app_build,
              r.device_model,
              r.clustered_data,
              r.addressed,
              r.addressed_at,
              r.created_at,
              r.updated_at,
              i.last_network_country

            FROM kyros_issue_reports r

            LEFT JOIN kyros_installations i
              ON i.installation_id=
                r.installation_id

            ${whereSql}

            ORDER BY
              r.addressed ASC,
              r.created_at DESC

            LIMIT $${values.length - 1}

            OFFSET $${values.length}
            `,
            values
          ),

          pool.query(`
            SELECT
              COUNT(*)::int
                AS total,

              COUNT(*) FILTER (
                WHERE addressed=FALSE
              )::int
                AS open,

              COUNT(*) FILTER (
                WHERE addressed=TRUE
              )::int
                AS addressed

            FROM kyros_issue_reports
          `),
        ]);

      res.json({
        ok: true,

        generatedAt:
          new Date()
            .toISOString(),

        counts:
          counts.rows[0],

        issues:
          rows.rows,

        limit,

        offset,
      });

    } catch (e) {

      console.error(
        "Admin issues error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "admin_issues_failed",
        });
    }
  }
);

app.patch(
  "/admin-issues/:issueId",
  async (req, res) => {
    try {
      const issueId =
        req.params.issueId;

      if (!uuid(issueId)) {
        return res
          .status(400)
          .json({
            ok: false,

            error:
              "invalid_issue_id",
          });
      }

      if (
        typeof req.body
          ?.addressed !==
        "boolean"
      ) {
        return res
          .status(400)
          .json({
            ok: false,

            error:
              "addressed_boolean_required",
          });
      }

      const addressed =
        req.body.addressed;

      const result =
        await pool.query(
          `
          UPDATE kyros_issue_reports

          SET
            addressed=$2,

            addressed_at=
              CASE
                WHEN $2
                  THEN COALESCE(
                    addressed_at,
                    NOW()
                  )
                ELSE NULL
              END,

            updated_at=
              NOW()

          WHERE issue_id=$1

          RETURNING
            issue_id,
            addressed,
            addressed_at,
            updated_at
          `,
          [
            issueId,
            addressed,
          ]
        );

      if (!result.rowCount) {
        return res
          .status(404)
          .json({
            ok: false,

            error:
              "issue_not_found",
          });
      }

      res.json({
        ok: true,

        issue:
          result.rows[0],
      });

    } catch (e) {

      console.error(
        "Issue update error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "issue_update_failed",
        });
    }
  }
);

/* ============================================================
   REMOTE CAMERA - CODE-BASED PAIRING
   ============================================================ */

/*
  CAMERA FLOW

  POST /api/remote-camera/create

  The CAMERA generates the six-digit connection code.

  The Studio does not generate it.

  The camera displays the code.
  Studio enters that code.
*/

app.post(
  "/api/remote-camera/create",
  async (req, res) => {
    try {
      const body =
        req.body &&
        typeof req.body === "object"
          ? req.body
          : {};

      const cameraInstallationId =
        await existingInstallationId(
          body.installationId
        );

      const cameraSessionId =
        await existingSessionId(
          body.sessionId
        );

      const remoteCameraId =
        crypto.randomUUID();

      const cameraToken =
        generatePeerToken();

      const cameraTokenHash =
        tokenHash(
          cameraToken
        );

      let inserted =
        null;

      for (
        let attempt = 0;
        attempt < 20;
        attempt++
      ) {
        const code =
          generateConnectionCode();

        try {
          const result =
            await pool.query(
              `
              INSERT INTO kyros_remote_camera_sessions (
                remote_camera_id,
                connection_code,
                status,
                camera_installation_id,
                camera_session_id,
                camera_token_hash,
                created_at,
                expires_at,
                last_seen_at,
                updated_at
              )
              VALUES (
                $1,
                $2,
                'waiting',
                $3,
                $4,
                $5,
                NOW(),
                NOW()+($6*INTERVAL '1 minute'),
                NOW(),
                NOW()
              )
              RETURNING *
              `,
              [
                remoteCameraId,
                code,
                cameraInstallationId,
                cameraSessionId,
                cameraTokenHash,
                REMOTE_CAMERA_CODE_TTL_MINUTES,
              ]
            );

          inserted =
            result.rows[0];

          break;

        } catch (e) {

          if (
            e?.code === "23505"
          ) {
            continue;
          }

          throw e;
        }
      }

      if (!inserted) {
        return res
          .status(503)
          .json({
            ok: false,

            error:
              "unable_to_allocate_connection_code",
          });
      }

      await logRemoteEvent(
        remoteCameraId,
        "camera",
        "session_created",
        {
          expiresAt:
            inserted.expires_at,
        }
      );

      res
        .status(201)
        .json({
          ok: true,

          remoteCameraId,

          code:
            inserted.connection_code,

          cameraToken,

          status:
            inserted.status,

          expiresAt:
            inserted.expires_at,
        });

    } catch (e) {

      console.error(
        "Remote camera create error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "remote_camera_create_failed",
        });
    }
  }
);

/*
  STUDIO FLOW

  Studio receives no pre-coded digits from the server.

  User enters the 6-digit code that the CAMERA displayed.

  POST /api/remote-camera/join

  {
    "code": "742918"
  }
*/

app.post(
  "/api/remote-camera/join",
  async (req, res) => {
    const client =
      await pool.connect();

    try {
      const body =
        req.body &&
        typeof req.body === "object"
          ? req.body
          : {};

      const code =
        normalizeConnectionCode(
          body.code
        );

      if (!code) {
        return res
          .status(400)
          .json({
            ok: false,

            error:
              "invalid_connection_code",
          });
      }

      const studioInstallationId =
        await existingInstallationId(
          body.installationId
        );

      const studioSessionId =
        await existingSessionId(
          body.sessionId
        );

      const studioToken =
        generatePeerToken();

      const studioTokenHash =
        tokenHash(
          studioToken
        );

      await client.query(
        "BEGIN"
      );

      const found =
        await client.query(
          `
          SELECT *
          FROM kyros_remote_camera_sessions

          WHERE connection_code=$1

            AND status='waiting'

            AND expires_at>NOW()

          FOR UPDATE

          LIMIT 1
          `,
          [
            code,
          ]
        );

      if (!found.rowCount) {
        await client.query(
          "ROLLBACK"
        );

        return res
          .status(404)
          .json({
            ok: false,

            error:
              "connection_code_not_found_or_expired",
          });
      }

      const remoteCameraId =
        found.rows[0]
          .remote_camera_id;

      const updated =
        await client.query(
          `
          UPDATE kyros_remote_camera_sessions

          SET
            connection_code=NULL,

            status='joined',

            studio_installation_id=$2,

            studio_session_id=$3,

            studio_token_hash=$4,

            joined_at=NOW(),

            last_seen_at=NOW(),

            expires_at=
              NOW()+(
                $5*INTERVAL '1 hour'
              ),

            updated_at=NOW()

          WHERE remote_camera_id=$1

          RETURNING *
          `,
          [
            remoteCameraId,
            studioInstallationId,
            studioSessionId,
            studioTokenHash,
            REMOTE_CAMERA_SESSION_TTL_HOURS,
          ]
        );

      await client.query(
        "COMMIT"
      );

      await logRemoteEvent(
        remoteCameraId,
        "studio",
        "code_joined",
        {}
      );

      res.json({
        ok: true,

        remoteCameraId,

        studioToken,

        status:
          updated.rows[0]
            .status,

        expiresAt:
          updated.rows[0]
            .expires_at,
      });

    } catch (e) {

      try {
        await client.query(
          "ROLLBACK"
        );
      } catch (_) {}

      console.error(
        "Remote camera join error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "remote_camera_join_failed",
        });

    } finally {

      client.release();
    }
  }
);

/* ============================================================
   REMOTE CAMERA ICE CONFIG
   ============================================================ */

/*
  This endpoint is called after the peer has authenticated.

  IMPORTANT:

  Google STUN is always returned by default.

  This means we DO NOT return:
      ice_servers_not_configured

  simply because TURN is not configured.

  TURN is optional and is appended when available.
*/

app.post(
  "/api/remote-camera/ice-config",
  async (req, res) => {
    try {
      const body =
        req.body &&
        typeof req.body === "object"
          ? req.body
          : {};

      const remoteCameraId =
        body.remoteCameraId;

      const role =
        str(
          body.role,
          20
        );

      const token =
        str(
          body.token,
          256
        );

      const session =
        await authenticateRemotePeer(
          remoteCameraId,
          role,
          token
        );

      if (!session) {
        return res
          .status(401)
          .json({
            ok: false,

            error:
              "unauthorized_remote_camera_peer",
          });
      }

      const iceServers =
        buildIceServers(
          remoteCameraId
        );

      res.json({
        ok: true,

        iceServers,

        ttlSeconds:
          TURN_CREDENTIAL_TTL_SECONDS,

        realm:
          TURN_REALM,

        stunAvailable:
          EFFECTIVE_STUN_URLS.length > 0,

        turnAvailable:
          TURN_CONFIGURED,
      });

    } catch (e) {

      console.error(
        "ICE config error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "ice_config_failed",
        });
    }
  }
);

/* ============================================================
   REMOTE CAMERA STATE
   ============================================================ */

app.patch(
  "/api/remote-camera/state",
  async (req, res) => {
    try {
      const body =
        req.body &&
        typeof req.body === "object"
          ? req.body
          : {};

      const remoteCameraId =
        body.remoteCameraId;

      const role =
        str(
          body.role,
          20
        );

      const token =
        str(
          body.token,
          256
        );

      const session =
        await authenticateRemotePeer(
          remoteCameraId,
          role,
          token
        );

      if (!session) {
        return res
          .status(401)
          .json({
            ok: false,

            error:
              "unauthorized_remote_camera_peer",
          });
      }

      const relayUsed =
        bool(
          body.relayUsed
        );

      const candidateType =
        str(
          body.candidateType,
          30
        );

      const roundTripMs =
        num(
          body.roundTripMs,
          0,
          600000
        );

      const packetLossPercent =
        num(
          body.packetLossPercent,
          0,
          100
        );

      const result =
        await pool.query(
          `
          UPDATE kyros_remote_camera_sessions

          SET
            relay_used=
              COALESCE(
                $2,
                relay_used
              ),

            selected_candidate_type=
              COALESCE(
                $3,
                selected_candidate_type
              ),

            round_trip_ms=
              COALESCE(
                $4,
                round_trip_ms
              ),

            packet_loss_percent=
              COALESCE(
                $5,
                packet_loss_percent
              ),

            last_seen_at=
              NOW(),

            updated_at=
              NOW()

          WHERE remote_camera_id=$1

          RETURNING
            status,
            relay_used,
            selected_candidate_type,
            round_trip_ms,
            packet_loss_percent
          `,
          [
            remoteCameraId,
            relayUsed,
            candidateType,
            roundTripMs,
            packetLossPercent,
          ]
        );

      res.json({
        ok: true,

        state:
          result.rows[0],
      });

    } catch (e) {

      console.error(
        "Remote camera state error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "remote_camera_state_failed",
        });
    }
  }
);

/* ============================================================
   REMOTE CAMERA CANCEL
   ============================================================ */

app.post(
  "/api/remote-camera/cancel",
  async (req, res) => {
    try {
      const body =
        req.body &&
        typeof req.body === "object"
          ? req.body
          : {};

      const remoteCameraId =
        body.remoteCameraId;

      const role =
        str(
          body.role,
          20
        );

      const token =
        str(
          body.token,
          256
        );

      const session =
        await authenticateRemotePeer(
          remoteCameraId,
          role,
          token
        );

      if (!session) {
        return res
          .status(401)
          .json({
            ok: false,

            error:
              "unauthorized_remote_camera_peer",
          });
      }

      await pool.query(
        `
        UPDATE kyros_remote_camera_sessions

        SET
          connection_code=NULL,

          status='cancelled',

          disconnected_at=
            COALESCE(
              disconnected_at,
              NOW()
            ),

          last_seen_at=
            NOW(),

          updated_at=
            NOW()

        WHERE remote_camera_id=$1
        `,
        [
          remoteCameraId,
        ]
      );

      closeRemoteRoom(
        remoteCameraId,
        "cancelled"
      );

      await logRemoteEvent(
        remoteCameraId,
        role,
        "session_cancelled",
        {}
      );

      res.json({
        ok: true,
      });

    } catch (e) {

      console.error(
        "Remote camera cancel error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "remote_camera_cancel_failed",
        });
    }
  }
);

/* ============================================================
   ADMIN DASHBOARD
   ============================================================ */

app.get(
  "/admin-dashboard",
  async (req, res) => {
    try {
      const activeLimit =
        limitInt(
          req.query.activeLimit,
          100,
          1,
          300
        );

      const recentLimit =
        limitInt(
          req.query.recentLimit,
          30,
          1,
          100
        );

      const timeout =
        ACTIVE_TIMEOUT_SECONDS;

      const [
        summary,
        platforms,
        countries,
        deviceModels,
        versions,
        activeDevices,
        liveProductions,
        remoteSessions,
        activity,
        trend,
        destinationStats,
        languageStats,
        issueStats,
      ] =
        await Promise.all([

          pool.query(
            `
            WITH active_sessions AS (
              SELECT *
              FROM kyros_sessions

              WHERE last_seen_at >
                NOW()-(
                  $1*INTERVAL '1 second'
                )
            ),

            active_broadcasts AS (
              SELECT *
              FROM kyros_broadcasts

              WHERE ended_at IS NULL

                AND last_seen_at >
                  NOW()-(
                    $1*INTERVAL '1 second'
                  )
            )

            SELECT
              (
                SELECT COUNT(*)::int
                FROM kyros_installations
              )
                total_installations,

              (
                SELECT COUNT(
                  DISTINCT installation_id
                )::int
                FROM active_sessions
              )
                active_now,

              (
                SELECT COUNT(
                  DISTINCT installation_id
                )::int
                FROM kyros_sessions
                WHERE last_seen_at >
                  NOW()-INTERVAL '24 hours'
              )
                active_24h,

              (
                SELECT COUNT(*)::int
                FROM active_broadcasts
              )
                live_broadcasts,

              (
                SELECT
                  COALESCE(
                    SUM(
                      cardinality(
                        destinations
                      )
                    ),
                    0
                  )::int

                FROM active_broadcasts
              )
                live_destinations,

              (
                SELECT COUNT(*)::int
                FROM active_sessions
                WHERE is_recording
              )
                recording_now,

              (
                SELECT COUNT(*)::int
                FROM active_sessions
                WHERE is_screen_sharing
              )
                screen_sharing_now,

              (
                SELECT COUNT(*)::int
                FROM active_sessions
                WHERE is_remote_camera
              )
                remote_camera_feature_now,

              (
                SELECT COUNT(*)::int
                FROM kyros_remote_camera_sessions

                WHERE status='connected'

                  AND last_seen_at >
                    NOW()-(
                      $1*INTERVAL '1 second'
                    )
              )
                remote_links_connected,

              (
                SELECT COUNT(*)::int
                FROM kyros_issue_reports
                WHERE addressed=FALSE
              )
                open_issues,

              (
                SELECT COUNT(*)::int
                FROM kyros_issue_reports
              )
                total_issues
            `,
            [
              timeout,
            ]
          ),

          pool.query(`
            SELECT
              platform,

              COUNT(*)::int
                installations,

              COUNT(*) FILTER (
                WHERE last_seen_at >
                  NOW()-INTERVAL '24 hours'
              )::int
                seen_24h

            FROM kyros_installations

            GROUP BY platform

            ORDER BY
              installations DESC,
              platform ASC
          `),

          pool.query(
            `
            WITH a AS (
              SELECT DISTINCT
                installation_id

              FROM kyros_sessions

              WHERE last_seen_at >
                NOW()-(
                  $1*INTERVAL '1 second'
                )
            ),

            b AS (
              SELECT
                installation_id,

                COUNT(*)::int
                  live_broadcasts

              FROM kyros_broadcasts

              WHERE ended_at IS NULL

                AND last_seen_at >
                  NOW()-(
                    $1*INTERVAL '1 second'
                  )

              GROUP BY installation_id
            )

            SELECT
              COALESCE(
                i.last_network_country,
                'unknown'
              )
                country,

              COUNT(*)::int
                installations,

              COUNT(
                a.installation_id
              )::int
                active_now,

              COALESCE(
                SUM(
                  b.live_broadcasts
                ),
                0
              )::int
                live_broadcasts

            FROM kyros_installations i

            LEFT JOIN a
              ON a.installation_id=
                i.installation_id

            LEFT JOIN b
              ON b.installation_id=
                i.installation_id

            GROUP BY
              COALESCE(
                i.last_network_country,
                'unknown'
              )

            ORDER BY
              active_now DESC,
              installations DESC,
              country ASC
            `,
            [
              timeout,
            ]
          ),

          pool.query(`
            SELECT
              COALESCE(
                NULLIF(model,''),
                'Unknown'
              )
                model,

              COALESCE(
                NULLIF(manufacturer,''),
                'Unknown'
              )
                manufacturer,

              COUNT(*)::int
                installations

            FROM kyros_installations

            GROUP BY
              manufacturer,
              model

            ORDER BY
              installations DESC

            LIMIT 20
          `),

          pool.query(`
            SELECT
              COALESCE(
                NULLIF(app_version,''),
                'unknown'
              )
                app_version,

              COALESCE(
                NULLIF(app_build,''),
                'unknown'
              )
                app_build,

              COUNT(*)::int
                installations

            FROM kyros_installations

            GROUP BY
              app_version,
              app_build

            ORDER BY
              installations DESC

            LIMIT 20
          `),

          pool.query(
            `
            WITH latest AS (
              SELECT DISTINCT ON (
                s.installation_id
              )

                s.installation_id,
                s.session_id,
                s.last_seen_at,
                s.started_at,
                s.app_state,
                s.is_foreground,
                s.is_broadcasting,
                s.is_recording,
                s.is_screen_sharing,
                s.is_remote_camera,
                s.network_transport,
                s.internet_validated,
                s.battery_percent,
                s.charging,
                s.power_save

              FROM kyros_sessions s

              WHERE s.last_seen_at >
                NOW()-(
                  $1*INTERVAL '1 second'
                )

              ORDER BY
                s.installation_id,
                s.last_seen_at DESC
            )

            SELECT
              l.*,

              i.platform,
              i.manufacturer,
              i.brand,
              i.model,
              i.os_version,
              i.os_api,
              i.app_version,
              i.app_build,
              i.app_language,
              i.device_language,
              i.device_locale,
              i.device_region,
              i.timezone,
              i.screen_width,
              i.screen_height,
              i.screen_density,
              i.screen_refresh_rate,
              i.cpu_cores,
              i.total_memory_mb,
              i.low_ram_device,
              i.last_network_country,
              i.first_seen_at,
              i.clustered_data,

              COALESCE(
                ab.destinations,
                '{}'::text[]
              )
                destinations,

              ab.broadcast_id,

              ab.started_at
                broadcast_started_at

            FROM latest l

            JOIN kyros_installations i
              ON i.installation_id=
                l.installation_id

            LEFT JOIN LATERAL (
              SELECT
                broadcast_id,
                destinations,
                started_at

              FROM kyros_broadcasts b

              WHERE b.installation_id=
                l.installation_id

                AND b.ended_at IS NULL

                AND b.last_seen_at >
                  NOW()-(
                    $1*INTERVAL '1 second'
                  )

              ORDER BY
                b.started_at DESC

              LIMIT 1
            ) ab
              ON TRUE

            ORDER BY
              l.is_broadcasting DESC,
              l.is_recording DESC,
              l.last_seen_at DESC

            LIMIT $2
            `,
            [
              timeout,
              activeLimit,
            ]
          ),

          pool.query(
            `
            SELECT
              b.broadcast_id,
              b.installation_id,
              b.session_id,
              b.destinations,
              b.started_at,
              b.last_seen_at,

              i.platform,
              i.model,
              i.app_version,
              i.last_network_country,

              s.network_transport,
              s.battery_percent,
              s.charging

            FROM kyros_broadcasts b

            JOIN kyros_installations i
              ON i.installation_id=
                b.installation_id

            LEFT JOIN kyros_sessions s
              ON s.session_id=
                b.session_id

            WHERE b.ended_at IS NULL

              AND b.last_seen_at >
                NOW()-(
                  $1*INTERVAL '1 second'
                )

            ORDER BY
              b.started_at DESC

            LIMIT 100
            `,
            [
              timeout,
            ]
          ),

          pool.query(`
            SELECT
              r.remote_camera_id,
              r.status,
              r.connection_mode,
              r.relay_used,
              r.selected_candidate_type,
              r.round_trip_ms,
              r.packet_loss_percent,
              r.created_at,
              r.joined_at,
              r.connected_at,
              r.disconnected_at,
              r.last_seen_at,
              r.expires_at,
              r.studio_installation_id,
              r.camera_installation_id,

              si.platform
                studio_platform,

              si.model
                studio_model,

              si.last_network_country
                studio_country,

              ci.platform
                camera_platform,

              ci.model
                camera_model,

              ci.last_network_country
                camera_country

            FROM kyros_remote_camera_sessions r

            LEFT JOIN kyros_installations si
              ON si.installation_id=
                r.studio_installation_id

            LEFT JOIN kyros_installations ci
              ON ci.installation_id=
                r.camera_installation_id

            ORDER BY
              r.last_seen_at DESC

            LIMIT 100
          `),

          pool.query(
            `
            SELECT *
            FROM (
              SELECT
                'installation'
                  kind,

                i.installation_id::text
                  entity_id,

                i.first_seen_at
                  occurred_at,

                i.platform,

                COALESCE(
                  i.model,
                  ''
                )
                  label,

                COALESCE(
                  i.last_network_country,
                  ''
                )
                  country,

                NULL::text
                  detail

              FROM kyros_installations i

              UNION ALL

              SELECT
                'broadcast'
                  kind,

                b.broadcast_id::text,

                b.started_at,

                i.platform,

                COALESCE(
                  i.model,
                  ''
                )
                  label,

                COALESCE(
                  i.last_network_country,
                  ''
                )
                  country,

                array_to_string(
                  b.destinations,
                  ', '
                )
                  detail

              FROM kyros_broadcasts b

              JOIN kyros_installations i
                ON i.installation_id=
                  b.installation_id

              UNION ALL

              SELECT
                'issue'
                  kind,

                r.issue_id::text,

                r.created_at,

                COALESCE(
                  r.platform,
                  'unknown'
                ),

                COALESCE(
                  r.title,
                  ''
                )
                  label,

                COALESCE(
                  i.last_network_country,
                  ''
                )
                  country,

                CASE
                  WHEN r.addressed
                    THEN 'addressed'
                  ELSE 'open'
                END
                  detail

              FROM kyros_issue_reports r

              LEFT JOIN kyros_installations i
                ON i.installation_id=
                  r.installation_id

              UNION ALL

              SELECT
                'remote_camera'
                  kind,

                e.remote_camera_id::text,

                e.created_at,

                e.peer_role,

                COALESCE(
                  e.event_type,
                  ''
                )
                  label,

                ''
                  country,

                e.details::text
                  detail

              FROM kyros_remote_camera_events e
            ) x

            ORDER BY
              occurred_at DESC

            LIMIT $1
            `,
            [
              recentLimit,
            ]
          ),

          pool.query(`
            WITH hours AS (
              SELECT
                generate_series(
                  date_trunc(
                    'hour',
                    NOW()
                  )-
                    INTERVAL '23 hours',

                  date_trunc(
                    'hour',
                    NOW()
                  ),

                  INTERVAL '1 hour'
                )
                  hour
            ),

            counts AS (
              SELECT
                date_trunc(
                  'hour',
                  last_seen_at
                )
                  hour,

                COUNT(*)::int
                  sessions

              FROM kyros_sessions

              WHERE last_seen_at >=
                NOW()-INTERVAL '24 hours'

              GROUP BY 1
            )

            SELECT
              h.hour,

              COALESCE(
                c.sessions,
                0
              )::int
                sessions

            FROM hours h

            LEFT JOIN counts c
              USING(hour)

            ORDER BY
              h.hour
          `),

          pool.query(
            `
            SELECT
              d.destination,

              COUNT(*)::int
                live

            FROM kyros_broadcasts b

            CROSS JOIN LATERAL
              UNNEST(
                b.destinations
              )
              d(destination)

            WHERE b.ended_at IS NULL

              AND b.last_seen_at >
                NOW()-(
                  $1*INTERVAL '1 second'
                )

            GROUP BY
              d.destination

            ORDER BY
              live DESC,
              d.destination
            `,
            [
              timeout,
            ]
          ),

          pool.query(`
            SELECT
              COALESCE(
                NULLIF(
                  app_language,
                  ''
                ),
                'unknown'
              )
                language,

              COUNT(*)::int
                installations

            FROM kyros_installations

            GROUP BY
              app_language

            ORDER BY
              installations DESC

            LIMIT 20
          `),

          pool.query(`
            SELECT
              COUNT(*)::int
                total,

              COUNT(*) FILTER (
                WHERE addressed=FALSE
              )::int
                open,

              COUNT(*) FILTER (
                WHERE addressed=TRUE
              )::int
                addressed,

              COUNT(*) FILTER (
                WHERE created_at >
                  NOW()-INTERVAL '24 hours'
              )::int
                created_24h

            FROM kyros_issue_reports
          `),
        ]);

      res.set(
        "Cache-Control",
        "no-store"
      );

      res.json({
        ok: true,

        generatedAt:
          new Date()
            .toISOString(),

        activeTimeoutSeconds:
          timeout,

        summary:
          summary.rows[0],

        installations: {
          platforms:
            platforms.rows,

          countries:
            countries.rows,

          deviceModels:
            deviceModels.rows,

          versions:
            versions.rows,

          languages:
            languageStats.rows,
        },

        activeDevices:
          activeDevices.rows,

        liveProductions:
          liveProductions.rows,

        remoteCameras:
          remoteSessions.rows,

        recentActivity:
          activity.rows,

        activityTrend24h:
          trend.rows,

        destinations:
          destinationStats.rows,

        issues:
          issueStats.rows[0],

        server: {
          uptimeSeconds:
            Math.floor(
              process.uptime()
            ),

          websocketRooms:
            typeof remoteRooms !==
            "undefined"
              ? remoteRooms.size
              : 0,

          node:
            process.version,
        },
      });

    } catch (e) {

      console.error(
        "Admin dashboard error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "admin_dashboard_failed",
        });
    }
  }
);

/* ============================================================
   ADMIN STATS
   ============================================================ */

app.get(
  "/admin-stats",
  async (req, res) => {
    try {
      const [
        inst,
        active,
        platforms,
        countries,
        languages,
        broadcasts,
        dests,
        issues,
        remoteCameraStats,
      ] =
        await Promise.all([

          pool.query(`
            SELECT
              COUNT(*)::int
                total

            FROM kyros_installations
          `),

          pool.query(
            `
            SELECT
              COUNT(
                DISTINCT installation_id
              )::int
                total

            FROM kyros_sessions

            WHERE last_seen_at >
              NOW()-(
                $1*INTERVAL '1 second'
              )
            `,
            [
              ACTIVE_TIMEOUT_SECONDS,
            ]
          ),

          pool.query(`
            SELECT
              platform,

              COUNT(*)::int
                installations

            FROM kyros_installations

            GROUP BY platform

            ORDER BY
              installations DESC
          `),

          pool.query(`
            SELECT
              COALESCE(
                last_network_country,
                'unknown'
              )
                country,

              COUNT(*)::int
                installations

            FROM kyros_installations

            GROUP BY
              last_network_country

            ORDER BY
              installations DESC
          `),

          pool.query(`
            SELECT
              COALESCE(
                app_language,
                'unknown'
              )
                language,

              COUNT(*)::int
                installations

            FROM kyros_installations

            GROUP BY
              app_language

            ORDER BY
              installations DESC
          `),

          pool.query(`
            SELECT
              COUNT(*)::int
                total

            FROM kyros_broadcasts
          `),

          pool.query(`
            SELECT
              destination,

              COUNT(*)::int
                broadcasts

            FROM
              kyros_broadcasts,
              UNNEST(
                destinations
              )
              destination

            GROUP BY
              destination

            ORDER BY
              broadcasts DESC
          `),

          pool.query(`
            SELECT
              COUNT(*)::int
                total,

              COUNT(*) FILTER (
                WHERE addressed=FALSE
              )::int
                open,

              COUNT(*) FILTER (
                WHERE addressed=TRUE
              )::int
                addressed

            FROM kyros_issue_reports
          `),

          pool.query(`
            SELECT
              COUNT(*)::int
                total,

              COUNT(*) FILTER (
                WHERE status='waiting'
              )::int
                waiting,

              COUNT(*) FILTER (
                WHERE status='joined'
              )::int
                joined,

              COUNT(*) FILTER (
                WHERE status='connected'
              )::int
                connected,

              COUNT(*) FILTER (
                WHERE relay_used=TRUE
              )::int
                relayed

            FROM kyros_remote_camera_sessions
          `),
        ]);

      res.json({
        ok: true,

        generatedAt:
          new Date()
            .toISOString(),

        installations: {
          total:
            inst.rows[0]
              .total,

          activeSessions:
            active.rows[0]
              .total,

          platforms:
            platforms.rows,

          countries:
            countries.rows,

          languages:
            languages.rows,
        },

        broadcasts: {
          total:
            broadcasts.rows[0]
              .total,

          destinations:
            dests.rows,
        },

        issues:
          issues.rows[0],

        remoteCamera:
          remoteCameraStats.rows[0],
      });

    } catch (e) {

      console.error(
        "Stats error:",
        e
      );

      res
        .status(500)
        .json({
          ok: false,

          error:
            "stats_failed",
        });
    }
  }
);

/* ============================================================
   HEALTH
   ============================================================ */

app.get(
  "/health",
  async (req, res) => {
    try {
      await pool.query(
        "SELECT 1"
      );

      res.json({
        ok: true,

        service:
          "KyroS",

        database:
          "connected",

        remoteCameraSignaling:
          "ready",

        turnConfigured:
          TURN_CONFIGURED,

        turnMode:
          COTURN_CONFIGURED
            ? "coturn-hmac"
            : (
                EXTERNAL_TURN_CONFIGURED
                  ? "external"
                  : "none"
              ),

        stunConfigured:
          EFFECTIVE_STUN_URLS.length > 0,

        stunProvider:
          STUN_URLS.length
            ? "custom"
            : "google-public",

        stunServers:
          EFFECTIVE_STUN_URLS,

        serverTime:
          new Date()
            .toISOString(),
      });

    } catch (_) {

      res
        .status(503)
        .json({
          ok: false,

          service:
            "KyroS",

          database:
            "unavailable",
        });
    }
  }
);

/* ============================================================
   HTTP + WEBSOCKET SIGNALING SERVER
   ============================================================ */

const server =
  http.createServer(app);

const wss =
  new WebSocketServer({
    noServer: true,

    maxPayload:
      64 * 1024,
  });

/*
  remoteCameraId ->

  {
    studio: WebSocket|null,
    camera: WebSocket|null
  }
*/

const remoteRooms =
  new Map();

function sendWs(
  ws,
  payload
) {
  if (
    ws &&
    ws.readyState ===
      WebSocket.OPEN
  ) {
    ws.send(
      JSON.stringify(
        payload
      )
    );
  }
}

function getRemoteRoom(
  remoteCameraId
) {
  let room =
    remoteRooms.get(
      remoteCameraId
    );

  if (!room) {
    room = {
      studio: null,
      camera: null,
    };

    remoteRooms.set(
      remoteCameraId,
      room
    );
  }

  return room;
}

function closeRemoteRoom(
  remoteCameraId,
  reason = "closed"
) {
  const room =
    remoteRooms.get(
      remoteCameraId
    );

  if (!room) {
    return;
  }

  for (
    const ws of [
      room.studio,
      room.camera,
    ]
  ) {
    if (
      ws &&
      ws.readyState ===
        WebSocket.OPEN
    ) {
      try {
        sendWs(
          ws,
          {
            type:
              "session_closed",

            reason,
          }
        );

        ws.close(
          1000,
          reason
        );

      } catch (_) {}
    }
  }

  remoteRooms.delete(
    remoteCameraId
  );
}

async function markRemoteConnected(
  remoteCameraId
) {
  await pool.query(
    `
    UPDATE kyros_remote_camera_sessions

    SET
      status='connected',

      connected_at=
        COALESCE(
          connected_at,
          NOW()
        ),

      disconnected_at=NULL,

      last_seen_at=NOW(),

      updated_at=NOW()

    WHERE remote_camera_id=$1

      AND status IN (
        'joined',
        'disconnected',
        'connected'
      )
    `,
    [
      remoteCameraId,
    ]
  );

  await logRemoteEvent(
    remoteCameraId,
    "server",
    "peers_connected",
    {}
  );
}

async function markRemoteDisconnected(
  remoteCameraId,
  role
) {
  await pool.query(
    `
    UPDATE kyros_remote_camera_sessions

    SET
      status=
        CASE
          WHEN status IN (
            'cancelled',
            'expired'
          )
            THEN status

          ELSE 'disconnected'
        END,

      disconnected_at=
        CASE
          WHEN status IN (
            'cancelled',
            'expired'
          )
            THEN disconnected_at

          ELSE NOW()
        END,

      last_seen_at=NOW(),

      updated_at=NOW()

    WHERE remote_camera_id=$1
    `,
    [
      remoteCameraId,
    ]
  );

  await logRemoteEvent(
    remoteCameraId,
    role,
    "peer_disconnected",
    {}
  );
}

/* ============================================================
   WEBSOCKET UPGRADE
   ============================================================ */

server.on(
  "upgrade",
  async (
    req,
    socket,
    head
  ) => {
    try {
      const requestUrl =
        new URL(
          req.url,
          "http://localhost"
        );

      if (
        requestUrl.pathname !==
        "/remote-camera/ws"
      ) {
        socket.destroy();
        return;
      }

      const remoteCameraId =
        requestUrl
          .searchParams
          .get("session");

      const role =
        requestUrl
          .searchParams
          .get("role");

      const token =
        requestUrl
          .searchParams
          .get("token");

      const session =
        await authenticateRemotePeer(
          remoteCameraId,
          role,
          token
        );

      if (!session) {
        socket.write(
          "HTTP/1.1 401 Unauthorized\r\n\r\n"
        );

        socket.destroy();

        return;
      }

      wss.handleUpgrade(
        req,
        socket,
        head,
        ws => {
          ws.kyrosRemoteCameraId =
            remoteCameraId;

          ws.kyrosRole =
            role;

          ws.isAlive =
            true;

          wss.emit(
            "connection",
            ws,
            req
          );
        }
      );

    } catch (e) {

      console.error(
        "WebSocket upgrade error:",
        e
      );

      socket.destroy();
    }
  }
);

/* ============================================================
   WEBSOCKET CONNECTION
   ============================================================ */

wss.on(
  "connection",
  async ws => {
    const remoteCameraId =
      ws.kyrosRemoteCameraId;

    const role =
      ws.kyrosRole;

    const otherRole =
      role === "studio"
        ? "camera"
        : "studio";

    const room =
      getRemoteRoom(
        remoteCameraId
      );

    /*
      Only one active WebSocket
      per role.

      A reconnect replaces
      the old connection.
    */

    if (
      room[role] &&
      room[role] !== ws
    ) {
      try {
        room[role].close(
          4001,
          "replaced_by_reconnect"
        );
      } catch (_) {}
    }

    room[role] =
      ws;

    await pool.query(
      `
      UPDATE kyros_remote_camera_sessions

      SET
        last_seen_at=NOW(),
        updated_at=NOW()

      WHERE remote_camera_id=$1
      `,
      [
        remoteCameraId,
      ]
    );

    await logRemoteEvent(
      remoteCameraId,
      role,
      "websocket_connected",
      {}
    );

    sendWs(
      ws,
      {
        type:
          "ready",

        remoteCameraId,

        role,
      }
    );

    /*
      Both peers are now present.
    */

    if (
      room.studio &&
      room.camera
    ) {
      await markRemoteConnected(
        remoteCameraId
      );

      sendWs(
        room.studio,
        {
          type:
            "peer_ready",

          peerRole:
            "camera",
        }
      );

      sendWs(
        room.camera,
        {
          type:
            "peer_ready",

          peerRole:
            "studio",
        }
      );
    }

    ws.on(
      "pong",
      () => {
        ws.isAlive =
          true;
      }
    );

    ws.on(
      "message",
      async raw => {
        try {
          const text =
            raw.toString();

          if (
            text.length >
            64 * 1024
          ) {
            ws.close(
              1009,
              "message_too_large"
            );

            return;
          }

          const msg =
            JSON.parse(
              text
            );

          const type =
            str(
              msg?.type,
              30
            );

          if (
            !ALLOWED_SIGNAL_TYPES
              .has(type)
          ) {
            sendWs(
              ws,
              {
                type:
                  "error",

                error:
                  "unsupported_signal_type",
              }
            );

            return;
          }

          if (
            type === "ping"
          ) {
            sendWs(
              ws,
              {
                type:
                  "pong",

                time:
                  Date.now(),
              }
            );

            return;
          }

          const currentRoom =
            remoteRooms.get(
              remoteCameraId
            );

          const peer =
            currentRoom
              ?.[otherRole];

          if (
            !peer ||
            peer.readyState !==
              WebSocket.OPEN
          ) {
            sendWs(
              ws,
              {
                type:
                  "peer_unavailable",

                peerRole:
                  otherRole,
              }
            );

            return;
          }

          /*
            Relay signaling/control only.

            Camera video/audio is NOT sent
            through this WebSocket.

            Media travels through WebRTC.
          */

          sendWs(
            peer,
            {
              ...msg,

              from:
                role,
            }
          );

          await pool.query(
            `
            UPDATE kyros_remote_camera_sessions

            SET
              last_seen_at=NOW(),

              updated_at=NOW()

            WHERE remote_camera_id=$1
            `,
            [
              remoteCameraId,
            ]
          );

          if (
            type === "hangup"
          ) {
            await logRemoteEvent(
              remoteCameraId,
              role,
              "hangup",
              {}
            );
          }

        } catch (e) {

          sendWs(
            ws,
            {
              type:
                "error",

              error:
                "invalid_signal_message",
            }
          );
        }
      }
    );

    ws.on(
      "close",
      async () => {
        const currentRoom =
          remoteRooms.get(
            remoteCameraId
          );

        if (
          currentRoom &&
          currentRoom[role] ===
            ws
        ) {
          currentRoom[role] =
            null;
        }

        if (currentRoom) {
          sendWs(
            currentRoom[
              otherRole
            ],
            {
              type:
                "peer_disconnected",

              peerRole:
                role,
            }
          );

          if (
            !currentRoom.studio &&
            !currentRoom.camera
          ) {
            remoteRooms.delete(
              remoteCameraId
            );
          }
        }

        try {
          await markRemoteDisconnected(
            remoteCameraId,
            role
          );

        } catch (e) {

          console.warn(
            "Failed to mark remote disconnect:",
            e.message
          );
        }
      }
    );
  }
);

/* ============================================================
   WEBSOCKET HEARTBEAT
   ============================================================ */

const websocketHeartbeat =
  setInterval(
    () => {
      for (
        const ws of
        wss.clients
      ) {
        if (
          ws.isAlive ===
          false
        ) {
          try {
            ws.terminate();
          } catch (_) {}

          continue;
        }

        ws.isAlive =
          false;

        try {
          ws.ping();
        } catch (_) {}
      }
    },
    30000
  );

/* ============================================================
   EXPIRED REMOTE CAMERA CLEANUP
   ============================================================ */

const remoteCameraCleanup =
  setInterval(
    async () => {
      try {
        const expired =
          await pool.query(`
            UPDATE kyros_remote_camera_sessions

            SET
              connection_code=NULL,

              status='expired',

              updated_at=NOW()

            WHERE expires_at<=NOW()

              AND status IN (
                'waiting',
                'joined',
                'disconnected'
              )

            RETURNING
              remote_camera_id
          `);

        for (
          const row of
          expired.rows
        ) {
          closeRemoteRoom(
            row.remote_camera_id,
            "expired"
          );

          await logRemoteEvent(
            row.remote_camera_id,
            "server",
            "session_expired",
            {}
          );
        }

      } catch (e) {

        console.warn(
          "Remote camera cleanup failed:",
          e.message
        );
      }
    },
    60000
  );

wss.on(
  "close",
  () => {
    clearInterval(
      websocketHeartbeat
    );

    clearInterval(
      remoteCameraCleanup
    );
  }
);

/* ============================================================
   AUTHENTICATION + USER ACCOUNTS
   ============================================================ */

const AUTH_ISSUER = "https://api.kyros.app";
const AUTH_AUDIENCE = "kyros-api";
const AUTH_ACCESS_TTL_SECONDS = 15 * 60;
const AUTH_REFRESH_TTL_SECONDS = 30 * 24 * 60 * 60;
const AUTH_MAX_SESSIONS_PER_USER = 10;
const OTP_TTL_SECONDS = 10 * 60;
const OTP_RESEND_COOLDOWN_SECONDS = 60;
const OTP_MAX_ATTEMPTS = 5;
const PASSWORD_RESET_TOKEN_TTL_SECONDS = 10 * 60;
const FREE_TRIAL_DURATION_DAYS = 7;
const SUBSCRIPTION_CURRENCY = String(process.env.KYROS_CURRENCY || "KES").trim().toUpperCase();
const PAYMENT_SIMULATION_ENABLED =
  String(process.env.PAYMENT_SIMULATION_ENABLED || "false").toLowerCase() === "true" &&
  String(process.env.NODE_ENV || "development").toLowerCase() !== "production" &&
  String(process.env.VERCEL_ENV || "").toLowerCase() !== "production" &&
  String(process.env.KYROS_DATABASE_ENV || "").toLowerCase() === "development" &&
  DATABASE_URL.length > 0 &&
  !IS_KNOWN_PRODUCTION_DATABASE;
const SUBSCRIPTION_CYCLES = Object.freeze({
  monthly: { durationMonths: 1, priceEnv: "KYROS_PRO_MONTHLY_PRICE", label: "Monthly" },
  quarterly: { durationMonths: 3, priceEnv: "KYROS_PRO_QUARTERLY_PRICE", label: "Quarterly" },
  yearly: { durationMonths: 12, priceEnv: "KYROS_PRO_YEARLY_PRICE", label: "Yearly" },
});
const AUTH_JWT_SECRET = String(process.env.AUTH_JWT_SECRET || "");
const AUTH_ACCESS_SECRET = AUTH_JWT_SECRET
  ? crypto.createHmac("sha256", AUTH_JWT_SECRET).update("kyros-access-v1").digest()
  : null;
const AUTH_REFRESH_SECRET = AUTH_JWT_SECRET
  ? crypto.createHmac("sha256", AUTH_JWT_SECRET).update("kyros-refresh-v1").digest()
  : null;
const GOOGLE_CLIENT_IDS = new Set(
  String([
    process.env.GOOGLE_CLIENT_IDS || "",
    process.env.GOOGLE_WEB_CLIENT_ID || "",
    process.env.GOOGLE_ANDROID_CLIENT_ID || "",
  ].join(","))
    .split(",")
    .map(value => value.trim())
    .filter(Boolean)
);

function authConfigured(req, res, next) {
  if (
    !AUTH_ACCESS_SECRET || !AUTH_REFRESH_SECRET ||
    Buffer.byteLength(AUTH_JWT_SECRET, "utf8") < 64
  ) {
    return res.status(503).json({
      ok: false,
      error: "authentication_not_configured",
      message: "Set AUTH_JWT_SECRET to a random secret of at least 64 characters.",
    });
  }
  return next();
}

function createPublicId(prefix) {
  return `${prefix}${crypto.randomBytes(16).toString("hex")}`;
}

function normalizeEmail(value) {
  if (typeof value !== "string") return null;
  const email = value.trim().toLowerCase();
  if (email.length > 320 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return null;
  return email;
}

const BCRYPT_ROUNDS = 12;

function validatePassword(value) {
  return typeof value === "string" &&
    value.length >= 10 &&
    Buffer.byteLength(value, "utf8") <= 72 &&
    !/[\u0000-\u001f\u007f]/.test(value);
}

async function hashPassword(password) {
  return bcrypt.hash(password, BCRYPT_ROUNDS);
}

// Compatibility for accounts created by the previous pre-release backend.
// New passwords are bcrypt-hashed; successful legacy logins are upgraded.
const scryptAsync = (password, salt, keyLength, options) =>
  new Promise((resolve, reject) => {
    crypto.scrypt(password, salt, keyLength, options, (error, derivedKey) => {
      if (error) reject(error);
      else resolve(derivedKey);
    });
  });

async function verifyLegacyScryptPassword(password, encoded) {
  const parts = encoded.split("$");
  if (parts.length !== 6 || parts[0] !== "scrypt") return false;
  const cost = Number(parts[1]);
  const blockSize = Number(parts[2]);
  const parallelization = Number(parts[3]);
  if (
    !Number.isInteger(cost) || cost < 16384 || cost > 131072 ||
    !Number.isInteger(blockSize) || blockSize < 1 || blockSize > 16 ||
    !Number.isInteger(parallelization) || parallelization < 1 || parallelization > 4
  ) return false;
  let salt;
  let expected;
  try {
    salt = Buffer.from(parts[4], "base64url");
    expected = Buffer.from(parts[5], "base64url");
  } catch (_) {
    return false;
  }
  if (salt.length < 16 || expected.length !== 64) return false;
  const actual = await scryptAsync(password, salt, expected.length, {
    N: cost,
    r: blockSize,
    p: parallelization,
    maxmem: 64 * 1024 * 1024,
  });
  return crypto.timingSafeEqual(actual, expected);
}

async function verifyPassword(password, encoded) {
  if (typeof encoded !== "string") return false;
  if (/^\$2[aby]\$/.test(encoded)) return bcrypt.compare(password, encoded);
  if (encoded.startsWith("scrypt$")) return verifyLegacyScryptPassword(password, encoded);
  return false;
}

function signJwt(payload, secret, ttlSeconds) {
  return jwt.sign(payload, secret, {
    algorithm: "HS256",
    issuer: AUTH_ISSUER,
    audience: AUTH_AUDIENCE,
    expiresIn: ttlSeconds,
  });
}

function verifyJwt(token, secret, expectedUse) {
  if (typeof token !== "string" || token.length > 8192) throw new Error("invalid_token");
  try {
    const claims = jwt.verify(token, secret, {
      algorithms: ["HS256"],
      issuer: AUTH_ISSUER,
      audience: AUTH_AUDIENCE,
      clockTolerance: 5,
    });
    if (
      !claims || typeof claims !== "object" ||
      claims.token_use !== expectedUse ||
      typeof claims.sub !== "string" ||
      typeof claims.sid !== "string" ||
      typeof claims.jti !== "string"
    ) throw new Error("invalid_token");
    return claims;
  } catch (_) {
    throw new Error("invalid_token");
  }
}

function sha256Hex(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function otpDigest(email, purpose, code) {
  return crypto.createHmac("sha256", AUTH_JWT_SECRET)
    .update(`${purpose}:${email}:${code}`)
    .digest("hex");
}

function timingSafeStringEqual(left, right) {
  if (typeof left !== "string" || typeof right !== "string") return false;
  const a = Buffer.from(left, "utf8");
  const b = Buffer.from(right, "utf8");
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function safeJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      return [];
    }
  }
  return [];
}

function parseJsonObject(value) {
  if (value && typeof value === "object" && !Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
    } catch (_) {
      return null;
    }
  }
  return null;
}

function storedFreeTrial(user) {
  const value = parseJsonObject(user && user.free_trial_used);
  return value && Object.keys(value).length > 0 ? value : null;
}

function publicFreeTrial(user) {
  const trial = storedFreeTrial(user);
  if (!trial) return null;
  const result = { ...trial };
  const endValue = result.endsAt || result.ends_at || result.endDate || null;
  const endTime = endValue ? Date.parse(endValue) : NaN;
  if (Number.isFinite(endTime) && endTime <= Date.now() && result.status !== "revoked") {
    result.status = "expired";
  }
  return result;
}

function publicSubscription(user) {
  let data = parseJsonObject(user && user.subscription_data) || {};
  const storedPlan = typeof data.plan === "string" ? data.plan.toLowerCase() : "free";
  let plan = storedPlan;
  let status = user && user.subscription_status ? user.subscription_status : "free";
  let startDate = user && user.subscription_start ? user.subscription_start : null;
  let endDate = user && user.subscription_end ? user.subscription_end : null;
  let accessType = plan === "pro" || plan === "professional" ? "paid" : "free";
  const trial = publicFreeTrial(user || {});
  const trialEnd = trial && Date.parse(trial.endsAt || trial.ends_at || "");
  const trialActive = Boolean(
    trial && trial.status !== "revoked" && Number.isFinite(trialEnd) && trialEnd > Date.now()
  );
  const paidActive = (plan === "pro" || plan === "professional") &&
    status === "active" && (!endDate || Date.parse(endDate) > Date.now());

  if (paidActive) {
    accessType = "paid";
  } else if (trialActive) {
    plan = "pro";
    status = "active";
    startDate = trial.startedAt || trial.startDate || null;
    endDate = trial.endsAt || trial.endDate || null;
    accessType = "free_trial";
  } else if (status === "active" && endDate && Date.parse(endDate) <= Date.now()) {
    status = "expired";
  }

  return {
    plan,
    status,
    startDate,
    endDate,
    autoRenew: Boolean(user && user.auto_renew),
    accessType,
    isTrialActive: trialActive && !paidActive,
    canStartFreeTrial: !trial && !paidActive,
    freeTrial: trial,
  };
}

function publicUser(user) {
  const trial = publicFreeTrial(user || {});
  const trialUsed = Boolean(storedFreeTrial(user || {}));
  const subscription = publicSubscription(user || {});
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    phoneNumber: user.phone_number || null,
    signInMethod: user.sign_in_method,
    role: user.role,
    accountStatus: user.account_status,
    subscription,
    emailVerified: Boolean(user.email_verified),
    phoneVerified: Boolean(user.phone_verified),
    country: user.country || null,
    profileImage: user.profile_image || null,
    freeTrialUsed: trialUsed,
    hasUsedFreeTrial: trialUsed,
    canStartFreeTrial: subscription.canStartFreeTrial,
    freeTrial: trial,
    createdAt: user.created_at,
    lastLoginAt: user.last_login_at || null,
  };
}

function createTokenPair(user, sessionId) {
  const common = {
    sub: user.id,
    sid: sessionId,
    ver: Number(user.token_version || 1),
    jti: crypto.randomUUID(),
  };
  const accessToken = signJwt(
    { ...common, token_use: "access" }, AUTH_ACCESS_SECRET, AUTH_ACCESS_TTL_SECONDS
  );
  const refreshToken = signJwt(
    { ...common, token_use: "refresh" }, AUTH_REFRESH_SECRET, AUTH_REFRESH_TTL_SECONDS
  );
  return { accessToken, refreshToken };
}

async function createStoredSession(client, userId) {
  const result = await client.query(
    `SELECT id, token_version, refresh_sessions, account_status
       FROM users
      WHERE id = $1 AND deleted_at IS NULL
      FOR UPDATE`,
    [userId]
  );
  if (result.rowCount !== 1 || result.rows[0].account_status !== "active") {
    throw new Error("account_unavailable");
  }
  const user = result.rows[0];
  const now = Date.now();
  let sessions = safeJsonArray(user.refresh_sessions).filter(session =>
    session && typeof session.sid === "string" &&
    typeof session.expiresAt === "string" && Date.parse(session.expiresAt) > now
  );
  while (sessions.length >= AUTH_MAX_SESSIONS_PER_USER) sessions.shift();
  const sid = crypto.randomUUID();
  const tokens = createTokenPair(user, sid);
  sessions.push({
    sid,
    tokenHash: sha256Hex(tokens.refreshToken),
    expiresAt: new Date(now + AUTH_REFRESH_TTL_SECONDS * 1000).toISOString(),
    createdAt: new Date(now).toISOString(),
  });
  await client.query(
    `UPDATE users SET refresh_sessions = $2::jsonb, updated_at = NOW() WHERE id = $1`,
    [userId, JSON.stringify(sessions)]
  );
  return tokens;
}

function authRateLimit(options = {}) {
  const windowMs = options.windowMs || 15 * 60 * 1000;
  const max = options.max || 10;
  const buckets = new Map();
  return (req, res, next) => {
    const now = Date.now();
    const key = String(req.ip || req.socket.remoteAddress || "unknown");
    let bucket = buckets.get(key);
    if (!bucket || bucket.resetAt <= now) {
      bucket = { count: 0, resetAt: now + windowMs };
      buckets.set(key, bucket);
    }
    bucket.count += 1;
    if (buckets.size > 10000) {
      for (const [ip, entry] of buckets) if (entry.resetAt <= now) buckets.delete(ip);
    }
    res.set("RateLimit-Limit", String(max));
    res.set("RateLimit-Remaining", String(Math.max(0, max - bucket.count)));
    res.set("RateLimit-Reset", String(Math.ceil(bucket.resetAt / 1000)));
    if (bucket.count > max) {
      return res.status(429).json({
        ok: false,
        error: "too_many_requests",
        message: "Too many requests. Please try again later.",
      });
    }
    return next();
  };
}

const loginRateLimit = authRateLimit({ max: 10 });
const signupRateLimit = authRateLimit({ max: 5 });
const googleRateLimit = authRateLimit({ max: 15 });
const otpRateLimit = authRateLimit({ max: 5, windowMs: 15 * 60 * 1000 });
const passwordResetRateLimit = authRateLimit({ max: 5, windowMs: 15 * 60 * 1000 });
const subscriptionCheckoutRateLimit = authRateLimit({ max: 8, windowMs: 60 * 60 * 1000 });

async function requireAuth(req, res, next) {
  if (!AUTH_ACCESS_SECRET || !AUTH_REFRESH_SECRET || Buffer.byteLength(AUTH_JWT_SECRET, "utf8") < 64) {
    return res.status(503).json({
      ok: false,
      error: "authentication_not_configured",
      message: "Set AUTH_JWT_SECRET to a random secret of at least 64 characters.",
    });
  }
  try {
    const authorization = String(req.headers.authorization || "");
    const match = authorization.match(/^Bearer\s+(.+)$/i);
    if (!match) return res.status(401).json({ ok: false, error: "authentication_required" });
    const claims = verifyJwt(match[1], AUTH_ACCESS_SECRET, "access");
    const result = await pool.query(
      `SELECT id, name, email, phone_number, sign_in_method, role,
              account_status, subscription_status, subscription_start,
              subscription_end, auto_renew, subscription_data,
              email_verified, phone_verified, country, profile_image,
              free_trial_used, created_at, last_login_at, token_version,
              EXISTS (
                SELECT 1 FROM jsonb_array_elements(refresh_sessions) AS session
                 WHERE session->>'sid' = $2
                   AND (session->>'expiresAt')::timestamptz > NOW()
              ) AS session_active
         FROM users
        WHERE id = $1 AND deleted_at IS NULL
        LIMIT 1`,
      [claims.sub, claims.sid]
    );
    if (result.rowCount !== 1) return res.status(401).json({ ok: false, error: "invalid_session" });
    const user = result.rows[0];
    if (
      user.account_status !== "active" ||
      Number(user.token_version) !== Number(claims.ver) ||
      !user.session_active
    ) return res.status(401).json({ ok: false, error: "session_expired" });
    req.auth = { claims, user };
    return next();
  } catch (error) {
    if (error.message === "invalid_token") {
      return res.status(401).json({ ok: false, error: "invalid_or_expired_token" });
    }
    console.error("Authentication middleware error:", error.message);
    return res.status(500).json({ ok: false, error: "authentication_unavailable" });
  }
}

const googleAuthClient = new OAuth2Client();

async function verifyGoogleIdToken(idToken) {
  if (GOOGLE_CLIENT_IDS.size === 0) {
    const error = new Error("google_sign_in_not_configured");
    error.statusCode = 503;
    throw error;
  }
  if (typeof idToken !== "string" || idToken.length > 16384) {
    const error = new Error("invalid_google_token");
    error.statusCode = 401;
    throw error;
  }
  let payload;
  try {
    const ticket = await googleAuthClient.verifyIdToken({
      idToken,
      audience: Array.from(GOOGLE_CLIENT_IDS),
    });
    payload = ticket.getPayload();
  } catch (verificationError) {
    const networkFailure = Boolean(verificationError && (
      verificationError.code === "ECONNRESET" || verificationError.code === "ECONNREFUSED" ||
      verificationError.code === "ETIMEDOUT" || verificationError.code === "EAI_AGAIN" ||
      verificationError.code === "ENOTFOUND" || verificationError.code === "ECONNABORTED" ||
      verificationError.response
    ));
    const error = new Error(networkFailure ? "google_keys_unavailable" : "invalid_google_token");
    error.statusCode = networkFailure ? 503 : 401;
    throw error;
  }
  const email = normalizeEmail(payload && payload.email);
  const emailVerified = payload && (
    payload.email_verified === true || payload.email_verified === "true"
  );
  const authorizedPartyValid = !payload || !payload.azp || GOOGLE_CLIENT_IDS.has(payload.azp);
  if (
    !payload || typeof payload.sub !== "string" || payload.sub.length < 5 ||
    payload.sub.length > 255 || !email || !emailVerified || !authorizedPartyValid
  ) {
    const error = new Error("invalid_google_token");
    error.statusCode = 401;
    throw error;
  }
  let picture = null;
  if (typeof payload.picture === "string" && payload.picture.length <= 2048) {
    try {
      const pictureUrl = new URL(payload.picture);
      if (pictureUrl.protocol === "https:") picture = pictureUrl.toString();
    } catch (_) {
      picture = null;
    }
  }
  return {
    sub: payload.sub,
    email,
    name: typeof payload.name === "string" && payload.name.trim()
      ? payload.name.trim().slice(0, 160)
      : email.split("@")[0],
    picture,
  };
}

function authErrorResponse(res, error, fallback = "authentication_failed") {
  const status = Number.isInteger(error.statusCode) ? error.statusCode : 500;
  const known = new Set([
    "google_sign_in_not_configured", "invalid_google_token", "google_keys_unavailable",
    "google_keys_invalid", "account_unavailable", "email_delivery_not_configured",
    "email_delivery_failed", "otp_resend_too_soon",
  ]);
  const code = known.has(error.message) ? error.message : fallback;
  const messages = {
    google_sign_in_not_configured: "Google Sign-In is not configured on the server.",
    invalid_google_token: "Google could not verify this sign-in. Please try again.",
    google_keys_unavailable: "Google verification is temporarily unavailable.",
    email_delivery_not_configured: "Email delivery is not configured on the server.",
    email_delivery_failed: "The verification email could not be sent. Please try again shortly.",
    otp_resend_too_soon: "Please wait before requesting another code.",
    account_unavailable: "This account is unavailable.",
  };
  return res.status(status).json({ ok: false, error: code, message: messages[code] || "The authentication request could not be completed." });
}

function escapeHtml(value) {
  return String(value || "").replace(/[&<>"']/g, character => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[character]);
}

function smtpConfiguration() {
  const host = String(process.env.SMTP_HOST || "").trim();
  const port = Number(process.env.SMTP_PORT || 587);
  const from = String(process.env.SMTP_FROM || process.env.MAIL_FROM || "").trim();
  const user = String(process.env.SMTP_USER || "").trim();
  const pass = String(process.env.SMTP_PASS || "");
  if (!host || !Number.isInteger(port) || port < 1 || port > 65535 || !from || Boolean(user) !== Boolean(pass)) {
    return null;
  }
  return {
    host,
    port,
    secure: String(process.env.SMTP_SECURE || (port === 465 ? "true" : "false")).toLowerCase() === "true",
    auth: user ? { user, pass } : undefined,
    from,
    replyTo: String(process.env.SMTP_REPLY_TO || "").trim() || undefined,
  };
}

let mailTransporter = null;
let mailTransporterKey = null;

function getMailTransporter() {
  const config = smtpConfiguration();
  if (!config) {
    const error = new Error("email_delivery_not_configured");
    error.statusCode = 503;
    throw error;
  }
  const key = JSON.stringify({
    host: config.host, port: config.port, secure: config.secure,
    user: config.auth && config.auth.user, from: config.from,
  });
  if (!mailTransporter || mailTransporterKey !== key) {
    mailTransporter = nodemailer.createTransport({
      host: config.host,
      port: config.port,
      secure: config.secure,
      auth: config.auth,
      connectionTimeout: 10000,
      greetingTimeout: 10000,
      socketTimeout: 20000,
      pool: true,
      maxConnections: 3,
      maxMessages: 100,
    });
    mailTransporterKey = key;
  }
  return { transporter: mailTransporter, config };
}

async function sendOtpEmail({ email, name, code, purpose }) {
  const { transporter, config } = getMailTransporter();
  const displayName = escapeHtml(name || "KyroS Creator");
  const isReset = purpose === "password_reset";
  const subject = isReset ? "Your KyroS password reset code" : "Verify your KyroS email address";
  const heading = isReset ? "Reset your password" : "Verify your email";
  const instruction = isReset
    ? "Use this one-time code to continue resetting your KyroS password."
    : "Use this one-time code to verify your email address and activate your KyroS account.";
  const text = `Hello ${name || "KyroS Creator"},\n\n${instruction}\n\nYour verification code is: ${code}\n\nThis code expires in 10 minutes and can only be used once. If you did not request this, you can ignore this email.\n\nKyroS Support`;
  const html = `<!doctype html><html><body style="margin:0;padding:32px;background:#0a0e16;color:#f4f5fb;font-family:Arial,sans-serif"><div style="max-width:520px;margin:auto;padding:32px;background:#121827;border:1px solid #273149;border-radius:14px"><div style="font-size:13px;letter-spacing:3px;color:#a89cff;font-weight:bold">KYROS</div><h1 style="font-size:25px;margin:24px 0 12px">${heading}</h1><p style="color:#c4cada;line-height:1.6">Hello ${displayName}, ${instruction}</p><div style="margin:28px 0;padding:18px;text-align:center;background:#0a0e16;border:1px solid #343d59;border-radius:10px;font-size:32px;letter-spacing:9px;font-weight:bold">${code}</div><p style="color:#aeb6c8;line-height:1.6">This code expires in <strong>10 minutes</strong> and can only be used once. If you did not request this, you can safely ignore this email.</p><p style="margin-top:28px;color:#a89cff">KyroS Support</p></div></body></html>`;
  await transporter.sendMail({
    from: config.from,
    to: email,
    replyTo: config.replyTo,
    subject,
    text,
    html,
  });
}

async function issueEmailOtp({ userId, email, name, purpose }) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail || !["email_verification", "password_reset"].includes(purpose)) {
    throw new Error("invalid_otp_request");
  }
  // Fail before creating a code if SMTP is not configured.
  getMailTransporter();
  const code = String(crypto.randomInt(0, 1000000)).padStart(6, "0");
  const codeHash = otpDigest(normalizedEmail, purpose, code);
  const client = await pool.connect();
  let otpId = null;
  try {
    await client.query("BEGIN");
    const recent = await client.query(
      `SELECT id FROM kyros_auth_otps
        WHERE user_id = $1 AND purpose = $2
          AND created_at > NOW() - ($3::text || ' seconds')::interval
        ORDER BY created_at DESC LIMIT 1 FOR UPDATE`,
      [userId, purpose, OTP_RESEND_COOLDOWN_SECONDS]
    );
    if (recent.rowCount) {
      await client.query("ROLLBACK");
      const error = new Error("otp_resend_too_soon");
      error.statusCode = 429;
      throw error;
    }
    await client.query(
      `UPDATE kyros_auth_otps SET used_at = COALESCE(used_at, NOW())
        WHERE user_id = $1 AND purpose = $2 AND used_at IS NULL`,
      [userId, purpose]
    );
    const inserted = await client.query(
      `INSERT INTO kyros_auth_otps
         (user_id, email, purpose, code_hash, expires_at, max_attempts)
       VALUES ($1, $2, $3, $4, NOW() + ($5::text || ' seconds')::interval, $6)
       RETURNING id`,
      [userId, normalizedEmail, purpose, codeHash, OTP_TTL_SECONDS, OTP_MAX_ATTEMPTS]
    );
    otpId = inserted.rows[0].id;
    await client.query("COMMIT");
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    throw error;
  } finally {
    client.release();
  }
  try {
    await sendOtpEmail({ email: normalizedEmail, name, code, purpose });
  } catch (error) {
    await pool.query(
      `UPDATE kyros_auth_otps SET used_at = COALESCE(used_at, NOW()) WHERE id = $1`,
      [otpId]
    ).catch(() => {});
    if (error.message === "email_delivery_not_configured") throw error;
    const deliveryError = new Error("email_delivery_failed");
    deliveryError.statusCode = 503;
    throw deliveryError;
  }
  return { expiresIn: OTP_TTL_SECONDS, resendAfter: OTP_RESEND_COOLDOWN_SECONDS };
}

async function consumeEmailOtp({ email, code, purpose }) {
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail || typeof code !== "string" || !/^\d{6}$/.test(code)) {
    return { ok: false, error: "invalid_or_expired_code" };
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(
      `SELECT id, user_id, email, code_hash, expires_at, attempts, max_attempts
         FROM kyros_auth_otps
        WHERE LOWER(email) = $1 AND purpose = $2 AND used_at IS NULL
        ORDER BY created_at DESC LIMIT 1 FOR UPDATE`,
      [normalizedEmail, purpose]
    );
    if (result.rowCount !== 1) {
      await client.query("ROLLBACK");
      client.release();
      return { ok: false, error: "invalid_or_expired_code" };
    }
    const otp = result.rows[0];
    if (new Date(otp.expires_at).getTime() <= Date.now() || Number(otp.attempts) >= Number(otp.max_attempts)) {
      await client.query("ROLLBACK");
      client.release();
      return { ok: false, error: "invalid_or_expired_code" };
    }
    const valid = timingSafeStringEqual(otp.code_hash, otpDigest(normalizedEmail, purpose, code));
    if (!valid) {
      await client.query(
        `UPDATE kyros_auth_otps SET attempts = attempts + 1 WHERE id = $1`,
        [otp.id]
      );
      await client.query("COMMIT");
      client.release();
      return { ok: false, error: "invalid_or_expired_code" };
    }
    return { ok: true, client, otp };
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    client.release();
    throw error;
  }
}

function releaseConsumedOtp(result) {
  if (result && result.client) result.client.release();
}

function userSelectColumns(prefix = "") {
  const p = prefix ? `${prefix}.` : "";
  return `${p}id, ${p}name, ${p}email, ${p}phone_number, ${p}sign_in_method, ${p}role,
          ${p}account_status, ${p}subscription_status, ${p}subscription_start,
          ${p}subscription_end, ${p}auto_renew, ${p}subscription_data,
          ${p}email_verified, ${p}phone_verified, ${p}country, ${p}profile_image,
          ${p}free_trial_used, ${p}created_at, ${p}last_login_at`;
}

async function registerWithEmail(req, res) {
  const body = req.body || {};
  const name = typeof body.name === "string" ? body.name.trim() : "";
  const email = normalizeEmail(body.email);
  const password = body.password;
  if (name.length < 2 || name.length > 160 || !email || !validatePassword(password)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_registration_details",
      message: "Provide a valid name, email address, and password of at least 10 characters and no more than 72 UTF-8 bytes.",
    });
  }
  const phoneNumber = typeof body.phoneNumber === "string" ? body.phoneNumber.trim().slice(0, 40) : null;
  const country = typeof body.country === "string" && /^[A-Za-z]{2}$/.test(body.country.trim())
    ? body.country.trim().toUpperCase() : null;
  const passwordHash = await hashPassword(password);
  const client = await pool.connect();
  let userId;
  try {
    await client.query("BEGIN");
    const inserted = await client.query(
      `INSERT INTO users (
         id, name, email, password_hash, phone_number, sign_in_method,
         role, account_status, subscription_status, subscription_data,
         email_verified, country, free_trial_used
       ) VALUES ($1, $2, $3, $4, $5, 'password', 'user', 'active', 'free',
                 '{"plan":"free"}'::jsonb, FALSE, $6, NULL)
       RETURNING id`,
      [createPublicId("USR"), name, email, passwordHash, phoneNumber, country]
    );
    userId = inserted.rows[0].id;
    await client.query("COMMIT");
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    if (error.code === "23505") return res.status(409).json({ ok: false, error: "email_already_registered" });
    console.error("Registration database error:", error.message);
    return res.status(500).json({ ok: false, error: "registration_failed" });
  } finally {
    client.release();
  }
  try {
    const delivery = await issueEmailOtp({ userId, email, name, purpose: "email_verification" });
    return res.status(201).json({
      ok: true,
      verificationRequired: true,
      email,
      purpose: "email_verification",
      expiresIn: delivery.expiresIn,
      resendAfter: delivery.resendAfter,
      message: "Your account has been created. Enter the verification code sent to your email.",
    });
  } catch (error) {
    return authErrorResponse(res, error, "email_verification_delivery_failed");
  }
}

async function loginWithEmail(req, res) {
  const email = normalizeEmail(req.body && req.body.email);
  const password = req.body && req.body.password;
  if (!email || typeof password !== "string" || Buffer.byteLength(password, "utf8") > 72) {
    return res.status(400).json({ ok: false, error: "invalid_login_details" });
  }
  try {
    const result = await pool.query(
      `SELECT id, password_hash, account_status, email_verified
         FROM users WHERE LOWER(email) = $1 AND deleted_at IS NULL LIMIT 1`,
      [email]
    );
    if (result.rowCount !== 1 || !result.rows[0].password_hash) {
      return res.status(401).json({ ok: false, error: "invalid_credentials" });
    }
    const user = result.rows[0];
    const passwordValid = await verifyPassword(password, user.password_hash);
    if (!passwordValid || user.account_status !== "active") {
      return res.status(401).json({ ok: false, error: "invalid_credentials" });
    }
    if (!user.email_verified) {
      return res.status(403).json({
        ok: false,
        error: "email_verification_required",
        email,
        message: "Verify your email address before signing in. You can request a new code.",
      });
    }
    if (user.password_hash.startsWith("scrypt$")) {
      const upgradedHash = await hashPassword(password);
      await pool.query(
        `UPDATE users SET password_hash = $2, updated_at = NOW()
          WHERE id = $1 AND password_hash = $3`,
        [user.id, upgradedHash, user.password_hash]
      );
    }
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const tokens = await createStoredSession(client, user.id);
      const updated = await client.query(
        `UPDATE users SET last_login_at = NOW(), updated_at = NOW()
          WHERE id = $1 AND account_status = 'active' AND deleted_at IS NULL
          RETURNING ${userSelectColumns()}`,
        [user.id]
      );
      if (updated.rowCount !== 1) throw new Error("account_unavailable");
      await client.query("COMMIT");
      return res.json({
        ok: true, ...tokens, tokenType: "Bearer", expiresIn: AUTH_ACCESS_TTL_SECONDS,
        user: publicUser(updated.rows[0]),
      });
    } catch (error) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      if (error.message === "account_unavailable") return res.status(403).json({ ok: false, error: "account_unavailable" });
      throw error;
    } finally {
      client.release();
    }
  } catch (error) {
    console.error("Login error:", error.message);
    return res.status(500).json({ ok: false, error: "login_failed" });
  }
}

async function verifyEmailAddress(req, res) {
  const email = normalizeEmail(req.body && req.body.email);
  const code = String((req.body && (req.body.code || req.body.otp)) || "");
  if (!email || !/^\d{6}$/.test(code)) {
    return res.status(400).json({ ok: false, error: "invalid_or_expired_code" });
  }
  let consumed;
  try {
    consumed = await consumeEmailOtp({ email, code, purpose: "email_verification" });
    if (!consumed.ok) return res.status(400).json({ ok: false, error: consumed.error });
    const client = consumed.client;
    const otp = consumed.otp;
    try {
      await client.query(`UPDATE kyros_auth_otps SET used_at = NOW() WHERE id = $1`, [otp.id]);
      const updated = await client.query(
        `UPDATE users SET email_verified = TRUE, updated_at = NOW()
          WHERE id = $1 AND LOWER(email) = $2 AND deleted_at IS NULL
          RETURNING id`,
        [otp.user_id, email]
      );
      if (updated.rowCount !== 1) {
        await client.query("ROLLBACK");
        return res.status(404).json({ ok: false, error: "account_not_found" });
      }
      const tokens = await createStoredSession(client, otp.user_id);
      const userResult = await client.query(
        `UPDATE users SET last_login_at = NOW(), updated_at = NOW()
          WHERE id = $1 RETURNING ${userSelectColumns()}`,
        [otp.user_id]
      );
      await client.query("COMMIT");
      return res.json({
        ok: true, verified: true, ...tokens, tokenType: "Bearer", expiresIn: AUTH_ACCESS_TTL_SECONDS,
        user: publicUser(userResult.rows[0]),
        message: "Email verified. You are now signed in.",
      });
    } catch (error) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      throw error;
    } finally {
      releaseConsumedOtp(consumed);
    }
  } catch (error) {
    console.error("Email verification error:", error.message);
    return res.status(500).json({ ok: false, error: "email_verification_failed" });
  }
}

async function resendEmailVerification(req, res) {
  const email = normalizeEmail(req.body && req.body.email);
  if (!email) return res.status(400).json({ ok: false, error: "invalid_email" });
  try {
    const found = await pool.query(
      `SELECT id, name, email_verified FROM users
        WHERE LOWER(email) = $1 AND deleted_at IS NULL LIMIT 1`,
      [email]
    );
    if (found.rowCount === 1 && !found.rows[0].email_verified) {
      const delivery = await issueEmailOtp({
        userId: found.rows[0].id, email, name: found.rows[0].name, purpose: "email_verification",
      });
      return res.json({
        ok: true, verificationRequired: true, email,
        expiresIn: delivery.expiresIn, resendAfter: delivery.resendAfter,
        message: "If this account needs verification, a new code has been sent.",
      });
    }
    return res.json({ ok: true, message: "If this account needs verification, a new code has been sent." });
  } catch (error) {
    return authErrorResponse(res, error, "email_verification_delivery_failed");
  }
}

async function loginWithGoogle(req, res) {
  let googleIdentity;
  try {
    googleIdentity = await verifyGoogleIdToken(req.body && req.body.idToken);
  } catch (error) {
    return authErrorResponse(res, error, "google_verification_failed");
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const found = await client.query(
      `SELECT id FROM users WHERE sign_in_id = $1 AND deleted_at IS NULL LIMIT 1 FOR UPDATE`,
      [googleIdentity.sub]
    );
    let userId;
    if (found.rowCount === 1) {
      userId = found.rows[0].id;
    } else {
      const byEmail = await client.query(
        `SELECT id FROM users WHERE LOWER(email) = $1 AND deleted_at IS NULL LIMIT 1 FOR UPDATE`,
        [googleIdentity.email]
      );
      if (byEmail.rowCount === 1) {
        await client.query("ROLLBACK");
        return res.status(409).json({
          ok: false,
          error: "account_link_required",
          message: "An account already exists for this email. Sign in to that account first, then link Google from account settings.",
        });
      }
      const inserted = await client.query(
        `INSERT INTO users (
           id, name, email, sign_in_method, sign_in_id, role, account_status,
           subscription_status, subscription_data, email_verified, profile_image,
           free_trial_used
         ) VALUES ($1, $2, $3, 'google', $4, 'user', 'active', 'free',
                   '{"plan":"free"}'::jsonb, TRUE, $5, NULL)
         RETURNING id`,
        [createPublicId("USR"), googleIdentity.name, googleIdentity.email, googleIdentity.sub, googleIdentity.picture]
      );
      userId = inserted.rows[0].id;
    }
    const tokens = await createStoredSession(client, userId);
    const updated = await client.query(
      `UPDATE users SET last_login_at = NOW(), updated_at = NOW()
        WHERE id = $1 AND account_status = 'active' AND deleted_at IS NULL
        RETURNING ${userSelectColumns()}`,
      [userId]
    );
    if (updated.rowCount !== 1) throw new Error("account_unavailable");
    await client.query("COMMIT");
    return res.json({
      ok: true, ...tokens, tokenType: "Bearer", expiresIn: AUTH_ACCESS_TTL_SECONDS,
      user: publicUser(updated.rows[0]), isNewUser: found.rowCount === 0,
    });
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    if (error.code === "23505") return res.status(409).json({ ok: false, error: "account_conflict" });
    if (error.message === "account_unavailable") return res.status(403).json({ ok: false, error: "account_unavailable" });
    console.error("Google sign-in error:", error.message);
    return res.status(500).json({ ok: false, error: "google_sign_in_failed" });
  } finally {
    client.release();
  }
}

app.post("/api/auth/register", authConfigured, signupRateLimit, registerWithEmail);
app.post("/api/auth/signup", authConfigured, signupRateLimit, registerWithEmail);
app.post("/api/auth/login", authConfigured, loginRateLimit, loginWithEmail);
app.post("/api/auth/google", authConfigured, googleRateLimit, loginWithGoogle);

app.post("/api/auth/google/link", requireAuth, googleRateLimit, async (req, res) => {
  let identity;
  try {
    identity = await verifyGoogleIdToken(req.body && req.body.idToken);
  } catch (error) {
    return authErrorResponse(res, error, "google_verification_failed");
  }
  if (identity.email !== req.auth.user.email) {
    return res.status(409).json({ ok: false, error: "google_email_mismatch" });
  }
  try {
    const result = await pool.query(
      `UPDATE users
          SET sign_in_id = $2,
              sign_in_method = CASE WHEN password_hash IS NULL THEN 'google' ELSE 'password+google' END,
              email_verified = TRUE,
              profile_image = COALESCE(profile_image, $3),
              updated_at = NOW()
        WHERE id = $1 AND deleted_at IS NULL
          AND (sign_in_id IS NULL OR sign_in_id = $2)
        RETURNING id, sign_in_method`,
      [req.auth.user.id, identity.sub, identity.picture]
    );
    if (result.rowCount !== 1) return res.status(409).json({ ok: false, error: "google_account_already_linked" });
    return res.json({ ok: true, message: "Google account linked successfully." });
  } catch (error) {
    if (error.code === "23505") return res.status(409).json({ ok: false, error: "google_account_already_linked" });
    console.error("Google account linking error:", error.message);
    return res.status(500).json({ ok: false, error: "google_link_failed" });
  }
});

app.post("/api/auth/email/verification/request", authConfigured, otpRateLimit, resendEmailVerification);
app.post("/api/auth/resend-verification", authConfigured, otpRateLimit, resendEmailVerification);
app.post("/api/auth/email/verification/verify", authConfigured, otpRateLimit, verifyEmailAddress);
app.post("/api/auth/verify-email", authConfigured, otpRateLimit, verifyEmailAddress);

app.post("/api/auth/forgot-password", authConfigured, passwordResetRateLimit, async (req, res) => {
  const email = normalizeEmail(req.body && req.body.email);
  if (!email) return res.status(400).json({ ok: false, error: "invalid_email" });
  try {
    const found = await pool.query(
      `SELECT id, name FROM users WHERE LOWER(email) = $1 AND deleted_at IS NULL LIMIT 1`,
      [email]
    );
    if (found.rowCount === 1) {
      await issueEmailOtp({ userId: found.rows[0].id, email, name: found.rows[0].name, purpose: "password_reset" });
    } else {
      // Check mail configuration even for unknown accounts to avoid account enumeration.
      getMailTransporter();
    }
    return res.json({
      ok: true,
      verificationRequired: true,
      email,
      expiresIn: OTP_TTL_SECONDS,
      message: "If an account exists for this email, a password reset code has been sent.",
    });
  } catch (error) {
    return authErrorResponse(res, error, "password_reset_delivery_failed");
  }
});
async function requestPasswordReset(req, res) {
  const email = normalizeEmail(req.body && req.body.email);
  if (!email) return res.status(400).json({ ok: false, error: "invalid_email" });
  try {
    const found = await pool.query(
      `SELECT id, name FROM users WHERE LOWER(email) = $1 AND deleted_at IS NULL LIMIT 1`,
      [email]
    );
    if (found.rowCount === 1) {
      await issueEmailOtp({ userId: found.rows[0].id, email, name: found.rows[0].name, purpose: "password_reset" });
    } else {
      getMailTransporter();
    }
    return res.json({ ok: true, verificationRequired: true, email, expiresIn: OTP_TTL_SECONDS,
      message: "If an account exists for this email, a password reset code has been sent." });
  } catch (error) {
    return authErrorResponse(res, error, "password_reset_delivery_failed");
  }
}
app.post("/api/auth/password-reset/request", authConfigured, passwordResetRateLimit, requestPasswordReset);

app.post("/api/auth/password-reset/verify", authConfigured, otpRateLimit, async (req, res) => {
  const email = normalizeEmail(req.body && req.body.email);
  const code = String((req.body && (req.body.code || req.body.otp)) || "");
  if (!email || !/^\d{6}$/.test(code)) return res.status(400).json({ ok: false, error: "invalid_or_expired_code" });
  let consumed;
  try {
    consumed = await consumeEmailOtp({ email, code, purpose: "password_reset" });
    if (!consumed.ok) return res.status(400).json({ ok: false, error: consumed.error });
    const { client, otp } = consumed;
    try {
      const resetToken = crypto.randomBytes(32).toString("base64url");
      await client.query(
        `UPDATE kyros_auth_otps
            SET used_at = NOW(), reset_token_hash = $2,
                reset_token_expires_at = NOW() + ($3::text || ' seconds')::interval,
                reset_token_used_at = NULL
          WHERE id = $1`,
        [otp.id, sha256Hex(resetToken), PASSWORD_RESET_TOKEN_TTL_SECONDS]
      );
      await client.query("COMMIT");
      return res.json({
        ok: true,
        verified: true,
        resetToken,
        expiresIn: PASSWORD_RESET_TOKEN_TTL_SECONDS,
        message: "Code verified. You can now choose a new password.",
      });
    } catch (error) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      throw error;
    } finally {
      releaseConsumedOtp(consumed);
    }
  } catch (error) {
    console.error("Password reset code verification error:", error.message);
    return res.status(500).json({ ok: false, error: "password_reset_verification_failed" });
  }
});

async function completePasswordReset(req, res) {
  const email = normalizeEmail(req.body && req.body.email);
  const resetToken = req.body && req.body.resetToken;
  const newPassword = req.body && (req.body.newPassword || req.body.password);
  const confirmPassword = req.body && (req.body.confirmPassword || req.body.passwordConfirmation);
  if (!email || typeof resetToken !== "string" || resetToken.length < 32 || resetToken.length > 256) {
    return res.status(400).json({ ok: false, error: "invalid_password_reset_request" });
  }
  if (!validatePassword(newPassword) || newPassword !== confirmPassword) {
    return res.status(400).json({
      ok: false,
      error: "invalid_new_password",
      message: "Passwords must match, contain at least 10 characters, and use no more than 72 UTF-8 bytes.",
    });
  }
  const newHash = await hashPassword(newPassword);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const otpResult = await client.query(
      `SELECT id, user_id FROM kyros_auth_otps
        WHERE LOWER(email) = $1 AND purpose = 'password_reset'
          AND reset_token_hash = $2 AND reset_token_used_at IS NULL
          AND reset_token_expires_at > NOW()
        ORDER BY created_at DESC LIMIT 1 FOR UPDATE`,
      [email, sha256Hex(resetToken)]
    );
    if (otpResult.rowCount !== 1) {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: "invalid_or_expired_reset_token" });
    }
    const otp = otpResult.rows[0];
    const updated = await client.query(
      `UPDATE users
          SET password_hash = $2, token_version = token_version + 1,
              refresh_sessions = '[]'::jsonb, updated_at = NOW()
        WHERE id = $1 AND deleted_at IS NULL AND account_status = 'active'
        RETURNING ${userSelectColumns()}`,
      [otp.user_id, newHash]
    );
    if (updated.rowCount !== 1) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "account_unavailable" });
    }
    await client.query(
      `UPDATE kyros_auth_otps SET reset_token_used_at = NOW() WHERE id = $1`,
      [otp.id]
    );
    const tokens = await createStoredSession(client, otp.user_id);
    const userResult = await client.query(
      `UPDATE users SET last_login_at = NOW(), updated_at = NOW()
        WHERE id = $1 RETURNING ${userSelectColumns()}`,
      [otp.user_id]
    );
    await client.query("COMMIT");
    return res.json({
      ok: true,
      ...tokens,
      tokenType: "Bearer",
      expiresIn: AUTH_ACCESS_TTL_SECONDS,
      user: publicUser(userResult.rows[0]),
      message: "Your password has been updated. You are now signed in.",
    });
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    console.error("Password reset completion error:", error.message);
    return res.status(500).json({ ok: false, error: "password_reset_failed" });
  } finally {
    client.release();
  }
}
app.post("/api/auth/password-reset/complete", authConfigured, passwordResetRateLimit, completePasswordReset);
app.post("/api/auth/password-reset/confirm", authConfigured, passwordResetRateLimit, completePasswordReset);
app.post("/api/auth/reset-password", authConfigured, passwordResetRateLimit, completePasswordReset);

app.post("/api/auth/change-password", requireAuth, loginRateLimit, async (req, res) => {
  const currentPassword = req.body && req.body.currentPassword;
  const newPassword = req.body && req.body.newPassword;
  const confirmPassword = req.body && req.body.confirmPassword;
  if (typeof currentPassword !== "string" || !validatePassword(newPassword) || newPassword !== confirmPassword) {
    return res.status(400).json({ ok: false, error: "invalid_new_password" });
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const found = await client.query(
      `SELECT id, password_hash FROM users WHERE id = $1 AND deleted_at IS NULL FOR UPDATE`,
      [req.auth.user.id]
    );
    if (found.rowCount !== 1 || !found.rows[0].password_hash ||
        !(await verifyPassword(currentPassword, found.rows[0].password_hash))) {
      await client.query("ROLLBACK");
      return res.status(401).json({ ok: false, error: "current_password_incorrect" });
    }
    const newHash = await hashPassword(newPassword);
    await client.query(
      `UPDATE users SET password_hash = $2, token_version = token_version + 1,
              refresh_sessions = '[]'::jsonb, updated_at = NOW() WHERE id = $1`,
      [req.auth.user.id, newHash]
    );
    const tokens = await createStoredSession(client, req.auth.user.id);
    const userResult = await client.query(
      `UPDATE users SET last_login_at = NOW() WHERE id = $1 RETURNING ${userSelectColumns()}`,
      [req.auth.user.id]
    );
    await client.query("COMMIT");
    return res.json({ ok: true, ...tokens, tokenType: "Bearer", expiresIn: AUTH_ACCESS_TTL_SECONDS, user: publicUser(userResult.rows[0]) });
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    console.error("Password change error:", error.message);
    return res.status(500).json({ ok: false, error: "password_change_failed" });
  } finally {
    client.release();
  }
});

app.post("/api/auth/refresh", authConfigured, loginRateLimit, async (req, res) => {
  const refreshToken = req.body && req.body.refreshToken;
  let claims;
  try {
    claims = verifyJwt(refreshToken, AUTH_REFRESH_SECRET, "refresh");
  } catch (_) {
    return res.status(401).json({ ok: false, error: "invalid_or_expired_refresh_token" });
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(
      `SELECT id, token_version, account_status, refresh_sessions
         FROM users WHERE id = $1 AND deleted_at IS NULL FOR UPDATE`,
      [claims.sub]
    );
    if (result.rowCount !== 1 || result.rows[0].account_status !== "active" ||
        Number(result.rows[0].token_version) !== Number(claims.ver)) {
      await client.query("ROLLBACK");
      return res.status(401).json({ ok: false, error: "invalid_or_expired_refresh_token" });
    }
    const user = result.rows[0];
    const sessions = safeJsonArray(user.refresh_sessions);
    const current = sessions.find(session => session.sid === claims.sid);
    if (!current || Date.parse(current.expiresAt) <= Date.now() ||
        !timingSafeStringEqual(current.tokenHash, sha256Hex(refreshToken))) {
      await client.query("ROLLBACK");
      return res.status(401).json({ ok: false, error: "invalid_or_expired_refresh_token" });
    }
    const tokens = createTokenPair(user, claims.sid);
    const nextSessions = sessions.map(session => session.sid === claims.sid
      ? { ...session, tokenHash: sha256Hex(tokens.refreshToken), expiresAt: new Date(Date.now() + AUTH_REFRESH_TTL_SECONDS * 1000).toISOString() }
      : session
    );
    await client.query(
      `UPDATE users SET refresh_sessions = $2::jsonb, updated_at = NOW() WHERE id = $1`,
      [user.id, JSON.stringify(nextSessions)]
    );
    const profile = await client.query(
      `SELECT ${userSelectColumns()} FROM users WHERE id = $1`, [user.id]
    );
    await client.query("COMMIT");
    return res.json({
      ok: true, ...tokens, tokenType: "Bearer", expiresIn: AUTH_ACCESS_TTL_SECONDS,
      user: publicUser(profile.rows[0]),
    });
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    console.error("Token refresh error:", error.message);
    return res.status(500).json({ ok: false, error: "token_refresh_failed" });
  } finally {
    client.release();
  }
});

app.post("/api/auth/logout", requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(`SELECT refresh_sessions FROM users WHERE id = $1 FOR UPDATE`, [req.auth.user.id]);
    if (result.rowCount === 1) {
      const sessions = safeJsonArray(result.rows[0].refresh_sessions);
      const allDevices = req.body && req.body.allDevices === true;
      const remaining = allDevices ? [] : sessions.filter(session => session.sid !== req.auth.claims.sid);
      await client.query(
        `UPDATE users SET refresh_sessions = $2::jsonb,
             token_version = CASE WHEN $3 THEN token_version + 1 ELSE token_version END,
             updated_at = NOW() WHERE id = $1`,
        [req.auth.user.id, JSON.stringify(remaining), allDevices]
      );
    }
    await client.query("COMMIT");
    return res.json({ ok: true, message: "Signed out successfully." });
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    console.error("Logout error:", error.message);
    return res.status(500).json({ ok: false, error: "logout_failed" });
  } finally {
    client.release();
  }
});

app.get("/api/auth/me", requireAuth, (req, res) => {
  return res.json({ ok: true, user: publicUser(req.auth.user) });
});

app.get("/api/subscription", requireAuth, (req, res) => {
  const trial = publicFreeTrial(req.auth.user);
  return res.json({
    ok: true,
    subscription: publicSubscription(req.auth.user),
    freeTrial: trial,
    freeTrialUsed: Boolean(storedFreeTrial(req.auth.user)),
    canStartFreeTrial: publicSubscription(req.auth.user).canStartFreeTrial,
  });
});

function subscriptionPriceForCycle(cycle) {
  const definition = SUBSCRIPTION_CYCLES[cycle];
  if (!definition) return null;
  const raw = process.env[definition.priceEnv];
  if (raw == null || String(raw).trim() === "") return null;
  const amount = Number(raw);
  return Number.isFinite(amount) && amount >= 0 ? amount : null;
}

function publicSubscriptionPlans() {
  return Object.entries(SUBSCRIPTION_CYCLES).map(([billingCycle, definition]) => {
    const price = subscriptionPriceForCycle(billingCycle);
    return {
      id: billingCycle,
      plan: "pro",
      name: definition.label,
      billingCycle,
      durationMonths: definition.durationMonths,
      price,
      amount: price,
      currency: /^[A-Z]{3}$/.test(SUBSCRIPTION_CURRENCY) ? SUBSCRIPTION_CURRENCY : "KES",
      priceConfigured: price !== null,
      paymentMethods: ["mpesa", "pesapal", "card"],
    };
  });
}

app.get("/api/subscription/plans", (req, res) => {
  return res.json({
    ok: true,
    plans: publicSubscriptionPlans(),
    currency: /^[A-Z]{3}$/.test(SUBSCRIPTION_CURRENCY) ? SUBSCRIPTION_CURRENCY : "KES",
  });
});

async function startSubscriptionCheckout(req, res) {
  const body = req.body && typeof req.body === "object" ? req.body : {};
  const plan = String(body.plan || "pro").trim().toLowerCase();
  const billingCycle = String(body.billingCycle || body.billing_cycle || "").trim().toLowerCase();
  const paymentMethod = String(body.paymentMethod || body.payment_method || "").trim().toLowerCase();
  const idempotencyKey = String(body.idempotencyKey || body.requestId || body.request_id || "").trim();
  const phoneNumber = body.phoneNumber == null ? "" : String(body.phoneNumber).trim();

  if (!["pro", "professional"].includes(plan)) {
    return res.status(400).json({ ok: false, error: "invalid_subscription_plan", message: "Choose the KyroS Pro plan." });
  }
  const cycle = SUBSCRIPTION_CYCLES[billingCycle];
  if (!cycle) {
    return res.status(400).json({ ok: false, error: "invalid_billing_cycle", message: "Choose monthly, quarterly or yearly billing." });
  }
  const requestedDuration = body.durationMonths == null ? body.duration : body.durationMonths;
  if (requestedDuration != null && String(requestedDuration).trim() !== "" &&
      Number(requestedDuration) !== cycle.durationMonths) {
    return res.status(400).json({
      ok: false,
      error: "billing_duration_mismatch",
      message: "The selected billing cycle determines the subscription duration. Please select the matching cycle.",
    });
  }
  if (!["mpesa", "pesapal", "card"].includes(paymentMethod)) {
    return res.status(400).json({ ok: false, error: "invalid_payment_method", message: "Choose M-Pesa, Pesapal or card checkout." });
  }
  if (!/^[A-Za-z0-9._:-]{8,128}$/.test(idempotencyKey)) {
    return res.status(400).json({ ok: false, error: "invalid_request_id", message: "A valid checkout request ID is required. Please retry from the app." });
  }
  if (paymentMethod === "mpesa" && !/^\+?[0-9][0-9\s()-]{6,19}$/.test(phoneNumber)) {
    return res.status(400).json({ ok: false, error: "invalid_phone_number", message: "Enter a valid M-Pesa phone number." });
  }

  // The current release has no live payment-provider integration. Simulated
  // success is deliberately impossible in production, even if the flag is set.
  if (!PAYMENT_SIMULATION_ENABLED) {
    return res.status(503).json({
      ok: false,
      error: "payment_provider_not_configured",
      message: "Secure payment processing is not available yet. Please try again when KyroS payment services are enabled.",
    });
  }

  const configuredPrice = subscriptionPriceForCycle(billingCycle);
  const amount = configuredPrice === null ? 0 : configuredPrice;
  const currency = /^[A-Z]{3}$/.test(SUBSCRIPTION_CURRENCY) ? SUBSCRIPTION_CURRENCY : "KES";
  const metadataInput = body.metadata && typeof body.metadata === "object" && !Array.isArray(body.metadata)
    ? body.metadata : {};
  // Never accept PAN, CVV, PIN, expiry or other raw card credentials.
  const safeMetadata = {};
  for (const key of ["cardBrand", "last4", "cardholderName", "checkoutSessionId", "providerRequestId"]) {
    const value = metadataInput[key];
    if (typeof value === "string" && value.trim().length > 0 && value.length <= 120) {
      safeMetadata[key] = value.trim();
    }
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const found = await client.query(
      `SELECT ${userSelectColumns()} FROM users
        WHERE id = $1 AND deleted_at IS NULL AND account_status = 'active'
        FOR UPDATE`,
      [req.auth.user.id]
    );
    if (found.rowCount !== 1) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "account_unavailable" });
    }
    const user = found.rows[0];

    // A retry with the same key returns the original result, never a second term.
    const prior = await client.query(
      `SELECT id, payment_status, payment_method, amount, currency,
              subscription_start, subscription_end, metadata, created_at
         FROM payments
        WHERE user_id = $1 AND idempotency_key = $2
        LIMIT 1 FOR UPDATE`,
      [user.id, idempotencyKey]
    );
    if (prior.rowCount === 1) {
      const payment = prior.rows[0];
      if (payment.payment_status === "successful") {
        await client.query("COMMIT");
        const refreshed = await pool.query(
          `SELECT ${userSelectColumns()} FROM users WHERE id = $1 AND deleted_at IS NULL`,
          [user.id]
        );
        const latestUser = refreshed.rows[0] || user;
        return res.json({
          ok: true,
          idempotentReplay: true,
          simulated: Boolean(payment.metadata && payment.metadata.simulated),
          message: "This checkout request was already processed. Your KyroS Pro subscription is active.",
          payment: {
            id: payment.id, status: payment.payment_status, method: payment.payment_method,
            amount: Number(payment.amount), currency: payment.currency,
            startDate: payment.subscription_start, endDate: payment.subscription_end,
          },
          subscription: publicSubscription(latestUser),
          user: publicUser(latestUser),
        });
      }
      await client.query("COMMIT");
      return res.status(409).json({
        ok: false,
        error: "checkout_request_already_exists",
        paymentStatus: payment.payment_status,
        message: "This checkout request ID has already been used. Start a new checkout if you need another request.",
      });
    }

    const currentSubscription = publicSubscription(user);
    if (currentSubscription.plan === "pro" && currentSubscription.status === "active") {
      await client.query("ROLLBACK");
      return res.status(409).json({
        ok: false,
        error: "subscription_already_active",
        subscription: currentSubscription,
        message: "KyroS Pro is already active on this account.",
      });
    }

    const paymentId = createPublicId("pay");
    const activationMetadata = {
      simulated: true,
      simulationMode: "development_only",
      priceConfigured: configuredPrice !== null,
      billingCycle,
      durationMonths: cycle.durationMonths,
      paymentMethod,
      requestId: idempotencyKey,
      ...(paymentMethod === "mpesa" ? { phoneNumber } : {}),
      ...safeMetadata,
    };
    const paymentResult = await client.query(
      `INSERT INTO payments (
         id, user_id, user_name, user_email, amount, currency, payment_method,
         transaction_reference, payment_status, payment_purpose,
         subscription_start, subscription_end, metadata, paid_at, idempotency_key
       ) VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8, 'successful', $9,
         NOW(), NOW() + ($10::text || ' months')::interval, $11::jsonb, NOW(), $12
       )
       RETURNING id, payment_status, payment_method, amount, currency,
                 subscription_start, subscription_end, metadata, created_at`,
      [
        paymentId, user.id, user.name, user.email, amount, currency, paymentMethod,
        `SIM-${paymentId}`, `pro_subscription_${billingCycle}`,
        cycle.durationMonths, JSON.stringify(activationMetadata), idempotencyKey,
      ]
    );
    const payment = paymentResult.rows[0];
    const subscriptionData = {
      plan: "pro",
      billingCycle,
      durationMonths: cycle.durationMonths,
      paymentId,
      paymentMethod,
      activatedAt: payment.subscription_start,
      activationSource: "simulated_payment",
      simulated: true,
      priceConfigured: configuredPrice !== null,
    };
    const updated = await client.query(
      `UPDATE users
          SET subscription_status = 'active',
              subscription_start = $2,
              subscription_end = $3,
              auto_renew = FALSE,
              subscription_data = $4::jsonb,
              updated_at = NOW()
        WHERE id = $1
        RETURNING ${userSelectColumns()}`,
      [user.id, payment.subscription_start, payment.subscription_end, JSON.stringify(subscriptionData)]
    );
    await client.query("COMMIT");

    const updatedUser = updated.rows[0];
    return res.status(200).json({
      ok: true,
      simulated: true,
      message: configuredPrice === null
        ? "Development-only simulated checkout completed. No payment was collected because no plan price is configured. KyroS Pro is active for testing."
        : "Development-only simulated checkout completed. No real payment was collected. KyroS Pro is active for testing.",
      payment: {
        id: payment.id,
        status: payment.payment_status,
        method: payment.payment_method,
        amount: Number(payment.amount),
        currency: payment.currency,
        startDate: payment.subscription_start,
        endDate: payment.subscription_end,
      },
      subscription: publicSubscription(updatedUser),
      user: publicUser(updatedUser),
    });
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    if (error && error.code === "23505") {
      return res.status(409).json({ ok: false, error: "checkout_request_already_exists", message: "This checkout request was already submitted. Refresh your subscription status." });
    }
    console.error("Subscription checkout error:", error.message);
    return res.status(500).json({ ok: false, error: "subscription_checkout_failed", message: "KyroS could not complete this checkout request." });
  } finally {
    client.release();
  }
}
app.post("/api/subscription/checkout", requireAuth, subscriptionCheckoutRateLimit, startSubscriptionCheckout);

async function startFreeTrial(req, res) {
  const timezoneRaw = req.body && req.body.timezone;
  let timezone = "UTC";
  if (typeof timezoneRaw === "string" && timezoneRaw.length <= 80) {
    try {
      new Intl.DateTimeFormat("en-US", { timeZone: timezoneRaw });
      timezone = timezoneRaw;
    } catch (_) {}
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const found = await client.query(
      `SELECT ${userSelectColumns()} FROM users
        WHERE id = $1 AND deleted_at IS NULL AND account_status = 'active'
        FOR UPDATE`,
      [req.auth.user.id]
    );
    if (found.rowCount !== 1) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "account_unavailable" });
    }
    const existingSubscription = publicSubscription(found.rows[0]);
    if (existingSubscription.accessType === "paid" && existingSubscription.status === "active") {
      await client.query("ROLLBACK");
      return res.status(409).json({
        ok: false,
        error: "subscription_already_active",
        subscription: existingSubscription,
        message: "KyroS Pro is already active on this account.",
      });
    }
    if (storedFreeTrial(found.rows[0])) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        ok: false,
        error: "free_trial_already_used",
        freeTrialUsed: true,
        freeTrial: publicFreeTrial(found.rows[0]),
        message: "The 7-day free trial has already been used on this account.",
      });
    }
    const updated = await client.query(
      `UPDATE users
          SET free_trial_used = jsonb_build_object(
                'trialId', $2::text,
                'plan', 'pro',
                'status', 'active',
                'durationDays', $3::integer,
                'startedAt', NOW(),
                'endsAt', NOW() + ($3::text || ' days')::interval,
                'activatedAt', NOW(),
                'activatedBy', 'account_owner',
                'activationSource', 'kyros_app',
                'timezone', $4::text,
                'policyVersion', 1
              ),
              updated_at = NOW()
        WHERE id = $1
        RETURNING ${userSelectColumns()}`,
      [req.auth.user.id, crypto.randomUUID(), FREE_TRIAL_DURATION_DAYS, timezone]
    );
    await client.query("COMMIT");
    const user = updated.rows[0];
    return res.json({
      ok: true,
      message: "Your 7-day KyroS Pro trial is now active.",
      freeTrialUsed: true,
      freeTrial: publicFreeTrial(user),
      subscription: publicSubscription(user),
      user: publicUser(user),
    });
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    console.error("Free trial activation error:", error.message);
    return res.status(500).json({ ok: false, error: "free_trial_activation_failed" });
  } finally {
    client.release();
  }
}
app.post("/api/subscription/trial/start", requireAuth, startFreeTrial);
app.post("/api/auth/free-trial/start", requireAuth, startFreeTrial);

app.get("/api/auth/status", requireAuth, (req, res) => {
  return res.json({ ok: true, authenticated: true, user: publicUser(req.auth.user) });
});

/* ============================================================
   404
   ============================================================ */

app.use(
  (req, res) => {
    res
      .status(404)
      .json({
        ok: false,

        error:
          "not_found",
      });
  }
);

/* ============================================================
   START SERVER
   ============================================================ */

async function start() {
  try {
    await initializeDatabase();

    server.listen(
      PORT,
      () => {
        console.log(
          `KyroS server running on port ${PORT}`
        );

        console.log(
          "Remote Camera code pairing + WebRTC signaling ready."
        );

        console.log(
          `STUN configured: ${EFFECTIVE_STUN_URLS.join(", ")}`
        );

        if (
          COTURN_CONFIGURED
        ) {
          console.log(
            `TURN configured with KyroS coturn: ${TURN_HOST}`
          );

        } else if (
          EXTERNAL_TURN_CONFIGURED
        ) {
          console.log(
            `TURN configured with external provider: ${TURN_URLS.join(", ")}`
          );

        } else {

          console.log(
            "TURN relay not configured. Direct WebRTC via Google STUN is enabled; add TURN later for restrictive NAT/firewall networks."
          );
        }
      }
    );

  } catch (e) {

    console.error(
      "Failed to start KyroS server:",
      e
    );

    process.exit(1);
  }
}

start();
