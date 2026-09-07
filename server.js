const express = require("express");
const { Pool } = require("pg");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs");

const app = express();

app.set("trust proxy", true);
app.disable("x-powered-by");
app.use(express.json({ limit: "128kb" }));

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("DATABASE_URL is not configured.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

const PUBLIC_DIR = path.join(__dirname, "public");
const RESOURCES_DIR = path.join(__dirname, "resources");

const ACTIVE_TIMEOUT_SECONDS = 180;

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

        platform VARCHAR(20) NOT NULL DEFAULT 'unknown',

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

        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        last_network_country VARCHAR(10),

        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_sessions (
        session_id UUID PRIMARY KEY,

        installation_id UUID NOT NULL
          REFERENCES kyros_installations(installation_id)
          ON DELETE CASCADE,

        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ended_at TIMESTAMPTZ,

        app_state VARCHAR(50) NOT NULL DEFAULT 'unknown',

        is_foreground BOOLEAN NOT NULL DEFAULT TRUE,
        is_broadcasting BOOLEAN NOT NULL DEFAULT FALSE,
        is_recording BOOLEAN NOT NULL DEFAULT FALSE,
        is_screen_sharing BOOLEAN NOT NULL DEFAULT FALSE,
        is_remote_camera BOOLEAN NOT NULL DEFAULT FALSE,

        network_transport VARCHAR(30),
        internet_validated BOOLEAN,

        battery_percent INTEGER,
        charging BOOLEAN,
        power_save BOOLEAN,

        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS kyros_broadcasts (
        broadcast_id UUID PRIMARY KEY,

        session_id UUID NOT NULL
          REFERENCES kyros_sessions(session_id)
          ON DELETE CASCADE,

        installation_id UUID NOT NULL
          REFERENCES kyros_installations(installation_id)
          ON DELETE CASCADE,

        destinations TEXT[] NOT NULL DEFAULT '{}',

        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ended_at TIMESTAMPTZ,

        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
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

    await client.query("COMMIT");

    console.log("KyroS database initialized.");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

/* ============================================================
   VALIDATION HELPERS
   ============================================================ */

function str(value, max = 200) {
  if (typeof value !== "string") {
    return null;
  }

  const result = value.trim();

  return result
    ? result.slice(0, max)
    : null;
}

function int(value, min = null, max = null) {
  const number = Number(value);

  if (!Number.isInteger(number)) {
    return null;
  }

  if (min !== null && number < min) {
    return null;
  }

  if (max !== null && number > max) {
    return null;
  }

  return number;
}

function num(value, min = null, max = null) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return null;
  }

  if (min !== null && number < min) {
    return null;
  }

  if (max !== null && number > max) {
    return null;
  }

  return number;
}

function bool(value) {
  return typeof value === "boolean"
    ? value
    : null;
}

function uuid(value) {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
      value
    )
  );
}

function platform(value) {
  const result = (str(value, 20) || "unknown").toLowerCase();

  return ALLOWED_PLATFORMS.has(result)
    ? result
    : "unknown";
}

function destinations(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return [
    ...new Set(
      value
        .filter((item) => typeof item === "string")
        .map((item) => item.trim().toLowerCase())
        .filter((item) => ALLOWED_DESTINATIONS.has(item))
    ),
  ].slice(0, 20);
}

function country(req) {
  const possibleHeaders = [
    req.headers["cf-ipcountry"],
    req.headers["x-vercel-ip-country"],
    req.headers["x-country-code"],
  ];

  for (const header of possibleHeaders) {
    if (
      typeof header === "string" &&
      /^[A-Za-z]{2}$/.test(header.trim())
    ) {
      return header.trim().toUpperCase();
    }
  }

  return null;
}

/* ============================================================
   PUBLIC WEBSITE
   ============================================================ */

app.use(
  express.static(PUBLIC_DIR, {
    extensions: ["html"],
    maxAge: "1h",
  })
);

/* ============================================================
   APP DOWNLOADS
   ============================================================ */

app.get("/download/:platform", (req, res) => {
  const requestedPlatform = String(
    req.params.platform || ""
  ).toLowerCase();

  const item = DOWNLOADS[requestedPlatform];

  if (!item) {
    return res.status(404).json({
      ok: false,
      error: "unsupported_platform",
    });
  }

  const filePath = path.join(
    RESOURCES_DIR,
    item.file
  );

  if (
    !fs.existsSync(filePath) ||
    fs.statSync(filePath).size === 0
  ) {
    return res.status(503).json({
      ok: false,
      error: "release_not_available",
      platform: requestedPlatform,
      message:
        "This KyroS release package has not been uploaded yet.",
    });
  }

  return res.download(
    filePath,
    item.downloadName
  );
});

/* ============================================================
   DOWNLOAD AVAILABILITY
   ============================================================ */

app.get("/api/downloads", (req, res) => {
  const releases = {};

  for (const [name, item] of Object.entries(DOWNLOADS)) {
    const filePath = path.join(
      RESOURCES_DIR,
      item.file
    );

    releases[name] = {
      available:
        fs.existsSync(filePath) &&
        fs.statSync(filePath).size > 0,

      url: `/download/${name}`,
    };
  }

  res.json({
    ok: true,
    releases,
  });
});

/* ============================================================
   KYROS TELEMETRY
   ============================================================ */

app.post("/api/telemetry", async (req, res) => {
  const client = await pool.connect();

  try {
    const body = req.body || {};

    const installationId =
      body.installationId;

    let sessionId =
      body.sessionId;

    if (!uuid(installationId)) {
      return res.status(400).json({
        ok: false,
        error: "invalid_installation_id",
      });
    }

    if (!uuid(sessionId)) {
      sessionId = crypto.randomUUID();
    }

    const device =
      body.device &&
      typeof body.device === "object"
        ? body.device
        : {};

    const appInfo =
      body.app &&
      typeof body.app === "object"
        ? body.app
        : {};

    const display =
      body.display &&
      typeof body.display === "object"
        ? body.display
        : {};

    const hardware =
      body.hardware &&
      typeof body.hardware === "object"
        ? body.hardware
        : {};

    const network =
      body.network &&
      typeof body.network === "object"
        ? body.network
        : {};

    const power =
      body.power &&
      typeof body.power === "object"
        ? body.power
        : {};

    const state =
      body.state &&
      typeof body.state === "object"
        ? body.state
        : {};

    const broadcast =
      body.broadcast &&
      typeof body.broadcast === "object"
        ? body.broadcast
        : {};

    const currentPlatform =
      platform(device.platform);

    const currentDestinations =
      destinations(
        broadcast.destinations
      );

    const broadcasting =
      bool(broadcast.active) ??
      currentDestinations.length > 0;

    const networkCountry =
      country(req);

    await client.query("BEGIN");

    /* --------------------------------------------------------
       INSTALLATION
       -------------------------------------------------------- */

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
        first_seen_at,
        last_seen_at,
        created_at,
        updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,
        $19,$20,$21,$22,
        NOW(),NOW(),NOW(),NOW()
      )

      ON CONFLICT (installation_id)
      DO UPDATE SET

        platform =
          EXCLUDED.platform,

        manufacturer =
          COALESCE(
            EXCLUDED.manufacturer,
            kyros_installations.manufacturer
          ),

        brand =
          COALESCE(
            EXCLUDED.brand,
            kyros_installations.brand
          ),

        model =
          COALESCE(
            EXCLUDED.model,
            kyros_installations.model
          ),

        os_version =
          COALESCE(
            EXCLUDED.os_version,
            kyros_installations.os_version
          ),

        os_api =
          COALESCE(
            EXCLUDED.os_api,
            kyros_installations.os_api
          ),

        app_version =
          COALESCE(
            EXCLUDED.app_version,
            kyros_installations.app_version
          ),

        app_build =
          COALESCE(
            EXCLUDED.app_build,
            kyros_installations.app_build
          ),

        app_language =
          COALESCE(
            EXCLUDED.app_language,
            kyros_installations.app_language
          ),

        device_language =
          COALESCE(
            EXCLUDED.device_language,
            kyros_installations.device_language
          ),

        device_locale =
          COALESCE(
            EXCLUDED.device_locale,
            kyros_installations.device_locale
          ),

        device_region =
          COALESCE(
            EXCLUDED.device_region,
            kyros_installations.device_region
          ),

        timezone =
          COALESCE(
            EXCLUDED.timezone,
            kyros_installations.timezone
          ),

        screen_width =
          COALESCE(
            EXCLUDED.screen_width,
            kyros_installations.screen_width
          ),

        screen_height =
          COALESCE(
            EXCLUDED.screen_height,
            kyros_installations.screen_height
          ),

        screen_density =
          COALESCE(
            EXCLUDED.screen_density,
            kyros_installations.screen_density
          ),

        screen_refresh_rate =
          COALESCE(
            EXCLUDED.screen_refresh_rate,
            kyros_installations.screen_refresh_rate
          ),

        cpu_cores =
          COALESCE(
            EXCLUDED.cpu_cores,
            kyros_installations.cpu_cores
          ),

        total_memory_mb =
          COALESCE(
            EXCLUDED.total_memory_mb,
            kyros_installations.total_memory_mb
          ),

        low_ram_device =
          COALESCE(
            EXCLUDED.low_ram_device,
            kyros_installations.low_ram_device
          ),

        last_network_country =
          COALESCE(
            EXCLUDED.last_network_country,
            kyros_installations.last_network_country
          ),

        last_seen_at = NOW(),
        updated_at = NOW()
      `,
      [
        installationId,
        currentPlatform,

        str(device.manufacturer, 100),
        str(device.brand, 100),
        str(device.model, 150),

        str(device.osVersion, 100),
        int(device.osApi, 1, 1000),

        str(appInfo.version, 50),
        str(appInfo.build, 50),
        str(appInfo.language, 30),

        str(device.language, 30),
        str(device.locale, 50),
        str(device.region, 20),
        str(device.timezone, 100),

        int(display.width, 1, 50000),
        int(display.height, 1, 50000),

        num(display.density, 0.1, 20),
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
      ]
    );

    /* --------------------------------------------------------
       SESSION
       -------------------------------------------------------- */

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
        $1,$2,
        NOW(),NOW(),
        $3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,
        NOW(),NOW()
      )

      ON CONFLICT (session_id)
      DO UPDATE SET

        last_seen_at = NOW(),

        app_state =
          EXCLUDED.app_state,

        is_foreground =
          EXCLUDED.is_foreground,

        is_broadcasting =
          EXCLUDED.is_broadcasting,

        is_recording =
          EXCLUDED.is_recording,

        is_screen_sharing =
          EXCLUDED.is_screen_sharing,

        is_remote_camera =
          EXCLUDED.is_remote_camera,

        network_transport =
          EXCLUDED.network_transport,

        internet_validated =
          EXCLUDED.internet_validated,

        battery_percent =
          EXCLUDED.battery_percent,

        charging =
          EXCLUDED.charging,

        power_save =
          EXCLUDED.power_save,

        updated_at = NOW()
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

    /* --------------------------------------------------------
       BROADCAST
       -------------------------------------------------------- */

    let broadcastId =
      uuid(broadcast.broadcastId)
        ? broadcast.broadcastId
        : null;

    if (broadcasting) {
      if (!broadcastId) {
        const existingBroadcast =
          await client.query(
            `
            SELECT broadcast_id

            FROM kyros_broadcasts

            WHERE session_id = $1
              AND ended_at IS NULL

            ORDER BY started_at DESC

            LIMIT 1
            `,
            [sessionId]
          );

        broadcastId =
          existingBroadcast.rows[0]
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

          destinations =
            EXCLUDED.destinations,

          last_seen_at =
            NOW(),

          updated_at =
            NOW()
        `,
        [
          broadcastId,
          sessionId,
          installationId,
          currentDestinations,
        ]
      );
    } else {
      await client.query(
        `
        UPDATE kyros_broadcasts

        SET
          ended_at =
            COALESCE(
              ended_at,
              NOW()
            ),

          last_seen_at =
            NOW(),

          updated_at =
            NOW()

        WHERE session_id = $1
          AND ended_at IS NULL
        `,
        [sessionId]
      );

      broadcastId = null;
    }

    await client.query("COMMIT");

    return res.json({
      ok: true,

      installationId,
      sessionId,
      broadcastId,

      serverTime:
        new Date().toISOString(),

      activeTimeoutSeconds:
        ACTIVE_TIMEOUT_SECONDS,
    });
  } catch (error) {
    try {
      await client.query(
        "ROLLBACK"
      );
    } catch (_) {}

    console.error(
      "Telemetry error:",
      error
    );

    return res.status(500).json({
      ok: false,
      error: "telemetry_failed",
    });
  } finally {
    client.release();
  }
});

/* ============================================================
   ADMIN STATISTICS

   Dashboard:
       /admin.html

   Statistics API:
       /admin-stats
   ============================================================ */

app.get("/admin-stats", async (req, res) => {
  try {
    const [
      installations,
      active,
      platforms,
      countries,
      languages,
      broadcasts,
      destinationStats,
    ] = await Promise.all([
      pool.query(`
        SELECT COUNT(*)::int AS total
        FROM kyros_installations
      `),

      pool.query(
        `
        SELECT COUNT(*)::int AS total

        FROM kyros_sessions

        WHERE last_seen_at >
          NOW() -
          ($1 * INTERVAL '1 second')
        `,
        [ACTIVE_TIMEOUT_SECONDS]
      ),

      pool.query(`
        SELECT
          platform,
          COUNT(*)::int AS installations

        FROM kyros_installations

        GROUP BY platform

        ORDER BY installations DESC
      `),

      pool.query(`
        SELECT
          COALESCE(
            last_network_country,
            'unknown'
          ) AS country,

          COUNT(*)::int AS installations

        FROM kyros_installations

        GROUP BY last_network_country

        ORDER BY installations DESC
      `),

      pool.query(`
        SELECT
          COALESCE(
            app_language,
            'unknown'
          ) AS language,

          COUNT(*)::int AS installations

        FROM kyros_installations

        GROUP BY app_language

        ORDER BY installations DESC
      `),

      pool.query(`
        SELECT COUNT(*)::int AS total
        FROM kyros_broadcasts
      `),

      pool.query(`
        SELECT
          destination,
          COUNT(*)::int AS broadcasts

        FROM kyros_broadcasts,
        UNNEST(destinations) destination

        GROUP BY destination

        ORDER BY broadcasts DESC
      `),
    ]);

    return res.json({
      ok: true,

      generatedAt:
        new Date().toISOString(),

      installations: {
        total:
          installations.rows[0].total,

        activeSessions:
          active.rows[0].total,

        platforms:
          platforms.rows,

        countries:
          countries.rows,

        languages:
          languages.rows,
      },

      broadcasts: {
        total:
          broadcasts.rows[0].total,

        destinations:
          destinationStats.rows,
      },
    });
  } catch (error) {
    console.error(
      "Stats error:",
      error
    );

    return res.status(500).json({
      ok: false,
      error: "stats_failed",
    });
  }
});

/* ============================================================
   HEALTH
   ============================================================ */

app.get("/health", async (req, res) => {
  try {
    await pool.query(
      "SELECT 1"
    );

    return res.json({
      ok: true,
      service: "KyroS",
      database: "connected",
      serverTime:
        new Date().toISOString(),
    });
  } catch (error) {
    return res.status(503).json({
      ok: false,
      service: "KyroS",
      database: "unavailable",
    });
  }
});

/* ============================================================
   404
   ============================================================ */

app.use((req, res) => {
  return res.status(404).json({
    ok: false,
    error: "not_found",
  });
});

/* ============================================================
   START SERVER
   ============================================================ */

async function start() {
  try {
    await initializeDatabase();

    app.listen(PORT, () => {
      console.log(
        `KyroS server2 running on port ${PORT}`
      );

      console.log(
        `Admin dashboard: /admin.html`
      );

      console.log(
        `Admin statistics: /admin-stats`
      );
    });
  } catch (error) {
    console.error(
      "Failed to start KyroS server:",
      error
    );

    process.exit(1);
  }
}

start();
