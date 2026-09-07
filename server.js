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
  ssl: { rejectUnauthorized: false },
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
      CREATE TABLE IF NOT EXISTS kyros_issue_reports (
        issue_id UUID PRIMARY KEY,

        installation_id UUID
          REFERENCES kyros_installations(installation_id)
          ON DELETE SET NULL,

        session_id UUID
          REFERENCES kyros_sessions(session_id)
          ON DELETE SET NULL,

        email VARCHAR(320),

        title VARCHAR(200) NOT NULL,

        description TEXT NOT NULL,

        screenshots JSONB NOT NULL DEFAULT '[]'::jsonb,

        platform VARCHAR(20),

        app_version VARCHAR(50),

        app_build VARCHAR(50),

        device_model VARCHAR(150),

        addressed BOOLEAN NOT NULL DEFAULT FALSE,

        addressed_at TIMESTAMPTZ,

        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

function str(v, max = 200) {
  if (typeof v !== "string") {
    return null;
  }

  const s = v.trim();

  return s
    ? s.slice(0, max)
    : null;
}

function int(v, min = null, max = null) {
  const n = Number(v);

  if (!Number.isInteger(n)) {
    return null;
  }

  if (min !== null && n < min) {
    return null;
  }

  if (max !== null && n > max) {
    return null;
  }

  return n;
}

function num(v, min = null, max = null) {
  const n = Number(v);

  if (!Number.isFinite(n)) {
    return null;
  }

  if (min !== null && n < min) {
    return null;
  }

  if (max !== null && n > max) {
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
  const p = (
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
          (x) =>
            typeof x === "string"
        )
        .map(
          (x) =>
            x.trim().toLowerCase()
        )
        .filter(
          (x) =>
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
      (x) =>
        typeof x === "string"
    )
    .map(
      (x) =>
        x.trim()
    )
    .filter(Boolean)
    .slice(0, 6)
    .map(
      (x) =>
        x.slice(0, 1000)
    );
}

function limitInt(v, fallback, min, max) {
  const n = Number.parseInt(v, 10);

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
          req.params.platform || ""
        ).toLowerCase()
      ];

    if (!item) {
      return res.status(404).json({
        ok: false,
        error: "unsupported_platform",
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
      return res.status(503).json({
        ok: false,
        error: "release_not_available",
        platform: req.params.platform,
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
      const [name, item]
      of Object.entries(DOWNLOADS)
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
   ONE TELEMETRY ENDPOINT
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
        return res.status(400).json({
          ok: false,
          error: "invalid_installation_id",
        });
      }

      if (!uuid(sessionId)) {
        sessionId =
          crypto.randomUUID();
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

      await client.query("BEGIN");

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
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
          $12,$13,$14,$15,$16,$17,$18,$19,$20,
          $21,$22,NOW(),NOW(),NOW(),NOW()
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

          last_seen_at =
            NOW(),

          updated_at =
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
          $1,$2,NOW(),NOW(),$3,$4,$5,$6,$7,$8,
          $9,$10,$11,$12,$13,NOW(),NOW()
        )

        ON CONFLICT (session_id)
        DO UPDATE SET

          last_seen_at =
            NOW(),

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

          updated_at =
            NOW()
        `,
        [
          sessionId,

          installationId,

          str(
            state.appState,
            50
          ) ||
          "unknown",

          bool(
            state.foreground
          ) ??
          true,

          broadcasting,

          bool(
            state.recording
          ) ??
          false,

          bool(
            state.screenSharing
          ) ??
          false,

          bool(
            state.remoteCamera
          ) ??
          false,

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

              WHERE
                session_id = $1
                AND ended_at IS NULL

              ORDER BY
                started_at DESC

              LIMIT 1
              `,
              [
                sessionId,
              ]
            );

          broadcastId =
            found.rows[0]?.broadcast_id ||
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
            $1,$2,$3,$4,NOW(),NOW(),NOW(),NOW()
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
            dests,
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

          WHERE
            session_id = $1
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
          new Date().toISOString(),

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

      res.status(500).json({
        ok: false,
        error: "telemetry_failed",
      });
    } finally {
      client.release();
    }
  }
);

/* ============================================================
   BUG / ISSUE REPORTING

   App submission:
   POST /api/issues

   Admin list:
   GET /admin-issues

   Admin status:
   PATCH /admin-issues/:issueId
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

      if (
        !title ||
        !description
      ) {
        return res.status(400).json({
          ok: false,
          error:
            "title_and_description_required",
        });
      }

      /*
        Only keep foreign keys that actually exist.

        This allows a report to be accepted even if it
        arrives before the first telemetry request.
      */

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

            WHERE installation_id = $1
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

            WHERE session_id = $1
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
            addressed,
            created_at,
            updated_at
          )

          VALUES (
            $1,$2,$3,$4,$5,$6,$7::jsonb,
            $8,$9,$10,$11,FALSE,NOW(),NOW()
          )

          ON CONFLICT (issue_id)
          DO UPDATE SET

            email =
              COALESCE(
                EXCLUDED.email,
                kyros_issue_reports.email
              ),

            title =
              EXCLUDED.title,

            description =
              EXCLUDED.description,

            screenshots =
              EXCLUDED.screenshots,

            platform =
              COALESCE(
                EXCLUDED.platform,
                kyros_issue_reports.platform
              ),

            app_version =
              COALESCE(
                EXCLUDED.app_version,
                kyros_issue_reports.app_version
              ),

            app_build =
              COALESCE(
                EXCLUDED.app_build,
                kyros_issue_reports.app_build
              ),

            device_model =
              COALESCE(
                EXCLUDED.device_model,
                kyros_issue_reports.device_model
              ),

            updated_at =
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
          ]
        );

      res.status(201).json({
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

      res.status(500).json({
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

      if (status === "open") {
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

        where.push(
          `
          (
            r.title ILIKE $${values.length}
            OR r.description ILIKE $${values.length}
            OR COALESCE(r.email,'') ILIKE $${values.length}
            OR COALESCE(r.device_model,'') ILIKE $${values.length}
          )
          `
        );
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
              r.addressed,
              r.addressed_at,
              r.created_at,
              r.updated_at,
              i.last_network_country

            FROM kyros_issue_reports r

            LEFT JOIN kyros_installations i
              ON i.installation_id =
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

          pool.query(
            `
            SELECT
              COUNT(*)::int AS total,

              COUNT(*) FILTER (
                WHERE addressed=FALSE
              )::int AS open,

              COUNT(*) FILTER (
                WHERE addressed=TRUE
              )::int AS addressed

            FROM kyros_issue_reports
            `
          ),
        ]);

      res.json({
        ok: true,

        generatedAt:
          new Date().toISOString(),

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

      res.status(500).json({
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
        return res.status(400).json({
          ok: false,
          error: "invalid_issue_id",
        });
      }

      if (
        typeof req.body?.addressed !==
        "boolean"
      ) {
        return res.status(400).json({
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
            addressed = $2,

            addressed_at =
              CASE
                WHEN $2
                THEN COALESCE(
                  addressed_at,
                  NOW()
                )
                ELSE NULL
              END,

            updated_at =
              NOW()

          WHERE issue_id = $1

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
        return res.status(404).json({
          ok: false,
          error: "issue_not_found",
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

      res.status(500).json({
        ok: false,
        error:
          "issue_update_failed",
      });
    }
  }
);

/* ============================================================
   ADMIN AGGREGATE STATS

   Protect this route before public production.
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
      ] =
        await Promise.all([
          pool.query(
            `
            SELECT
              COUNT(*)::int total

            FROM kyros_installations
            `
          ),

          pool.query(
            `
            SELECT
              COUNT(
                DISTINCT installation_id
              )::int total

            FROM kyros_sessions

            WHERE
              last_seen_at >
              NOW() -
              (
                $1 *
                INTERVAL '1 second'
              )
            `,
            [
              ACTIVE_TIMEOUT_SECONDS,
            ]
          ),

          pool.query(
            `
            SELECT
              platform,
              COUNT(*)::int installations

            FROM kyros_installations

            GROUP BY platform

            ORDER BY installations DESC
            `
          ),

          pool.query(
            `
            SELECT
              COALESCE(
                last_network_country,
                'unknown'
              ) country,

              COUNT(*)::int installations

            FROM kyros_installations

            GROUP BY
              last_network_country

            ORDER BY
              installations DESC
            `
          ),

          pool.query(
            `
            SELECT
              COALESCE(
                app_language,
                'unknown'
              ) language,

              COUNT(*)::int installations

            FROM kyros_installations

            GROUP BY
              app_language

            ORDER BY
              installations DESC
            `
          ),

          pool.query(
            `
            SELECT
              COUNT(*)::int total

            FROM kyros_broadcasts
            `
          ),

          pool.query(
            `
            SELECT
              destination,
              COUNT(*)::int broadcasts

            FROM
              kyros_broadcasts,
              UNNEST(destinations) destination

            GROUP BY
              destination

            ORDER BY
              broadcasts DESC
            `
          ),

          pool.query(
            `
            SELECT
              COUNT(*)::int total,

              COUNT(*) FILTER (
                WHERE addressed=FALSE
              )::int open,

              COUNT(*) FILTER (
                WHERE addressed=TRUE
              )::int addressed

            FROM kyros_issue_reports
            `
          ),
        ]);

      res.json({
        ok: true,

        generatedAt:
          new Date().toISOString(),

        installations: {
          total:
            inst.rows[0].total,

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
            dests.rows,
        },

        issues:
          issues.rows[0],
      });
    } catch (e) {
      console.error(
        "Stats error:",
        e
      );

      res.status(500).json({
        ok: false,
        error:
          "stats_failed",
      });
    }
  }
);

app.get(
  "/health",
  async (req, res) => {
    try {
      await pool.query(
        "SELECT 1"
      );

      res.json({
        ok: true,
        service: "KyroS",
        database: "connected",
        serverTime:
          new Date().toISOString(),
      });
    } catch (_) {
      res.status(503).json({
        ok: false,
        service: "KyroS",
        database: "unavailable",
      });
    }
  }
);

app.use(
  (req, res) =>
    res.status(404).json({
      ok: false,
      error: "not_found",
    })
);

async function start() {
  try {
    await initializeDatabase();

    app.listen(
      PORT,
      () => {
        console.log(
          `KyroS server running on port ${PORT}`
        );
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
