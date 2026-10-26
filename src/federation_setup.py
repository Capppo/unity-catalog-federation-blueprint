# Databricks notebook source
# MAGIC %md
# MAGIC # Platform – federation setup
# MAGIC
# MAGIC Deployed by the `data_platform` bundle as job `platform_federation_setup`. Idempotent: safe to re-run.
# MAGIC
# MAGIC | Parameter | Set by the bundle to |
# MAGIC |---|---|
# MAGIC | `env` | `${bundle.target}` (dev, test, prod) |
# MAGIC | `registry_path` | `config/sources_<env>.yml` in the bundle |
# MAGIC | `rotate` | `true` to re-apply credentials from UC secrets to existing connections |
# MAGIC
# MAGIC Steps: platform catalog → secrets check → federation connections and foreign catalogs →
# MAGIC JDBC connections → JDBC view catalogs → verification. The run fails if anything is missing or broken.
# MAGIC
# MAGIC Requires serverless environment v4+ (or DBR 17.3 LTS+, standard access mode) to read UC secrets.

# COMMAND ----------

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("registry_path", "")
dbutils.widgets.dropdown("rotate", "false", ["false", "true"])

ENV = dbutils.widgets.get("env").strip().lower()
REGISTRY_PATH = dbutils.widgets.get("registry_path").strip()
ROTATE = dbutils.widgets.get("rotate") == "true"

assert ENV in ("dev", "test", "prod"), f"unknown environment '{ENV}'"

PLATFORM = f"_platform_{ENV}"
SECRETS_SCHEMA = "secrets"
JDBC_DRIVERS = f"/Volumes/{PLATFORM}/drivers/jdbc"
CERTS = f"/Volumes/{PLATFORM}/certs/ca"

if REGISTRY_PATH and not REGISTRY_PATH.startswith("/Workspace"):
    REGISTRY_PATH = "/Workspace" + REGISTRY_PATH

print(f"env={ENV}  platform={PLATFORM}  rotate={ROTATE}\nregistry={REGISTRY_PATH}")

# COMMAND ----------

# =====================================================================
# 1. Platform catalog (created here, not as a bundle resource, so the
#    bundle does not depend on the direct deployment engine)
# =====================================================================
for stmt in [
    f"CREATE CATALOG IF NOT EXISTS {PLATFORM} COMMENT 'Platform infrastructure (secrets, drivers, certificates) - {ENV}'",
    f"CREATE SCHEMA IF NOT EXISTS {PLATFORM}.secrets COMMENT 'Unity Catalog secrets, named by intended use. Restricted access.'",
    f"CREATE SCHEMA IF NOT EXISTS {PLATFORM}.drivers COMMENT 'Shared drivers and libraries'",
    f"CREATE VOLUME IF NOT EXISTS {PLATFORM}.drivers.jdbc COMMENT 'JDBC drivers, original file names'",
    f"CREATE SCHEMA IF NOT EXISTS {PLATFORM}.certs COMMENT 'Server/CA certificates (PEM). Admins only.'",
    f"CREATE VOLUME IF NOT EXISTS {PLATFORM}.certs.ca COMMENT 'CA / server certificates: <source_id>_<description>.crt'",
    # Drivers readable by anyone querying JDBC connections; secrets and certs get no grants
    f"GRANT USE CATALOG ON CATALOG {PLATFORM} TO `account users`",
    f"GRANT USE SCHEMA ON SCHEMA {PLATFORM}.drivers TO `account users`",
    f"GRANT READ VOLUME ON VOLUME {PLATFORM}.drivers.jdbc TO `account users`",
]:
    spark.sql(stmt)
print(f"OK  {PLATFORM}")

# COMMAND ----------

# =====================================================================
# 2. Registry and naming rules
# =====================================================================
import yaml

with open(REGISTRY_PATH) as f:
    REGISTRY = yaml.safe_load(f)

FED = REGISTRY.get("federation") or []
JDBC = REGISTRY.get("jdbc") or []
VIEW_OWNER = REGISTRY.get("view_owner") or None


def conn_base(s):
    """conn_<tech>_<source_id>[_<qualifier>]; tech written once if source_id starts with it."""
    sid, tech = s["source_id"], s["tech"]
    base = f"conn_{sid}" if sid.startswith(f"{tech}_") else f"conn_{tech}_{sid}"
    return base + (f"_{s['qualifier']}" if s.get("qualifier") else "")


def conn_name(s):
    return f"{conn_base(s)}_{ENV}"


def secret_prefix(s):
    """Default: connection name without qualifier and env; overridable for shared identities."""
    if s.get("secret_prefix"):
        return s["secret_prefix"]
    sid, tech = s["source_id"], s["tech"]
    return f"conn_{sid}" if sid.startswith(f"{tech}_") else f"conn_{tech}_{sid}"


def catalog_name(s, prefix):
    variant = f"_{s['variant']}" if s.get("variant") else ""
    return f"{prefix}_{s['source_id']}{variant}_{ENV}"


for s in FED:
    print(f"{conn_name(s):40} -> {catalog_name(s, 'fed')}")
for s in JDBC:
    print(f"{conn_name(s):40} -> {catalog_name(s, 'jdbc')} (views)")

# COMMAND ----------

# =====================================================================
# 3. Secrets check (values are created in Catalog Explorer, never in code)
# =====================================================================
required = list(dict.fromkeys(
    f"{secret_prefix(s)}_{k}" for s in FED + JDBC for k in ("user", "password")))

try:
    present = {m.key.split(".")[-1]
               for m in dbutils.secrets.list(catalog=PLATFORM, schema=SECRETS_SCHEMA)}
except Exception as e:
    present = set()
    print(f"Unable to list secrets: {e}")

missing = {k for k in required if k not in present}
for k in required:
    print(f"{'OK     ' if k not in missing else 'MISSING'}  {PLATFORM}.{SECRETS_SCHEMA}.{k}")


def secret(key):
    return dbutils.secrets.get(catalog=PLATFORM, schema=SECRETS_SCHEMA, key=key)


def has_secrets(s):
    return not {f"{secret_prefix(s)}_user", f"{secret_prefix(s)}_password"} & missing

# COMMAND ----------

# =====================================================================
# 4. Federation connections and foreign catalogs
#    Connections are created with the SDK: credentials never appear in SQL or query history.
# =====================================================================
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType

w = WorkspaceClient()
existing_conns = {c.name for c in w.connections.list()}
report = []          # (object, kind, status, detail)


def sql_lit(x):
    """Databricks SQL string literal ('' is NOT an escape in Spark SQL)."""
    return "'" + x.replace("\\", "\\\\").replace("'", "\\'") + "'"


def upsert_connection(name, ctype, options, comment, env_settings=None):
    if name in existing_conns:
        if ROTATE:
            w.connections.update(name=name, options=options)
            return "ROTATED"
        return "EXISTS"
    kwargs = dict(name=name, connection_type=ctype, options=options, comment=comment)
    if env_settings is not None:
        kwargs["environment_settings"] = env_settings
    w.connections.create(**kwargs)
    existing_conns.add(name)
    return "CREATED"


for s in FED:
    conn, cat = conn_name(s), catalog_name(s, "fed")
    if not has_secrets(s):
        report.append((conn, "connection", "SKIPPED", "missing secrets"))
        continue

    options = {"host": s["host"], "port": str(s["port"]),
               "user": secret(f"{secret_prefix(s)}_user"),
               "password": secret(f"{secret_prefix(s)}_password"),
               **(s.get("connection_options") or {})}
    if s.get("cert"):
        try:
            with open(f"{CERTS}/{s['cert']}") as f:
                options["userProvidedServerCertificate"] = f.read().strip()
            options["trustServerCertificate"] = "false"
        except Exception as e:
            report.append((conn, "connection", "SKIPPED", f"certificate: {e}"))
            continue

    try:
        status = upsert_connection(conn, ConnectionType(s["type"]), options,
                                   f"{s['comment']} | {s['host']}:{s['port']}")
        report.append((conn, "connection", status, ""))
    except Exception as e:
        report.append((conn, "connection", "ERROR", str(e).splitlines()[0][:200]))
        continue

    stmt = f"CREATE FOREIGN CATALOG IF NOT EXISTS {cat} USING CONNECTION {conn} COMMENT {sql_lit(s['comment'])}"
    if s.get("catalog_options"):
        stmt += " OPTIONS (" + ", ".join(f"{k} {sql_lit(str(v))}" for k, v in s["catalog_options"].items()) + ")"
    try:
        spark.sql(stmt)
        report.append((cat, "foreign catalog", "OK", ""))
    except Exception as e:
        report.append((cat, "foreign catalog", "ERROR", str(e).splitlines()[0][:200]))

# COMMAND ----------

# =====================================================================
# 5. JDBC connections (driver JARs resolved from the drivers volume)
# =====================================================================
import glob
from databricks.sdk.service.catalog import EnvironmentSettings

DEFAULT_ALLOW = "dbtable,query,partitionColumn,lowerBound,upperBound,numPartitions,fetchsize"


def resolve_jars(patterns):
    out = []
    for p in patterns:
        hits = sorted(glob.glob(f"{JDBC_DRIVERS}/{p}"))
        if not hits:
            raise FileNotFoundError(f"no driver matching {JDBC_DRIVERS}/{p}")
        out.append(hits[-1])
    return out


jdbc_ready = []
for s in JDBC:
    conn = conn_name(s)
    if not has_secrets(s):
        report.append((conn, "jdbc connection", "SKIPPED", "missing secrets"))
        continue
    try:
        jars = resolve_jars(s["jars"])
        options = {"url": s["url"],
                   "user": secret(f"{secret_prefix(s)}_user"),
                   "password": secret(f"{secret_prefix(s)}_password"),
                   "externalOptionsAllowList": s.get("allow", DEFAULT_ALLOW)}
        status = upsert_connection(conn, ConnectionType.JDBC, options,
                                   f"{s['comment']} | driver: {jars[0].rsplit('/', 1)[-1]}",
                                   EnvironmentSettings(java_dependencies=jars))
        spark.sql(f"SELECT * FROM remote_query('{conn}', query => 'SELECT 1 AS ok')").collect()
        report.append((conn, "jdbc connection", status, "query OK"))
        jdbc_ready.append(s)
    except Exception as e:
        report.append((conn, "jdbc connection", "ERROR", str(e).splitlines()[0][:200]))

# COMMAND ----------

# =====================================================================
# 6. JDBC view catalogs: one view per source table (like Denodo base views)
#    jdbc_<source_id>[_<variant>]_<env>.<view_schema>.<table>
# =====================================================================
import re

TABLE_LIST_QUERY = {
    "mariadb": "SELECT table_name AS t FROM information_schema.tables "
               "WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'",
    "pg":      "SELECT table_name AS t FROM information_schema.tables "
               "WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'",
    "denodo":  "SELECT name AS t FROM get_views() WHERE input_database_name = '{schema}'",
}


def uc_name(x):
    return re.sub(r"[^a-z0-9_]", "_", x.lower())


def q(x):
    return f'"{x}"'


for s in jdbc_ready:
    if not s.get("view_schemas"):
        continue
    conn, cat = conn_name(s), catalog_name(s, "jdbc")
    if s["tech"] not in TABLE_LIST_QUERY:
        report.append((cat, "view catalog", "SKIPPED", f"no table-list query for '{s['tech']}'"))
        continue

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat} COMMENT {sql_lit(f'Views over JDBC connection {conn}')}")
    for src_schema in s["view_schemas"]:
        sch = uc_name(src_schema)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch} COMMENT {sql_lit(f'Source schema {src_schema} via {conn}')}")
        list_sql = TABLE_LIST_QUERY[s["tech"]].format(schema=src_schema)
        tables = [r.t for r in spark.sql(
            f"SELECT * FROM remote_query('{conn}', query => {sql_lit(list_sql)})").collect()]

        ok = err = 0
        for t in tables:
            view = f"{cat}.{sch}.`{uc_name(t)}`"
            remote_sql = f"SELECT * FROM {q(src_schema)}.{q(t)}"
            try:
                spark.sql(f"CREATE OR REPLACE VIEW {view} "
                          f"COMMENT {sql_lit(f'Base view of {src_schema}.{t} via {conn}')} "
                          f"AS SELECT * FROM remote_query('{conn}', query => {sql_lit(remote_sql)})")
                if VIEW_OWNER:
                    spark.sql(f"ALTER VIEW {view} OWNER TO `{VIEW_OWNER}`")
                ok += 1
            except Exception as e:
                err += 1
                print(f"view {view}: {str(e).splitlines()[0][:150]}")
        # Errors on single views (e.g. Denodo metric views, which need evaluate_metric) are reported, not fatal
        report.append((f"{cat}.{sch}", "view schema", "OK" if ok else "ERROR",
                       f"{ok} views created, {err} failed"))

# COMMAND ----------

# =====================================================================
# 7. Verification and run outcome
# =====================================================================
actual = {c.name: c.connection_name for c in w.catalogs.list() if c.connection_name}
for s in FED:
    cat, conn = catalog_name(s, "fed"), conn_name(s)
    if cat not in actual:
        continue                                   # already reported as missing/error above
    if actual[cat] != conn:
        report.append((cat, "check", "ERROR", f"linked to {actual[cat]}, expected {conn}"))
        continue
    try:
        n = spark.sql(f"SHOW SCHEMAS IN {cat}").count()
        report.append((cat, "check", "OK", f"{n} schemas"))
    except Exception as e:
        report.append((cat, "check", "ERROR", "unreachable: " + str(e).splitlines()[0][:180]))

display(spark.createDataFrame(report, "object string, kind string, status string, detail string"))

failed = [r for r in report if r[2] in ("ERROR", "SKIPPED")]
if failed:
    raise RuntimeError(f"{len(failed)} object(s) failed or skipped: "
                       + ", ".join(f"{o} ({s})" for o, _, s, _ in failed))
