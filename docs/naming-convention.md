# A Naming Convention for Databricks Data Sources

*Connections, foreign catalogs, JDBC view catalogs, secrets and volumes in Unity Catalog. Tested on a real multi-source lab.*

---

## Why a naming convention

Databricks doesn't impose a naming standard. If you don't set one, you end up with a metastore where nobody can tell which foreign catalog uses which connection, where `postgres`, `pgsql` and `pg` all mean the same thing, and where renaming a server breaks every downstream query.

This document describes a convention built around a few principles. It was tested against a lab with heterogeneous sources: PostgreSQL, Oracle, MariaDB, YugabyteDB, Snowflake and two Denodo instances, reached through Lakehouse Federation, JDBC connections, or both.

---

## 1. General rules

- **Lowercase `snake_case` only.** Unity Catalog stores names in lowercase. Hyphens, spaces and capitals force backticks everywhere.
- **No leading digits, no special characters.** The only exception is the leading underscore reserved for platform catalogs (see §8).
- **Logical names over physical names.** Server names, hostnames, cloud providers and database products change during a system's life. The *business system* does not. Physical details go in tags and comments, not in names.
- **Consistency beats perfection.** An imperfect rule applied everywhere is worth more than a perfect one applied half the time.

---

## 2. Where the environment suffix goes

Separate workspaces for dev and prod isolate compute, jobs and users. They do **not** isolate names: all workspaces in the same region share **one Unity Catalog metastore**, so catalog and connection names must be unique across environments. Workspace-catalog binding controls *visibility*, not *naming*.

The rule is therefore to add `_<env>` only where the object's scope is shared:

| Scope | Objects | `_<env>` suffix |
|---|---|---|
| Metastore | catalogs, connections, foreign catalogs, external locations, storage credentials | **Yes** |
| Account | groups, service principals | **Yes**, if environment-specific |
| Inside a catalog | schemas, tables, views, volumes, UC secrets | **No**: the catalog already carries it |
| Workspace | jobs, pipelines, clusters, SQL warehouses, secret scopes | **No**: the workspace already carries it |

The environment always goes **at the end** of the name, so that listings group by object type and system, with dev and prod side by side:

```
fed_billing_dev
fed_billing_prod
```

To keep code portable across environments, set a default catalog per workspace or pass the catalog name as a job parameter (see §13 for bundles). Never hard-code the environment inside table names.

---

## 3. The source ID and the source registry

The core of the convention is the **source ID**: a short, logical, stable identifier for each data source, assigned and recorded centrally.

| Source kind | Source ID is… | Examples |
|---|---|---|
| SaaS application | the product name (plus an instance qualifier if needed) | `salesforce`, `salesforce_emea`, `servicenow` |
| Application database | the business system | `billing`, `crm`, `mes` |
| Database named after its purpose | the database name | `ecom`, `chinook` |
| Lab / sandbox environment | the lab name | `oralab`, `yugalab`, `ctulab` |
| Dataset replicated on several technologies | the dataset name | `pkdd_financial` |
| Instance of a federation platform | product + instance identifier | `denodo_29090` |

**Avoid** physical database names that recur everywhere (`main`, `appdb`, `postgres`, `db01`), names that may clash with future systems (`financial`, `data`), and anything that describes *where* the data lives rather than *what* it is.

The **source registry** records each source ID together with its technology, host, database, exposed schemas, access mode (federation / JDBC / ingestion), owner and connection name. It can be a table, or a list in the setup notebook that generates every object. A registry also answers the question "which catalog uses which connection?" without any reverse engineering.

---

## 4. Connections

```
conn_<tech>_<source_id>[_<qualifier>]_<env>
```

The connection is the **physical** object: it stores *how* to connect (technology, host, credentials). Only admins and data engineers see it, so it may carry the technology code.

### Rules

- **One connection per catalog (1:1).** Create a separate connection for every foreign catalog, even when two connections point to the same server with the same credentials. The connection name then always derives from the source ID, a database can move to another server without touching its neighbours, and the connection ↔ catalog mapping is obvious from the names.
- **Technology written once.** When the source ID already starts with the technology (SaaS products, Denodo), don't repeat it: `conn_sfdc_prod`, `conn_denodo_29090_dev`, not `conn_denodo_denodo_29090_dev`.
- **Qualifier for a second access path** to the same source, for example a JDBC connection next to a federation connection: `conn_pg_pkdd_financial_jdbc_dev`.
- **Technology code = the real technology of the source**, not the Databricks connection type. YugabyteDB uses the `POSTGRESQL` type but gets `yb`. MariaDB uses `MYSQL` but gets `mariadb`.

### Technology codes (closed list)

| Code | Technology | Code | Technology |
|---|---|---|---|
| `pg` | PostgreSQL | `snowflake` | Snowflake |
| `mssql` | SQL Server | `denodo` | Denodo |
| `azsql` | Azure SQL | `sfdc` | Salesforce |
| `ora` | Oracle | `snow` | ServiceNow |
| `mysql` | MySQL | `workday` | Workday |
| `mariadb` | MariaDB | `yb` | YugabyteDB |

Extend the list deliberately and document every new code.

---

## 5. Foreign catalogs (Lakehouse Federation)

```
fed_<source_id>[_<variant>]_<env>
```

The foreign catalog is the **logical** object: it is what users query, and its name appears in every query, view and lineage graph. It therefore uses the source ID only, with no technology and no host.

### Rules

- **No technology in the name.** If the billing system migrates from PostgreSQL to Azure SQL, create `conn_azsql_billing_prod`, recreate `fed_billing_prod` on it, re-grant, and drop `conn_pg_billing_prod`. Consumers see no change.
- **Variant qualifier** for the same dataset on several technologies, when the technology *is* the subject of comparison. Add it to **all** copies, the first one included: `fed_pkdd_financial_pg_dev`, `fed_pkdd_financial_mariadb_dev`.
- **Shared databases.** When one database hosts several applications, each with its own schema, you have two options:
  - **A (preferred).** One catalog per application, each on its own connection with a database user that can only see that application's schema: `fed_crm_prod`, `fed_ticketing_prod`. Names stay business-oriented, and least privilege is enforced at the source.
  - **B.** One catalog for the whole database, under an assigned logical name (`fed_corpapps_prod`), never under the physical database name.
- **MySQL / MariaDB** have no `database` option: one foreign catalog exposes every database the connection user can see, each as a schema. In that case the catalog represents the server (for example a lab), not a single dataset.

### Required catalog options by type

| Connection type | Option |
|---|---|
| PostgreSQL, SQL Server, Synapse, Redshift, Snowflake, Teradata | `database '...'` |
| Oracle | `service_name '...'` |
| Databricks | `catalog '...'` |
| MySQL | none (`tinyInt1isBit` optional) |

---

## 6. JDBC connections and JDBC view catalogs

Use a **JDBC connection** (`TYPE JDBC`, with a driver JAR from a volume) when federation is not possible: the database is not supported (Denodo), or a requirement can't be met (a MySQL server without TLS). JDBC connections don't create foreign catalogs. They are queried with `remote_query()` or `spark.read.format("jdbc")`.

To give JDBC sources the same shape as a foreign catalog, expose them through a **JDBC view catalog**:

```
jdbc_<source_id>[_<variant>]_<env>.<source_schema>.<source_table>
```

Each view is a thin `SELECT * FROM remote_query('<connection>', query => 'SELECT * FROM "<schema>"."<table>"')`, the equivalent of a Denodo base view. The `jdbc_` prefix describes the *access method*, just as `fed_` does. If the source later becomes federable, consumers only change the prefix: `jdbc_ctulab_dev.ctu_financial.loan` becomes `fed_ctulab_dev.ctu_financial.loan`.

### Rules

- **Generate the views, don't write them by hand.** List the source tables through `remote_query` (`information_schema.tables`, or `GET_VIEWS()` on Denodo) and create one view per table. Re-run after source schema changes: view columns are fixed at creation time.
- **View owner = a group or service principal with `USE CONNECTION`.** Consumers only need `SELECT` on the view, because the connection privilege is checked on the owner.
- **Expose only what is meant to be consumed.** For a semantic platform such as Denodo, map the published (interface/derived) views, not the base views.
- **Write a dedicated view for heavy joins or aggregations.** Put the logic *inside* the `remote_query` text so that it runs entirely at the source (see §11).

---

## 7. Ingestion (SaaS via Lakeflow Connect)

Many SaaS sources are ingested rather than federated. The connection follows §4 (`conn_sfdc_prod`). Data lands in **managed tables** inside a regular catalog, with the source ID as the schema name:

```
raw_<env>.<source_id>.<object>      e.g. raw_prod.salesforce.account
```

*This section was not exercised in the lab and should be validated on a real ingestion pipeline.*

---

## 8. The platform catalog

Shared infrastructure (secrets, drivers, certificates) lives in a dedicated catalog per environment:

```
_platform_<env>
 ├─ secrets            schema   → Unity Catalog secrets. No grants by default.
 ├─ drivers            schema
 │   └─ jdbc           volume   → JDBC driver JARs. READ VOLUME for connection users.
 └─ certs              schema
     └─ ca             volume   → Server / CA certificates (PEM). Admins only.
```

- **The leading underscore** marks the catalog as infrastructure, and in most tools it sorts it above the data catalogs. Reserve the prefix for platform catalogs only.
- **Don't use the default `workspace` catalog.** It is usually bound to a single workspace, it often grants broad permissions to all workspace users, and it carries no environment in its name.
- **Separate schemas for separate grants.** Driver JARs must be readable by everyone who queries a JDBC connection. Secrets and certificates must not be.

---

## 9. Secrets

Use **Unity Catalog secrets** (`catalog.schema.secret`) rather than workspace secret scopes. They are governed by Unity Catalog privileges, audited in `system.access.audit`, and shared across the workspaces attached to the metastore.

```
_platform_<env>.secrets.<connection name without env>_<key>
```

```
_platform_dev.secrets.conn_pg_ecom_user
_platform_dev.secrets.conn_pg_ecom_password
```

- **Named by intended use**, so every secret is traceable to its consumer. The `conn_` prefix leaves room for others (`job_`, `app_`).
- **No `_<env>`**: the catalog already carries it.
- **Exception: shared identities.** When many connections use the same database account, as with the 1:1 connection rule, you can name the secret by identity instead (`pg_lab_dbxreader_password`) to avoid updating N copies at rotation time.
- **Create values in Catalog Explorer**, not in code. Read them with `dbutils.secrets.get(catalog=..., schema=..., key=...)` on serverless (environment 4+) or DBR 17.3 LTS+. SQL warehouses cannot read them directly.
- **Create connections with the Python SDK.** Credentials read from secrets then never appear in a SQL statement or in the query history. In SQL, the `secret()` function only accepts workspace secret scopes.

---

## 10. Files in volumes

| Volume | File naming | Example |
|---|---|---|
| `_platform_<env>.drivers.jdbc` | The vendor's original file name (technology + version). Keep older versions side by side: rollback means changing a path. | `postgresql-42.7.4.jar`, `denodo-vdp-jdbcdriver-9.5.0.jar` |
| `_platform_<env>.certs.ca` | `<source_id>_<description>.crt` | `yugalab_root.crt` |

Resolve driver paths with a pattern (`mariadb-java-client-*.jar`) and take the latest match, so that upgrading a driver doesn't require code changes.

---

## 11. Tags and comments

Physical facts that change over time go in **tags and comments**, never in names:

| Tag | Values |
|---|---|
| `hosting` | `onprem`, `azure`, `aws`, `gcp`, `saas` |
| `host` | hostname of the source |
| `owner` | owning team |
| `access_mode` | `federation`, `jdbc`, `ingestion` |

A connection comment that ends with `host:port` makes Catalog Explorer readable at a glance.

---

## 12. Lifecycle

| Event | Action | Existing objects |
|---|---|---|
| New source | Add a registry entry, create the secrets, run the setup | Untouched |
| New database on an existing server | New registry entry → new connection + new catalog (1:1) | Untouched |
| New database on MySQL / MariaDB | Nothing: it appears as a new schema (if visible to the connection user) | Untouched |
| New schema in a JDBC source | Add it to the source's view schemas and regenerate the views | Untouched |
| Source migrates to another server or technology | New connection, recreate the catalog with the **same name**, re-grant, drop the old connection | Consumers see no change |
| Password rotation | Update the secret, then refresh the connection options | — |

Make the setup **idempotent**: `IF NOT EXISTS` for catalogs, skip existing connections, `CREATE OR REPLACE` for generated views. Re-running it is then always safe, and it can run as the deployment job of the platform bundle (§13).

---

## 13. Deployment with Declarative Automation Bundles

Declarative Automation Bundles (formerly Databricks Asset Bundles) handle *where* things are deployed. The convention handles *what they are called*. The two meet in a single rule:

> **Bundle target names are the environment codes.** Then `${bundle.target}` *is* the `_<env>` suffix, and no environment-specific name is ever written by hand.

### Rules

1. **Targets = environments.** Use a closed list (`dev`, `test`, `prod`) and use the same strings as target names. Each target points to its environment's workspace. The metastore is shared, which is why the suffix is needed.
2. **Build metastore-level names from `${bundle.target}`**, for example `fed_billing_${bundle.target}` and `_platform_${bundle.target}`. Workspace objects (jobs, pipelines, warehouses) keep plain names.
3. **Pass catalog names to code as parameters**, never hard-coded. Code then runs unchanged in every environment.
4. **Keep the source registry as a versioned file in the platform bundle**, one per environment (`config/sources_<env>.yml`), reviewed through pull requests. It becomes the single source of truth from which every connection and catalog name is derived.
5. **Development-mode prefixes are allowed for personal sandboxes only.** `mode: development` prefixes resources with the developer's name (`[dev fred] job_name`, and a `dev_<user>_` prefix on Unity Catalog objects). This breaks the "environment at the end" rule on purpose: a leading `dev_<user>_` marks a *personal sandbox*, while a trailing `_dev` marks the *shared* dev environment. The platform bundle **never** runs in development mode.

### Two kinds of bundle

| | Platform bundle | Project bundles |
|---|---|---|
| Owner | Platform team | Data product teams |
| Creates | `_platform_<env>`, connections, `fed_` and `jdbc_` catalogs | Jobs, pipelines, project catalogs and schemas |
| Data sources | Defines them, from the registry file | Consumes them through bundle variables |
| Runs as (prod) | Platform service principal | Project service principal |

Connections are not a bundle resource type. The platform bundle therefore deploys an **idempotent setup job** that reads the registry and creates connections, foreign catalogs, JDBC connections and view catalogs. The run fails if anything is missing, so a broken source surfaces in the deployment pipeline rather than in a user's query.

```yaml
# platform bundle - databricks.yml (excerpt)
resources:
  jobs:
    federation_setup:
      name: platform_federation_setup            # workspace object: no _<env>
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: ./src/federation_setup.py
            base_parameters:
              env: ${bundle.target}
              registry_path: ${workspace.file_path}/config/sources_${bundle.target}.yml

targets:
  dev:
    default: true
    workspace: { host: https://<dev-workspace> }
  prod:
    mode: production
    workspace: { host: https://<prod-workspace> }
    run_as: { service_principal_name: <platform-sp> }
```

```yaml
# project bundle - consuming a federated source
variables:
  billing_catalog:
    default: fed_billing_${bundle.target}

resources:
  jobs:
    sales_ingest_orders:
      name: sales_ingest_orders
      parameters:
        - name: source_catalog
          default: ${var.billing_catalog}
```

Secret **values** never go in a bundle. They are created in Catalog Explorer (or through the API) in `_platform_<env>.secrets`. The setup job only checks that they exist.

---

## 14. Lessons learned from the lab

These came out of applying the convention to real sources. Each one shaped a rule above.

**Pushdown depends on the Spark dialect, not on federation vs JDBC.**
The same aggregate query was tested on three paths:

| Path | Filters pushed | Aggregates pushed |
|---|---|---|
| JDBC → MariaDB (`jdbc:mariadb:` URL) | Yes | **No** |
| JDBC → PostgreSQL | Yes | Yes |
| Federation → PostgreSQL | Yes | Yes |

Federation and JDBC produce identical plans on PostgreSQL, because both run on the same Spark JDBC engine. Spark chooses the dialect from the URL: an unknown scheme falls back to the generic dialect, which pushes filters and column pruning but not aggregates. Federation still wins on governance (a browsable catalog and table-level grants) and on join pushdown between tables of the same source.

**Lakehouse Federation requires TLS for MySQL.** A MariaDB server without TLS cannot be federated. Check before building anything: the server handshake advertises the `CLIENT_SSL` capability. There are two workarounds: a JDBC connection with `sslMode=disable` (acceptable for public test data only), or copying the data to a server you control.

**MariaDB through the generic dialect needs `ANSI_QUOTES`.** Spark quotes column names with double quotes, which MariaDB reads as string literals by default. The symptom is `value 'ok' cannot be decoded as Integer`. The fix is to add `sessionVariables=sql_mode=ANSI_QUOTES` to the JDBC URL.

**`''` is not an escape in Spark SQL.** Adjacent string literals are concatenated, so a doubled quote silently disappears. When you build `remote_query(... query => '...')` strings in code, escape quotes as `\'`.

**Server certificates are passed as text.** `userProvidedServerCertificate` expects the PEM content, not a path. Store it in the `certs` volume and read it at connection creation. It must be paired with `trustServerCertificate 'false'`.

**Serverless egress IPs are published, and they change.** To allow-list Databricks serverless on a source firewall, read cloud and region with `SELECT current_metastore()` (`<cloud>:<region>:<id>`), then filter Databricks' published `ip-ranges.json` (service `Databricks`, type `outbound`). Re-check the list periodically.

**Third-party managed accounts can close the door.** A Snowflake account managed by an ELT vendor started rejecting all client IPs through a network policy that the account user cannot change. Prefer pushing data from such platforms to a destination you control.

**Semantic layers don't travel over JDBC, but their queries do.** Denodo metric views are not recognised by Databricks as metric views, because no semantic metadata crosses a JDBC driver. You can still query them through the connection, since `remote_query` passes the text to Denodo unchanged:

```sql
SELECT * FROM remote_query('conn_denodo_29090_dev', query =>
  'SELECT trans_year, evaluate_metric(transaction_count) AS n
     FROM mv_yuga_fin_account_activity GROUP BY trans_year');
```

Denodo computes the metric, the definition stays in one place, and Databricks receives only the aggregated result. Don't map metric views into JDBC view catalogs: a `SELECT *` view cannot evaluate them.

---

## Appendix: the lab, fully named

| Source | Connection | Catalog |
|---|---|---|
| PostgreSQL `ecom` (public demo) | `conn_pg_ecom_dev` | `fed_ecom_dev` |
| Oracle Database Free (lab) | `conn_ora_oralab_dev` | `fed_oralab_dev` |
| PostgreSQL lab server, one per database | `conn_pg_alydb_dev`, `conn_pg_chinook_dev`, `conn_pg_denodo_dev`, `conn_pg_postgres_dev` | `fed_alydb_dev`, `fed_chinook_dev`, `fed_denodo_dev`, `fed_postgres_dev` |
| PKDD'99 financial, copy on PostgreSQL | `conn_pg_pkdd_financial_dev` | `fed_pkdd_financial_pg_dev` |
| PKDD'99 financial, same copy via JDBC | `conn_pg_pkdd_financial_jdbc_dev` | `jdbc_pkdd_financial_pg_dev` |
| YugabyteDB Aeon (lab, TLS + CA cert) | `conn_yb_yugalab_dev` | `fed_yugalab_dev` |
| CTU relational repository (MariaDB, no TLS) | `conn_mariadb_ctulab_dev` (JDBC) | `jdbc_ctulab_dev` |
| Denodo instance 29090 | `conn_denodo_29090_dev` (JDBC) | `jdbc_denodo_29090_dev` |
| Denodo instance 39090 | `conn_denodo_39090_dev` (JDBC) | `jdbc_denodo_39090_dev` *(planned)* |
| Snowflake (ELT vendor managed) | `conn_snowflake_keboola_dev` | `fed_keboola_dev` *(on hold)* |
| Platform | — | `_platform_dev` |
