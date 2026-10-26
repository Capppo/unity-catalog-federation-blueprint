# unity-catalog-federation-blueprint

**A naming convention and a deployable bundle for data sources in Databricks Unity Catalog**: connections, Lakehouse Federation foreign catalogs, JDBC connections, Unity Catalog secrets, driver and certificate volumes.

It was born from a hands-on lab with heterogeneous sources (PostgreSQL, Oracle, MariaDB, YugabyteDB, Snowflake, Denodo) and from the questions that came up along the way. How do you name a connection so you know which catalog uses it? What happens when the same dataset lives on three technologies? How do you federate a server without TLS? Why does an aggregate get pushed down on one source and not on another?

> **Disclaimer.** This is an independent community project. It is not affiliated with, endorsed by, or supported by Databricks, Inc. or Denodo Technologies. Product names are trademarks of their respective owners.

---

## What's inside

| Path | Content |
|---|---|
| [`docs/naming-convention.md`](docs/naming-convention.md) | **The convention**: rules, rationale, edge cases, bundle integration and lessons learned |
| `databricks.yml` | Declarative Automation Bundle (formerly Databricks Asset Bundle) defining the setup job and the `dev` / `test` / `prod` targets |
| `src/federation_setup.py` | Idempotent setup notebook: platform catalog, secrets check, connections, foreign catalogs, JDBC connections, generated JDBC view catalogs, verification |
| `config/sources_example.yml` | Example **source registry**: the single file from which every name is derived |
| `examples/federation_tests.sql` | Three test queries: consistency across copies, cross-source join, semantic layer vs raw data |

## The convention in one table

| Object | Pattern | Example |
|---|---|---|
| Connection | `conn_<tech>_<source_id>[_<qualifier>]_<env>` | `conn_pg_billing_prod` |
| Foreign catalog | `fed_<source_id>[_<variant>]_<env>` | `fed_billing_prod` |
| JDBC view catalog | `jdbc_<source_id>[_<variant>]_<env>.<schema>.<table>` | `jdbc_denodo_29090_dev.financial.customer` |
| Platform catalog | `_platform_<env>` | `_platform_prod` |
| Secret | `_platform_<env>.secrets.<connection w/o env>_<key>` | `_platform_prod.secrets.conn_pg_billing_password` |
| JDBC driver | `/Volumes/_platform_<env>/drivers/jdbc/<original file>.jar` | `postgresql-42.7.4.jar` |
| Certificate | `/Volumes/_platform_<env>/certs/ca/<source_id>_<desc>.crt` | `yugalab_root.crt` |

Key ideas: **logical source IDs instead of physical names**, **one connection per catalog**, **environment suffix only on metastore-level objects**, and **bundle target = environment code**. The full reasoning is in [`docs/naming-convention.md`](docs/naming-convention.md).

---

## Quick start

**Prerequisites:** a Unity Catalog-enabled workspace, the Databricks CLI with bundle support, `CREATE CATALOG` and `CREATE CONNECTION` on the metastore, and serverless compute (environment v4+) or DBR 17.3 LTS+ in standard access mode.

1. **Create your registry.** Copy the example and describe your sources:
   ```bash
   cp config/sources_example.yml config/sources_dev.yml
   ```
   `config/sources_*.yml` is git-ignored (except the example), so your hosts stay out of the repository.

2. **Set your workspaces** in `databricks.yml` (`targets.dev.workspace.host`, and `prod` with a service principal in `run_as`).

3. **Deploy and run once** to create `_platform_dev`. The job fails and lists the missing secrets:
   ```bash
   databricks bundle deploy -t dev
   databricks bundle run federation_setup -t dev
   ```

4. **Create the secrets** listed in the job output, in Catalog Explorer → `_platform_dev.secrets` → *Create → Secret*: `<connection without env>_user` and `<connection without env>_password`.

5. **Upload JDBC drivers and certificates**, if your registry uses them, to `/Volumes/_platform_dev/drivers/jdbc/` and `/Volumes/_platform_dev/certs/ca/`.

6. **Run again.** Everything should end up `CREATED` / `EXISTS` / `OK`:
   ```bash
   databricks bundle run federation_setup -t dev
   ```

To rotate credentials after changing a secret value:
```bash
databricks bundle run federation_setup -t dev --notebook-params rotate=true
```

---

## Things worth knowing

- **Connections are not a bundle resource type**, so the bundle deploys an idempotent job that creates them from the registry.
- **Credentials never appear in SQL**: connections are created with the Python SDK from values read from Unity Catalog secrets.
- **Federation requires TLS for MySQL/MariaDB.** For servers without TLS the blueprint offers a JDBC connection plus generated views.
- **Pushdown depends on the Spark dialect**, chosen from the JDBC URL. On PostgreSQL, federation and JDBC push filters *and* aggregates. Through the generic dialect, only filters are pushed.
- **Denodo metric views** can't be mapped as views, but they can be queried through the connection with `remote_query()` and `evaluate_metric()`.

Details and more in the *Lessons learned* section of the convention.

---

## Contributing

Issues and pull requests are welcome, especially new technology codes, `TABLE_LIST_QUERY` entries for other JDBC sources, and real-world edge cases that challenge the convention.

## License

[MIT](LICENSE)
