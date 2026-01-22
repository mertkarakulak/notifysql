# NotifySQL

NotifySQL is a cron-friendly CLI that runs an SQL query and emails the result. It supports a TOML config file plus CLI flag overrides, and can output results as CSV, plain text, or an HTML table.

## Features

- Runs a single SQL query and emails the result
- Configurable via TOML file and CLI flags (flags override config)
- Output formats: CSV attachment, plain text, or HTML table
- SMTP with STARTTLS support
- CC and BCC support
- Connection-safe: opens and closes DB/SMTP connections per run
- Test flags for DB and mail
- Debug mode shows full SMTP dialogue

## Supported Databases

- MySQL / MariaDB
- PostgreSQL
- Microsoft SQL Server (MSSQL)
- ClickHouse

## Install

### macOS/Linux

```bash
git clone https://github.com/mertkarakulak/notifysql.git
cd notifysql
go mod tidy
go build -o notifysql
```

### Windows (PowerShell)

```powershell
git clone https://github.com/mertkarakulak/notifysql.git
cd notifysql
go mod tidy
go build -o notifysql.exe
```

## Quick Start

### macOS/Linux

```bash
cp config.sample.toml config.toml
./notifysql -sql "select * from users"
```

### Windows (PowerShell)

```powershell
copy config.sample.toml config.toml
.\notifysql.exe -sql "select * from users"
```

## Usage

```bash
# Run a one-off query (uses config.toml for DB/SMTP)
./notifysql -sql "select * from users" -output table

# Override DB and SMTP using flags
./notifysql -sql "select count(*) from orders" \
  -db-type postgres -db-host 127.0.0.1 -db-user app -db-pass secret -db-name app \
  -smtp-host smtp.example.com -smtp-port 587 -smtp-from report@example.com -smtp-to ops@example.com

# Test only
./notifysql -test-db
./notifysql -test-mail
```

### Windows (PowerShell) Example

```powershell
# Run a one-off query (uses config.toml for DB/SMTP)
.\notifysql.exe -sql "select * from users" -output table

# Override DB and SMTP using flags
.\notifysql.exe -sql "select count(*) from orders" `
  -db-type postgres -db-host 127.0.0.1 -db-user app -db-pass secret -db-name app `
  -smtp-host smtp.example.com -smtp-port 587 -smtp-from report@example.com -smtp-to ops@example.com
```

## Sample Config

See `config.sample.toml` for a full configuration template.

### Flags

- `-config` Path to TOML config file (default: `config.toml`)
- `-sql` SQL query to run
- `-output` Output format: `csv`, `text`, or `table`
- `-db-type` `mysql`, `mariadb`, `postgres`, `postgresql`, `pgx`, `mssql`, `sqlserver`, `clickhouse`
- `-db-host` Database host
- `-db-port` Database port
- `-db-user` Database user
- `-db-pass` Database password
- `-db-name` Database name
- `-db-sslmode` SSL mode (Postgres) or `require` for ClickHouse secure connection
- `-db-dsn` Full DSN (overrides host/user/pass/name)
- `-smtp-host` SMTP host
- `-smtp-port` SMTP port
- `-smtp-user` SMTP user
- `-smtp-pass` SMTP password
- `-smtp-from` From address
- `-smtp-to` Comma-separated recipients
- `-smtp-cc` Comma-separated CC list
- `-smtp-bcc` Comma-separated BCC list
- `-smtp-subject` Subject line
- `-smtp-tls` Use STARTTLS (`true`/`false`)
- `-test-db` Test DB connection only
- `-test-mail` Send test mail only
- `-show-query` Include SQL query in email (`true`/`false`)
- `-debug` Print SMTP dialogue and DB steps

### Required vs Optional Flags

Required means the value must be provided either by flags or in the config file.

**Normal run (no `-test-db` / `-test-mail`)**
- Required: `-sql`, `-db-type`, and either `-db-dsn` or (`-db-host`, `-db-user`, `-db-name`)
- Required: `-smtp-host`, `-smtp-port`, `-smtp-from`, `-smtp-to`
- Optional: `-db-port`, `-db-pass`, `-db-sslmode`, `-output`, `-smtp-user`, `-smtp-pass`, `-smtp-cc`, `-smtp-bcc`, `-smtp-subject`, `-smtp-tls`, `-show-query`, `-debug`

**`-test-db`**
- Required: `-db-type`, and either `-db-dsn` or (`-db-host`, `-db-user`, `-db-name`)
- Optional: `-db-port`, `-db-pass`, `-db-sslmode`, `-debug`

**`-test-mail`**
- Required: `-smtp-host`, `-smtp-port`, `-smtp-from`, `-smtp-to`
- Optional: `-smtp-user`, `-smtp-pass`, `-smtp-cc`, `-smtp-bcc`, `-smtp-subject`, `-smtp-tls`, `-debug`

**Config file note**
- `-config` is optional. If you pass it, the file must exist. If you do not pass it and `config.toml` is missing, the app still runs as long as required values are provided via flags.

## Output Formats

- `csv` (default): CSV attachment (`result.csv`)
- `text`: Tab-delimited plain text in mail body
- `table`: HTML table in mail body

## SMTP Debug Example

```bash
./notifysql -test-mail -debug
```

The debug output prints both client (`C:`) and server (`S:`) lines, with AUTH data redacted.

## Cron Example

```bash
# Every day at 08:00
0 8 * * * /usr/local/bin/notifysql -config /etc/notifysql/config.toml
```

## Notes

- The app opens DB and SMTP connections per run and closes them when finished.
- For large result sets, consider filtering your query.

## License

MIT
