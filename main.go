package main

import (
	"bytes"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"html"
	"net"
	"net/smtp"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	SQL       string     `toml:"sql"`
	Output    string     `toml:"output"`
	ShowQuery *bool      `toml:"show_query"`
	DB        DBConfig   `toml:"db"`
	SMTP      SMTPConfig `toml:"smtp"`
}

type DBConfig struct {
	Type    string `toml:"type"`
	Host    string `toml:"host"`
	Port    int    `toml:"port"`
	User    string `toml:"user"`
	Pass    string `toml:"pass"`
	Name    string `toml:"name"`
	SSLMode string `toml:"ssl_mode"`
	DSN     string `toml:"dsn"`
}

type SMTPConfig struct {
	Host    string   `toml:"host"`
	Port    int      `toml:"port"`
	User    string   `toml:"user"`
	Pass    string   `toml:"pass"`
	From    string   `toml:"from"`
	To      []string `toml:"to"`
	Cc      []string `toml:"cc"`
	Bcc     []string `toml:"bcc"`
	Subject string   `toml:"subject"`
	TLS     bool     `toml:"tls"`
}

type optionalBool struct {
	set   bool
	value bool
}

func (o *optionalBool) String() string {
	if !o.set {
		return ""
	}
	return strconv.FormatBool(o.value)
}

func (o *optionalBool) Set(value string) error {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	o.value = parsed
	o.set = true
	return nil
}

type optionalInt struct {
	set   bool
	value int
}

func (o *optionalInt) String() string {
	if !o.set {
		return ""
	}
	return strconv.Itoa(o.value)
}

func (o *optionalInt) Set(value string) error {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	o.value = parsed
	o.set = true
	return nil
}

func main() {
	configPath := flag.String("config", "config.toml", "Config file path")
	sqlFlag := flag.String("sql", "", "SQL query to run")
	outputFlag := flag.String("output", "", "Output format: csv, text, or table")
	mailTest := flag.Bool("test-mail", false, "Send test email only")
	dbTest := flag.Bool("test-db", false, "Test database connection only")
	debug := flag.Bool("debug", false, "Enable debug output")
	var showQueryFlag optionalBool

	var dbPort optionalInt
	var smtpPort optionalInt
	var smtpTLS optionalBool

	flag.String("db-type", "", "Database type: mysql or postgres")
	flag.String("db-host", "", "Database host")
	flag.Var(&dbPort, "db-port", "Database port")
	flag.String("db-user", "", "Database user")
	flag.String("db-pass", "", "Database password")
	flag.String("db-name", "", "Database name")
	flag.String("db-sslmode", "", "Database sslmode (postgres only)")
	flag.String("db-dsn", "", "Database DSN (overrides host/user/pass/name)")

	flag.String("smtp-host", "", "SMTP host")
	flag.Var(&smtpPort, "smtp-port", "SMTP port")
	flag.String("smtp-user", "", "SMTP user")
	flag.String("smtp-pass", "", "SMTP password")
	flag.String("smtp-from", "", "SMTP from address")
	flag.String("smtp-to", "", "Comma-separated to addresses")
	flag.String("smtp-cc", "", "Comma-separated cc addresses")
	flag.String("smtp-bcc", "", "Comma-separated bcc addresses")
	flag.String("smtp-subject", "", "Mail subject")
	flag.Var(&smtpTLS, "smtp-tls", "Use STARTTLS (true/false)")
	flag.Var(&showQueryFlag, "show-query", "Include SQL query in email (true/false)")

	flag.Parse()

	config, err := loadConfig(*configPath, flagPassed("config"))
	if err != nil {
		fatal(err)
	}

	config.SQL = overrideString(config.SQL, *sqlFlag)
	config.Output = overrideString(config.Output, *outputFlag)
	config.DB.Type = overrideString(config.DB.Type, flag.Lookup("db-type").Value.String())
	config.DB.Host = overrideString(config.DB.Host, flag.Lookup("db-host").Value.String())
	if dbPort.set {
		config.DB.Port = dbPort.value
	}
	config.DB.User = overrideString(config.DB.User, flag.Lookup("db-user").Value.String())
	config.DB.Pass = overrideString(config.DB.Pass, flag.Lookup("db-pass").Value.String())
	config.DB.Name = overrideString(config.DB.Name, flag.Lookup("db-name").Value.String())
	config.DB.SSLMode = overrideString(config.DB.SSLMode, flag.Lookup("db-sslmode").Value.String())
	config.DB.DSN = overrideString(config.DB.DSN, flag.Lookup("db-dsn").Value.String())

	config.SMTP.Host = overrideString(config.SMTP.Host, flag.Lookup("smtp-host").Value.String())
	if smtpPort.set {
		config.SMTP.Port = smtpPort.value
	}
	config.SMTP.User = overrideString(config.SMTP.User, flag.Lookup("smtp-user").Value.String())
	config.SMTP.Pass = overrideString(config.SMTP.Pass, flag.Lookup("smtp-pass").Value.String())
	config.SMTP.From = overrideString(config.SMTP.From, flag.Lookup("smtp-from").Value.String())
	config.SMTP.Subject = overrideString(config.SMTP.Subject, flag.Lookup("smtp-subject").Value.String())
	config.SMTP.To = overrideList(config.SMTP.To, flag.Lookup("smtp-to").Value.String())
	config.SMTP.Cc = overrideList(config.SMTP.Cc, flag.Lookup("smtp-cc").Value.String())
	config.SMTP.Bcc = overrideList(config.SMTP.Bcc, flag.Lookup("smtp-bcc").Value.String())
	if smtpTLS.set {
		config.SMTP.TLS = smtpTLS.value
	}

	showQuery := true
	if config.ShowQuery != nil {
		showQuery = *config.ShowQuery
	}
	if showQueryFlag.set {
		showQuery = showQueryFlag.value
	}

	if err := validateConfig(config, *mailTest, *dbTest); err != nil {
		fatal(err)
	}

	if *dbTest {
		if err := testDB(config.DB, *debug); err != nil {
			fatal(err)
		}
		fmt.Println("db connection OK")
		if !*mailTest {
			return
		}
	}
	if *mailTest {
		debugf(*debug, "mail test: building message")
		body := "Mail test OK."
		if err := sendMail(config.SMTP, body, "text/plain; charset=\"utf-8\"", nil, *debug); err != nil {
			fatal(err)
		}
		fmt.Println("mail send OK")
		return
	}

	columns, rows, err := runQuery(config.DB, config.SQL)
	if err != nil {
		fatal(err)
	}

	result, contentType, attachment, err := renderOutput(config.Output, columns, rows)
	if err != nil {
		fatal(err)
	}

	mailBody := buildMailBody(config.SQL, result, config.Output, contentType, showQuery)
	if err := sendMail(config.SMTP, mailBody, contentType, attachment, *debug); err != nil {
		fatal(err)
	}
}

func loadConfig(path string, required bool) (Config, error) {
	var config Config
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) && !required {
			return config, nil
		}
		return config, fmt.Errorf("config read failed: %w", err)
	}
	if info.IsDir() {
		return config, fmt.Errorf("config path is a directory: %s", path)
	}
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return config, fmt.Errorf("config decode failed: %w", err)
	}
	return config, nil
}

func validateConfig(config Config, mailTest bool, dbTest bool) error {
	if strings.TrimSpace(config.Output) != "" {
		if _, err := normalizeOutput(config.Output); err != nil {
			return err
		}
	}
	if !mailTest && !dbTest {
		if strings.TrimSpace(config.SQL) == "" {
			return errors.New("sql query is required (use -sql or config sql)")
		}
		if strings.TrimSpace(config.DB.Type) == "" {
			return errors.New("db.type is required")
		}
		if strings.TrimSpace(config.SMTP.Host) == "" {
			return errors.New("smtp.host is required")
		}
		if config.SMTP.Port == 0 {
			return errors.New("smtp.port is required")
		}
		if strings.TrimSpace(config.SMTP.From) == "" {
			return errors.New("smtp.from is required")
		}
		if len(config.SMTP.To) == 0 {
			return errors.New("smtp.to is required")
		}
		return nil
	}
	if dbTest {
		if strings.TrimSpace(config.DB.Type) == "" {
			return errors.New("db.type is required")
		}
	}
	if mailTest {
		if strings.TrimSpace(config.SMTP.Host) == "" {
			return errors.New("smtp.host is required")
		}
		if config.SMTP.Port == 0 {
			return errors.New("smtp.port is required")
		}
		if strings.TrimSpace(config.SMTP.From) == "" {
			return errors.New("smtp.from is required")
		}
		if len(config.SMTP.To) == 0 {
			return errors.New("smtp.to is required")
		}
	}
	return nil
}

func testDB(config DBConfig, debug bool) error {
	dsn, driver, err := buildDSN(config)
	if err != nil {
		return err
	}
	debugf(debug, "db test: open driver=%s", driver)
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return fmt.Errorf("db open failed: %w", err)
	}
	defer db.Close()
	debugf(debug, "db test: ping")
	if err := db.Ping(); err != nil {
		return fmt.Errorf("db ping failed: %w", err)
	}
	return nil
}

func runQuery(config DBConfig, query string) ([]string, [][]string, error) {
	dsn, driver, err := buildDSN(config)
	if err != nil {
		return nil, nil, err
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("db open failed: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("columns read failed: %w", err)
	}
	var rowData [][]string
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, nil, fmt.Errorf("row scan failed: %w", err)
		}
		row := make([]string, len(columns))
		for i, value := range values {
			row[i] = formatValue(value)
		}
		rowData = append(rowData, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("row iterate failed: %w", err)
	}
	return columns, rowData, nil
}

func buildDSN(config DBConfig) (string, string, error) {
	if strings.TrimSpace(config.DSN) != "" {
		switch strings.ToLower(config.Type) {
		case "mysql", "mariadb":
			return config.DSN, "mysql", nil
		case "postgres", "postgresql", "pgx":
			return config.DSN, "pgx", nil
		case "mssql", "sqlserver":
			return config.DSN, "sqlserver", nil
		case "clickhouse":
			return config.DSN, "clickhouse", nil
		default:
			return "", "", fmt.Errorf("unsupported db.type: %s", config.Type)
		}
	}

	switch strings.ToLower(config.Type) {
	case "mysql", "mariadb":
		port := config.Port
		if port == 0 {
			port = 3306
		}
		if strings.TrimSpace(config.Host) == "" {
			return "", "", errors.New("db.host is required")
		}
		if strings.TrimSpace(config.User) == "" {
			return "", "", errors.New("db.user is required")
		}
		if strings.TrimSpace(config.Name) == "" {
			return "", "", errors.New("db.name is required")
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", config.User, config.Pass, config.Host, port, config.Name)
		return dsn, "mysql", nil
	case "postgres", "postgresql", "pgx":
		port := config.Port
		if port == 0 {
			port = 5432
		}
		if strings.TrimSpace(config.Host) == "" {
			return "", "", errors.New("db.host is required")
		}
		if strings.TrimSpace(config.User) == "" {
			return "", "", errors.New("db.user is required")
		}
		if strings.TrimSpace(config.Name) == "" {
			return "", "", errors.New("db.name is required")
		}
		sslMode := config.SSLMode
		if strings.TrimSpace(sslMode) == "" {
			sslMode = "disable"
		}
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", config.User, config.Pass, config.Host, port, config.Name, sslMode)
		return dsn, "pgx", nil
	case "mssql", "sqlserver":
		port := config.Port
		if port == 0 {
			port = 1433
		}
		if strings.TrimSpace(config.Host) == "" {
			return "", "", errors.New("db.host is required")
		}
		if strings.TrimSpace(config.User) == "" {
			return "", "", errors.New("db.user is required")
		}
		if strings.TrimSpace(config.Name) == "" {
			return "", "", errors.New("db.name is required")
		}
		dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s", config.User, config.Pass, config.Host, port, config.Name)
		return dsn, "sqlserver", nil
	case "clickhouse":
		port := config.Port
		if port == 0 {
			port = 9000
		}
		if strings.TrimSpace(config.Host) == "" {
			return "", "", errors.New("db.host is required")
		}
		if strings.TrimSpace(config.User) == "" {
			return "", "", errors.New("db.user is required")
		}
		if strings.TrimSpace(config.Name) == "" {
			return "", "", errors.New("db.name is required")
		}
		sslMode := "false"
		if strings.EqualFold(strings.TrimSpace(config.SSLMode), "require") {
			sslMode = "true"
		}
		dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?secure=%s", config.User, config.Pass, config.Host, port, config.Name, sslMode)
		return dsn, "clickhouse", nil
	default:
		return "", "", fmt.Errorf("unsupported db.type: %s", config.Type)
	}
}

func formatValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(value)
	}
}

func buildMailBody(query string, result string, format string, contentType string, showQuery bool) string {
	label := strings.ToUpper(format)
	if strings.TrimSpace(label) == "" {
		label = "CSV"
	}
	if strings.HasPrefix(contentType, "text/html") {
		return buildHTMLBody(query, result, label, showQuery)
	}
	if showQuery {
		return fmt.Sprintf("SQL Query:\n%s\n\nResult (%s):\n%s", query, label, result)
	}
	return fmt.Sprintf("Result (%s):\n%s", label, result)
}

func sendMail(config SMTPConfig, body string, contentType string, attachment *Attachment, debug bool) error {
	if debug {
		return sendMailDebug(config, body, contentType, attachment, debug)
	}
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	debugf(debug, "smtp: server=%s", addr)
	message := buildMessage(config, body, contentType, attachment)

	recipients := append([]string{}, config.SMTPRecipients()...)
	if len(recipients) == 0 {
		return errors.New("no recipients specified")
	}
	debugf(debug, "smtp: recipients=%d", len(recipients))

	if config.TLS {
		debugf(debug, "smtp: dialing with STARTTLS")
		client, err := smtp.Dial(addr)
		if err != nil {
			return fmt.Errorf("smtp dial failed: %w", err)
		}
		defer client.Close()

		if ok, _ := client.Extension("STARTTLS"); ok {
			debugf(debug, "smtp: starttls")
			tlsConfig := &tls.Config{ServerName: config.Host}
			if err := client.StartTLS(tlsConfig); err != nil {
				return fmt.Errorf("starttls failed: %w", err)
			}
		} else {
			return errors.New("smtp server does not support STARTTLS")
		}

		if err := smtpAuth(config, client, debug); err != nil {
			return err
		}
		debugf(debug, "smtp: mail from=%s", config.From)
		if err := client.Mail(config.From); err != nil {
			return fmt.Errorf("smtp from failed: %w", err)
		}
		for _, recipient := range recipients {
			debugf(debug, "smtp: rcpt=%s", recipient)
			if err := client.Rcpt(recipient); err != nil {
				return fmt.Errorf("smtp rcpt failed: %w", err)
			}
		}
		debugf(debug, "smtp: sending data")
		writer, err := client.Data()
		if err != nil {
			return fmt.Errorf("smtp data failed: %w", err)
		}
		if _, err := writer.Write(message); err != nil {
			return fmt.Errorf("smtp write failed: %w", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("smtp close failed: %w", err)
		}
		debugf(debug, "smtp: quit")
		return client.Quit()
	}

	var auth smtp.Auth
	if strings.TrimSpace(config.User) != "" {
		debugf(debug, "smtp: using auth")
		auth = smtp.PlainAuth("", config.User, config.Pass, config.Host)
	}
	debugf(debug, "smtp: sendmail")
	if err := smtp.SendMail(addr, auth, config.From, recipients, message); err != nil {
		return fmt.Errorf("smtp send failed: %w", err)
	}
	return nil
}

func sendMailDebug(config SMTPConfig, body string, contentType string, attachment *Attachment, debug bool) error {
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	message := buildMessage(config, body, contentType, attachment)
	recipients := append([]string{}, config.SMTPRecipients()...)
	if len(recipients) == 0 {
		return errors.New("no recipients specified")
	}

	debugf(debug, "smtp: dial %s", addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("smtp dial failed: %w", err)
	}
	text := textproto.NewConn(conn)
	defer func() {
		_ = text.Close()
		_ = conn.Close()
	}()

	if err := expectSMTPResponse(text, debug, []int{220}); err != nil {
		return err
	}

	hostname := smtpHostname()
	capabilities, err := smtpEhlo(text, debug, hostname)
	if err != nil {
		return err
	}

	if config.TLS {
		if !capabilities["STARTTLS"] {
			return errors.New("smtp server does not support STARTTLS")
		}
		debugf(debug, "C: STARTTLS")
		if _, err := smtpCmdExpect(text, debug, "STARTTLS", []int{220}); err != nil {
			return err
		}
		tlsConn := tls.Client(conn, &tls.Config{ServerName: config.Host})
		if err := tlsConn.Handshake(); err != nil {
			return fmt.Errorf("starttls handshake failed: %w", err)
		}
		text = textproto.NewConn(tlsConn)
		conn = tlsConn
		capabilities, err = smtpEhlo(text, debug, hostname)
		if err != nil {
			return err
		}
	}

	if strings.TrimSpace(config.User) != "" {
		if !capabilities["AUTH"] {
			return errors.New("smtp server does not support AUTH")
		}
		authPayload := "\x00" + config.User + "\x00" + config.Pass
		encoded := base64.StdEncoding.EncodeToString([]byte(authPayload))
		debugf(debug, "C: AUTH PLAIN (redacted)")
		if _, err := smtpCmdExpect(text, debug, "AUTH PLAIN "+encoded, []int{235}); err != nil {
			return err
		}
	}

	debugf(debug, "C: MAIL FROM:<%s>", config.From)
	if _, err := smtpCmdExpect(text, debug, "MAIL FROM:<"+config.From+">", []int{250}); err != nil {
		return err
	}
	for _, recipient := range recipients {
		debugf(debug, "C: RCPT TO:<%s>", recipient)
		if _, err := smtpCmdExpect(text, debug, "RCPT TO:<"+recipient+">", []int{250, 251}); err != nil {
			return err
		}
	}
	debugf(debug, "C: DATA")
	if _, err := smtpCmdExpect(text, debug, "DATA", []int{354}); err != nil {
		return err
	}
	writer := text.DotWriter()
	if _, err := writer.Write(message); err != nil {
		return fmt.Errorf("smtp write failed: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("smtp close failed: %w", err)
	}
	if err := expectSMTPResponse(text, debug, []int{250}); err != nil {
		return err
	}
	debugf(debug, "C: QUIT")
	if _, err := smtpCmdExpect(text, debug, "QUIT", []int{221}); err != nil {
		return err
	}
	return nil
}

func smtpEhlo(conn *textproto.Conn, debug bool, hostname string) (map[string]bool, error) {
	debugf(debug, "C: EHLO %s", hostname)
	msg, err := smtpCmdExpect(conn, debug, "EHLO "+hostname, []int{250})
	if err != nil {
		return nil, err
	}
	capabilities := map[string]bool{}
	for _, line := range strings.Split(msg, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		capabilities[strings.ToUpper(fields[0])] = true
	}
	return capabilities, nil
}

func smtpCmdExpect(conn *textproto.Conn, debug bool, cmd string, expected []int) (string, error) {
	if err := conn.PrintfLine(cmd); err != nil {
		return "", fmt.Errorf("smtp write failed: %w", err)
	}
	code, msg, err := readSMTPResponse(conn)
	if err != nil {
		return "", err
	}
	debugf(debug, "S: %d %s", code, msg)
	for _, allowed := range expected {
		if code == allowed {
			return msg, nil
		}
	}
	return "", fmt.Errorf("smtp unexpected response: %d %s", code, msg)
}

func expectSMTPResponse(conn *textproto.Conn, debug bool, expected []int) error {
	code, msg, err := readSMTPResponse(conn)
	if err != nil {
		return err
	}
	debugf(debug, "S: %d %s", code, msg)
	for _, allowed := range expected {
		if code == allowed {
			return nil
		}
	}
	return fmt.Errorf("smtp unexpected response: %d %s", code, msg)
}

func readSMTPResponse(conn *textproto.Conn) (int, string, error) {
	line, err := conn.ReadLine()
	if err != nil {
		return 0, "", fmt.Errorf("smtp read failed: %w", err)
	}
	if len(line) < 3 {
		return 0, "", fmt.Errorf("smtp invalid response: %s", line)
	}
	code, err := strconv.Atoi(line[:3])
	if err != nil {
		return 0, "", fmt.Errorf("smtp invalid response code: %s", line)
	}
	message := ""
	if len(line) > 4 {
		message = line[4:]
	}
	if len(line) > 3 && line[3] == '-' {
		for {
			more, err := conn.ReadLine()
			if err != nil {
				return 0, "", fmt.Errorf("smtp read failed: %w", err)
			}
			if len(more) > 4 {
				message += "\n" + more[4:]
			}
			if strings.HasPrefix(more, fmt.Sprintf("%03d ", code)) {
				break
			}
		}
	}
	return code, message, nil
}

func smtpHostname() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "localhost"
	}
	return hostname
}

func smtpAuth(config SMTPConfig, client *smtp.Client, debug bool) error {
	if strings.TrimSpace(config.User) == "" {
		return nil
	}
	if ok, _ := client.Extension("AUTH"); !ok {
		return errors.New("smtp server does not support AUTH")
	}
	debugf(debug, "smtp: auth")
	auth := smtp.PlainAuth("", config.User, config.Pass, config.Host)
	if err := client.Auth(auth); err != nil {
		return fmt.Errorf("smtp auth failed: %w", err)
	}
	return nil
}

func buildMessage(config SMTPConfig, body string, contentType string, attachment *Attachment) []byte {
	resolvedContentType := contentType
	if strings.TrimSpace(resolvedContentType) == "" {
		resolvedContentType = "text/plain; charset=\"utf-8\""
	}
	if attachment != nil {
		return buildMultipartMessage(config, body, resolvedContentType, attachment)
	}
	headers := map[string]string{
		"From":         config.From,
		"To":           strings.Join(config.To, ", "),
		"Subject":      config.Subject,
		"MIME-Version": "1.0",
		"Content-Type": resolvedContentType,
	}
	if len(config.Cc) > 0 {
		headers["Cc"] = strings.Join(config.Cc, ", ")
	}
	var builder strings.Builder
	for key, value := range headers {
		builder.WriteString(key)
		builder.WriteString(": ")
		builder.WriteString(value)
		builder.WriteString("\r\n")
	}
	builder.WriteString("\r\n")
	builder.WriteString(body)
	return []byte(builder.String())
}

func (config SMTPConfig) SMTPRecipients() []string {
	recipients := append([]string{}, config.To...)
	recipients = append(recipients, config.Cc...)
	recipients = append(recipients, config.Bcc...)
	return recipients
}

func overrideString(current string, override string) string {
	if strings.TrimSpace(override) == "" {
		return current
	}
	return override
}

func normalizeOutput(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "csv", nil
	}
	format := strings.ToLower(strings.TrimSpace(value))
	switch format {
	case "csv":
		return "csv", nil
	case "table":
		return "table", nil
	case "text":
		return "text", nil
	default:
		return "", fmt.Errorf("unsupported output format: %s", value)
	}
}

func renderOutput(format string, columns []string, rows [][]string) (string, string, *Attachment, error) {
	normalized, err := normalizeOutput(format)
	if err != nil {
		return "", "", nil, err
	}
	if len(rows) == 0 {
		return "No rows returned.", "text/plain; charset=\"utf-8\"", nil, nil
	}
	if normalized == "table" {
		return renderTableHTML(columns, rows), "text/html; charset=\"utf-8\"", nil, nil
	}
	if normalized == "text" {
		return renderText(columns, rows), "text/plain; charset=\"utf-8\"", nil, nil
	}
	result, err := renderCSV(columns, rows)
	if err != nil {
		return "", "", nil, err
	}
	return "CSV result attached as result.csv.", "text/plain; charset=\"utf-8\"", &Attachment{
		Filename:    "result.csv",
		ContentType: "text/csv; charset=\"utf-8\"",
		Data:        []byte(result),
	}, nil
}

func renderCSV(columns []string, rows [][]string) (string, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	if err := writer.Write(columns); err != nil {
		return "", fmt.Errorf("csv header write failed: %w", err)
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("csv row write failed: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", fmt.Errorf("csv flush failed: %w", err)
	}
	return buffer.String(), nil
}

func renderText(columns []string, rows [][]string) string {
	var builder strings.Builder
	builder.WriteString(strings.Join(columns, "\t"))
	for _, row := range rows {
		builder.WriteString("\n")
		builder.WriteString(strings.Join(sanitizeRow(row), "\t"))
	}
	return builder.String()
}

func renderTableHTML(columns []string, rows [][]string) string {
	var builder strings.Builder
	builder.WriteString("<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\" style=\"border-collapse:collapse;\">\n")
	builder.WriteString("<thead><tr>")
	for _, column := range columns {
		builder.WriteString("<th>")
		builder.WriteString(html.EscapeString(column))
		builder.WriteString("</th>")
	}
	builder.WriteString("</tr></thead>\n")
	builder.WriteString("<tbody>\n")
	for _, row := range rows {
		builder.WriteString("<tr>")
		for _, cell := range row {
			builder.WriteString("<td>")
			builder.WriteString(html.EscapeString(sanitizeCell(cell)))
			builder.WriteString("</td>")
		}
		builder.WriteString("</tr>\n")
	}
	builder.WriteString("</tbody></table>")
	return builder.String()
}

func sanitizeCell(value string) string {
	value = strings.ReplaceAll(value, "\r\n", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	return value
}

func sanitizeRow(row []string) []string {
	clean := make([]string, len(row))
	for i, cell := range row {
		clean[i] = sanitizeCell(cell)
	}
	return clean
}

func buildHTMLBody(query string, result string, label string, showQuery bool) string {
	if showQuery {
		return fmt.Sprintf(
			"<html><body><p><strong>SQL Query:</strong></p><pre>%s</pre><p><strong>Result (%s):</strong></p>%s</body></html>",
			html.EscapeString(query),
			html.EscapeString(label),
			result,
		)
	}
	return fmt.Sprintf(
		"<html><body><p><strong>Result (%s):</strong></p>%s</body></html>",
		html.EscapeString(label),
		result,
	)
}

type Attachment struct {
	Filename    string
	ContentType string
	Data        []byte
}

func buildMultipartMessage(config SMTPConfig, body string, contentType string, attachment *Attachment) []byte {
	boundary := fmt.Sprintf("notifysql-%d", time.Now().UnixNano())
	headers := map[string]string{
		"From":         config.From,
		"To":           strings.Join(config.To, ", "),
		"Subject":      config.Subject,
		"MIME-Version": "1.0",
		"Content-Type": fmt.Sprintf("multipart/mixed; boundary=\"%s\"", boundary),
	}
	if len(config.Cc) > 0 {
		headers["Cc"] = strings.Join(config.Cc, ", ")
	}
	var builder strings.Builder
	for key, value := range headers {
		builder.WriteString(key)
		builder.WriteString(": ")
		builder.WriteString(value)
		builder.WriteString("\r\n")
	}
	builder.WriteString("\r\n")
	builder.WriteString("--" + boundary + "\r\n")
	builder.WriteString("Content-Type: " + contentType + "\r\n")
	builder.WriteString("Content-Transfer-Encoding: 7bit\r\n\r\n")
	builder.WriteString(body)
	builder.WriteString("\r\n")

	encoded := base64.StdEncoding.EncodeToString(attachment.Data)
	builder.WriteString("--" + boundary + "\r\n")
	builder.WriteString("Content-Type: " + attachment.ContentType + "\r\n")
	builder.WriteString("Content-Disposition: attachment; filename=\"" + attachment.Filename + "\"\r\n")
	builder.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	builder.WriteString(wrapBase64(encoded))
	builder.WriteString("\r\n--" + boundary + "--\r\n")
	return []byte(builder.String())
}

func wrapBase64(value string) string {
	if len(value) <= 76 {
		return value
	}
	var builder strings.Builder
	for len(value) > 76 {
		builder.WriteString(value[:76])
		builder.WriteString("\r\n")
		value = value[76:]
	}
	if len(value) > 0 {
		builder.WriteString(value)
	}
	return builder.String()
}

func overrideList(current []string, override string) []string {
	if strings.TrimSpace(override) == "" {
		return current
	}
	return splitList(override)
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	var items []string
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

func flagPassed(name string) bool {
	passed := false
	flag.Visit(func(flag *flag.Flag) {
		if flag.Name == name {
			passed = true
		}
	})
	return passed
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func debugf(enabled bool, format string, args ...interface{}) {
	if !enabled {
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "[debug] "+format+"\n", args...)
}
