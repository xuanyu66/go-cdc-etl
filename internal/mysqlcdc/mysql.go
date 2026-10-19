// Package mysqlcdc implements the fixed-table, ROW/FULL MySQL incremental source.
package mysqlcdc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"example.com/etl/internal/model"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type Connection struct {
	Addr, User, Password string
	ServerID             uint32
}
type ProbeResult struct {
	Position   model.Position            `json:"position"`
	ServerUUID string                    `json:"server_uuid"`
	Tables     map[string][]model.Column `json:"tables"`
}
type Config struct {
	Connection      Connection
	Tables          []string
	Start           model.Position
	SourceID        string
	ExpectedSchemas map[string][]model.Column
}

func literal(s string) string {
	return "CONVERT(0x" + hex.EncodeToString([]byte(s)) + " USING utf8mb4)"
}
func splitTable(s string) (string, string, error) {
	p := strings.Split(s, ".")
	if len(p) != 2 || p[0] == "" || p[1] == "" || strings.ContainsAny(s, "*`\x00") {
		return "", "", fmt.Errorf("expected exact database.table, got %q", s)
	}
	return p[0], p[1], nil
}
func baseType(s string) string {
	return strings.Fields(strings.SplitN(strings.ToLower(s), "(", 2)[0])[0]
}
func supported(t string) bool {
	switch baseType(t) {
	case "tinyint", "smallint", "mediumint", "int", "bigint", "decimal", "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "json", "date", "datetime", "timestamp", "time", "year":
		return true
	}
	return false
}

// Probe reads current metadata; callers must arrange a DDL-free setup window.
func Probe(ctx context.Context, c Connection, tables []string) (ProbeResult, error) {
	out := ProbeResult{Tables: map[string][]model.Column{}}
	if len(tables) == 0 {
		return out, fmt.Errorf("at least one exact table is required")
	}
	conn, err := client.ConnectWithContext(ctx, c.Addr, c.User, c.Password, "", 5*time.Second, func(conn *client.Conn) error {
		conn.ReadTimeout = 5 * time.Second
		conn.WriteTimeout = 5 * time.Second
		return nil
	})
	if err != nil {
		return out, err
	}
	defer conn.Close()
	r, err := conn.Execute("SELECT @@global.log_bin, @@global.binlog_format, @@global.binlog_row_image, @@server_uuid")
	if err != nil {
		return out, err
	}
	enabled, _ := r.GetString(0, 0)
	format, _ := r.GetString(0, 1)
	image, _ := r.GetString(0, 2)
	out.ServerUUID, _ = r.GetString(0, 3)
	if enabled != "1" || format != "ROW" || image != "FULL" {
		return out, fmt.Errorf("MySQL requires log_bin=ON, binlog_format=ROW, binlog_row_image=FULL")
	}
	for _, table := range tables {
		db, name, e := splitTable(table)
		if e != nil {
			return out, e
		}
		if _, ok := out.Tables[table]; ok {
			return out, fmt.Errorf("duplicate table %s", table)
		}
		pred := "TABLE_SCHEMA=" + literal(db) + " AND TABLE_NAME=" + literal(name)
		r, e = conn.Execute("SELECT ENGINE,TABLE_TYPE FROM information_schema.TABLES WHERE " + pred)
		if e != nil {
			return out, e
		}
		if r.RowNumber() != 1 {
			return out, fmt.Errorf("table %s does not exist", table)
		}
		engine, _ := r.GetString(0, 0)
		kind, _ := r.GetString(0, 1)
		if engine != "InnoDB" || kind != "BASE TABLE" {
			return out, fmt.Errorf("table %s must be an InnoDB base table", table)
		}
		r, e = conn.Execute("SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COALESCE(CHARACTER_SET_NAME,''),EXTRA FROM information_schema.COLUMNS WHERE " + pred + " ORDER BY ORDINAL_POSITION")
		if e != nil {
			return out, e
		}
		cols := []model.Column{}
		for i := 0; i < r.RowNumber(); i++ {
			n, _ := r.GetString(i, 0)
			t, _ := r.GetString(i, 1)
			nullable, _ := r.GetString(i, 2)
			charset, _ := r.GetString(i, 3)
			extra, _ := r.GetString(i, 4)
			if !supported(t) || strings.Contains(strings.ToUpper(extra), "GENERATED") || strings.Contains(strings.ToUpper(extra), "INVISIBLE") {
				return out, fmt.Errorf("unsupported source column %s.%s (%s, %s)", table, n, t, extra)
			}
			if charset != "" && charset != "binary" && charset != "utf8mb4" && charset != "utf8mb3" && charset != "utf8" && charset != "ascii" {
				return out, fmt.Errorf("unsupported charset %s on %s.%s", charset, table, n)
			}
			cols = append(cols, model.Column{Name: n, Type: strings.ToLower(t), Required: nullable == "NO"})
		}
		if len(cols) == 0 {
			return out, fmt.Errorf("table %s has no columns", table)
		}
		out.Tables[table] = cols
	}
	r, err = conn.Execute("SHOW BINARY LOG STATUS")
	if err != nil {
		r, err = conn.Execute("SHOW MASTER STATUS")
	}
	if err != nil {
		return out, err
	}
	if r.RowNumber() != 1 {
		return out, fmt.Errorf("no binary log position")
	}
	out.Position.File, _ = r.GetString(0, 0)
	pos, e := r.GetUint(0, 1)
	if e != nil || pos > uint64(^uint32(0)) {
		return out, fmt.Errorf("invalid binlog position")
	}
	out.Position.Offset = uint32(pos)
	return out, nil
}
func identity(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Read delivers only committed transactions. Any error leaves the caller's last
// acknowledged checkpoint authoritative; the syncer does not retry internally.
func Read(ctx context.Context, c Config, deliver func(model.Transaction) error) error {
	if c.Start.File == "" || c.Start.Offset < 4 || c.SourceID == "" || c.Connection.ServerID == 0 {
		return fmt.Errorf("source ID, server ID and file position >= 4 are required")
	}
	probe, err := Probe(ctx, c.Connection, c.Tables)
	if err != nil {
		return err
	}
	if probe.ServerUUID != c.SourceID {
		return fmt.Errorf("source identity changed; refusing file-position recovery on a different MySQL server")
	}
	for _, t := range c.Tables {
		if !reflect.DeepEqual(probe.Tables[t], c.ExpectedSchemas[t]) {
			return fmt.Errorf("source schema differs from pinned schema for %s; DDL requires a new validated pipeline", t)
		}
	}
	host, portText, err := net.SplitHostPort(c.Connection.Addr)
	if err != nil {
		return err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return err
	}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: c.Connection.ServerID, Flavor: "mysql", Host: host, Port: uint16(port), User: c.Connection.User, Password: c.Connection.Password, Charset: "utf8mb4", UseDecimal: true, TimestampStringLocation: time.UTC, DisableRetrySync: true, VerifyChecksum: true, ReadTimeout: 30 * time.Second, HeartbeatPeriod: 5 * time.Second})
	defer syncer.Close()
	stream, err := syncer.StartSync(mysql.Position{Name: c.Start.File, Pos: c.Start.Offset})
	if err != nil {
		return err
	}
	file := c.Start.File
	var events []model.Event
	var txnStart string
	var rowIndex uint64
	var bytes int
	reset := func() { events = nil; txnStart = ""; rowIndex = 0; bytes = 0 }
	commit := func(pos uint32) error {
		id := identity(c.SourceID, file, strconv.FormatUint(uint64(pos), 10))
		for i := range events {
			events[i].TransactionID = id
		}
		err := deliver(model.Transaction{ID: id, ResumeAfter: model.Position{File: file, Offset: pos}, Events: events})
		reset()
		return err
	}
	for {
		ev, e := stream.GetEvent(ctx)
		if e != nil {
			return e
		}
		if ev.Header.EventType == replication.TRANSACTION_PAYLOAD_EVENT || ev.Header.EventType == replication.INCIDENT_EVENT {
			return fmt.Errorf("unsupported unsafe binlog event %s at %s:%d", ev.Header.EventType, file, ev.Header.LogPos)
		}
		switch v := ev.Event.(type) {
		case *replication.RotateEvent:
			if txnStart != "" || len(events) > 0 {
				return fmt.Errorf("unexpected binlog rotation within transaction")
			}
			file = string(v.NextLogName)
		case *replication.QueryEvent:
			q := strings.ToUpper(strings.TrimSpace(string(v.Query)))
			switch q {
			case "BEGIN":
				if txnStart != "" {
					return fmt.Errorf("nested transaction")
				}
				txnStart = fmt.Sprintf("%s:%d", file, ev.Header.LogPos)
			case "COMMIT":
				if e = commit(ev.Header.LogPos); e != nil {
					return e
				}
			case "ROLLBACK":
				reset()
				if e = commit(ev.Header.LogPos); e != nil {
					return e
				}
			default:
				if txnStart == "" && unrelatedDDL(string(v.Query), string(v.Schema), c.Tables) {
					// Unsubscribed DDL is a complete zero-output source operation.
					// Persist its position through the regular sink/checkpoint path,
					// so restart never gets stuck replaying the same unrelated DDL.
					if e = commit(ev.Header.LogPos); e != nil {
						return e
					}
					continue
				}
				return fmt.Errorf("controlled DDL/statement stop at %s:%d: statement affects subscribed schema or cannot be safely classified", file, ev.Header.LogPos)
			}
		case *replication.XIDEvent:
			if e = commit(ev.Header.LogPos); e != nil {
				return e
			}
		case *replication.RowsEvent:
			table := string(v.Table.Schema) + "." + string(v.Table.Table)
			cols, ok := c.ExpectedSchemas[table]
			if !ok {
				continue
			}
			if txnStart == "" {
				return fmt.Errorf("rows outside a BEGIN transaction")
			}
			if e = validateTable(v.Table, cols); e != nil {
				return fmt.Errorf("%s: %w", table, e)
			}
			for _, skipped := range v.SkippedColumns {
				if len(skipped) > 0 {
					return fmt.Errorf("%s: incomplete row image; FULL required", table)
				}
			}
			op := ""
			step := 1
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				op = "insert"
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				op = "update"
				step = 2
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				op = "delete"
			default:
				return fmt.Errorf("unsupported row event %s", ev.Header.EventType)
			}
			if len(v.Rows)%step != 0 {
				return fmt.Errorf("invalid row image count")
			}
			for i := 0; i < len(v.Rows); i += step {
				a, e := convertRow(v.Rows[i], cols)
				if e != nil {
					return e
				}
				var before, after map[string]any
				switch op {
				case "insert":
					after = a
				case "delete":
					before = a
				case "update":
					before = a
					after, e = convertRow(v.Rows[i+1], cols)
					if e != nil {
						return e
					}
				}
				event := model.Event{ID: identity(c.SourceID, file, strconv.FormatUint(uint64(ev.Header.LogPos), 10), strconv.Itoa(i/step)), SourceID: c.SourceID, RowIndex: rowIndex, Database: string(v.Table.Schema), Table: string(v.Table.Table), Operation: op, Before: before, After: after, Columns: cols, SourcePosition: model.Position{File: file, Offset: ev.Header.LogPos}}
				b, e := json.Marshal(event)
				if e != nil {
					return e
				}
				bytes += len(b)
				if len(events) >= 10000 || bytes > 16*1024*1024 {
					return fmt.Errorf("transaction exceeds MVP limit (10000 subscribed events / 16 MiB encoded events)")
				}
				events = append(events, event)
				rowIndex++
			}
		}
	}
}
func validateTable(t *replication.TableMapEvent, cols []model.Column) error {
	if int(t.ColumnCount) != len(cols) || len(t.ColumnType) != len(cols) {
		return fmt.Errorf("column count changed")
	}
	for i, c := range cols {
		var allowed []byte
		switch baseType(c.Type) {
		case "tinyint":
			allowed = []byte{mysql.MYSQL_TYPE_TINY}
		case "smallint":
			allowed = []byte{mysql.MYSQL_TYPE_SHORT}
		case "mediumint":
			allowed = []byte{mysql.MYSQL_TYPE_INT24}
		case "int":
			allowed = []byte{mysql.MYSQL_TYPE_LONG}
		case "bigint":
			allowed = []byte{mysql.MYSQL_TYPE_LONGLONG}
		case "decimal":
			allowed = []byte{mysql.MYSQL_TYPE_NEWDECIMAL}
		case "varchar", "varbinary":
			allowed = []byte{mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING}
		case "char", "binary":
			allowed = []byte{mysql.MYSQL_TYPE_STRING}
		case "tinytext", "text", "mediumtext", "longtext", "tinyblob", "blob", "mediumblob", "longblob":
			allowed = []byte{mysql.MYSQL_TYPE_BLOB}
		case "json":
			allowed = []byte{mysql.MYSQL_TYPE_JSON}
		case "date":
			allowed = []byte{mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE}
		case "datetime":
			allowed = []byte{mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2}
		case "timestamp":
			allowed = []byte{mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2}
		case "time":
			allowed = []byte{mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2}
		case "year":
			allowed = []byte{mysql.MYSQL_TYPE_YEAR}
		}
		found := false
		for _, a := range allowed {
			found = found || a == t.ColumnType[i]
		}
		if !found {
			return fmt.Errorf("column %s wire type changed", c.Name)
		}
		if len(t.ColumnName) > i && string(t.ColumnName[i]) != c.Name {
			return fmt.Errorf("column name changed")
		}
	}
	return nil
}
func convertRow(row []any, cols []model.Column) (map[string]any, error) {
	if len(row) != len(cols) {
		return nil, fmt.Errorf("incomplete row")
	}
	out := map[string]any{}
	for i, c := range cols {
		v := row[i]
		if v == nil {
			if c.Required {
				return nil, fmt.Errorf("NULL in required source column %s", c.Name)
			}
			out[c.Name] = nil
			continue
		}
		t := baseType(c.Type)
		switch t {
		case "tinyint", "smallint", "mediumint", "int", "bigint", "year":
			value := reflect.ValueOf(v)
			if value.Kind() >= reflect.Int && value.Kind() <= reflect.Int64 {
				n := value.Int()
				if strings.Contains(c.Type, "unsigned") {
					bits := 64
					switch t {
					case "tinyint":
						bits = 8
					case "smallint":
						bits = 16
					case "mediumint":
						bits = 24
					case "int":
						bits = 32
					}
					u := uint64(n)
					if bits < 64 {
						u &= (uint64(1) << bits) - 1
					}
					out[c.Name] = strconv.FormatUint(u, 10)
				} else {
					out[c.Name] = strconv.FormatInt(n, 10)
				}
			} else if value.Kind() >= reflect.Uint && value.Kind() <= reflect.Uint64 {
				out[c.Name] = strconv.FormatUint(value.Uint(), 10)
			} else {
				return nil, fmt.Errorf("unexpected integer value for %s", c.Name)
			}
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			switch x := v.(type) {
			case []byte:
				out[c.Name] = base64.StdEncoding.EncodeToString(x)
			case string:
				out[c.Name] = base64.StdEncoding.EncodeToString([]byte(x))
			default:
				return nil, fmt.Errorf("unexpected binary value")
			}
		default:
			var s string
			switch x := v.(type) {
			case []byte:
				s = string(x)
			case string:
				s = x
			case fmt.Stringer:
				s = x.String()
			default:
				return nil, fmt.Errorf("unexpected value type %T for %s", v, c.Name)
			}
			if !utf8.ValidString(s) {
				return nil, fmt.Errorf("invalid UTF-8 in %s", c.Name)
			}
			out[c.Name] = s
		}
	}
	return out, nil
}
