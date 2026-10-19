package mysqlcdc

import (
	"math"
	"testing"

	"example.com/etl/internal/model"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/shopspring/decimal"
)

func TestLosslessRow(t *testing.T) {
	cols := []model.Column{{Name: "id", Type: "bigint unsigned", Required: true}, {Name: "amount", Type: "decimal(18,2)", Required: true}, {Name: "payload", Type: "blob"}, {Name: "nullable", Type: "varchar(10)"}, {Name: "timestamp", Type: "timestamp(6)"}}
	out, err := convertRow([]any{int64(-1), decimal.RequireFromString("1234567890123456.78"), []byte{0, 255}, nil, "2026-10-19 01:02:03.123456"}, cols)
	if err != nil {
		t.Fatal(err)
	}
	if out["id"] != "18446744073709551615" || out["amount"] != "1234567890123456.78" || out["payload"] != "AP8=" || out["nullable"] != nil {
		t.Fatalf("lossy row: %#v", out)
	}
	for _, tt := range []struct {
		typ  string
		in   any
		want string
	}{{"bigint", int64(math.MinInt64), "-9223372036854775808"}, {"int unsigned", int32(-1), "4294967295"}, {"mediumint unsigned", int32(-1), "16777215"}, {"tinyint unsigned", int8(-1), "255"}} {
		row, e := convertRow([]any{tt.in}, []model.Column{{Name: "v", Type: tt.typ}})
		if e != nil || row["v"] != tt.want {
			t.Fatalf("%s: %v %v", tt.typ, row, e)
		}
	}
}

func TestRejectCorruptRows(t *testing.T) {
	cols := []model.Column{{Name: "v", Type: "varchar(10)", Required: true}}
	for _, row := range [][]any{{}, {nil}, {string([]byte{255})}} {
		if _, err := convertRow(row, cols); err == nil {
			t.Fatalf("accepted invalid row %#v", row)
		}
	}
}

func TestSchemaWireValidation(t *testing.T) {
	cols := []model.Column{{Name: "id", Type: "bigint unsigned"}, {Name: "label", Type: "varchar(20)"}}
	table := &replication.TableMapEvent{ColumnCount: 2, ColumnType: []byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_VARCHAR}}
	if err := validateTable(table, cols); err != nil {
		t.Fatal(err)
	}
	table.ColumnType[0] = mysql.MYSQL_TYPE_LONG
	if err := validateTable(table, cols); err == nil {
		t.Fatal("accepted changed type")
	}
	table.ColumnCount = 3
	if err := validateTable(table, cols); err == nil {
		t.Fatal("accepted changed count")
	}
}

func TestIdentityStableAndSeparated(t *testing.T) {
	first := identity("source", "file", "10")
	second := identity("source", "file", "10")
	if first != second {
		t.Fatal("unstable")
	}
	if identity("ab", "c") == identity("a", "bc") {
		t.Fatal("ambiguous encoding")
	}
	if identity("s", "f", "10") == identity("s", "f", "11") {
		t.Fatal("positions collide")
	}
}

func TestUnsupportedTypesAndTables(t *testing.T) {
	for _, typ := range []string{"enum('a','b')", "set('a')", "float", "double", "bit(8)", "geometry"} {
		if supported(typ) {
			t.Fatalf("accepted %s", typ)
		}
	}
	for _, name := range []string{"orders", "shop.*", "a.b.c", "a.`b`"} {
		if _, _, err := splitTable(name); err == nil {
			t.Fatalf("accepted %s", name)
		}
	}
}
