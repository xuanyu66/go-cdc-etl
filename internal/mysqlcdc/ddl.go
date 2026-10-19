package mysqlcdc

import (
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

// unrelatedDDL returns true only when every statement is a recognized DDL
// operation whose referenced tables/databases do not intersect the fixed
// subscription. Parsing failures, unknown statements and ambiguous references
// remain controlled stops. Do not classify SQL using substring matching.
func unrelatedDDL(query, defaultDB string, tables []string) bool {
	statements, _, err := parser.New().ParseSQL(query)
	if err != nil || len(statements) == 0 {
		return false
	}
	subscribed := make(map[string]bool, len(tables))
	databases := make(map[string]bool)
	for _, table := range tables {
		db, _, err := splitTable(table)
		if err != nil {
			return false
		}
		subscribed[table] = true
		databases[db] = true
	}
	for _, statement := range statements {
		switch node := statement.(type) {
		case *ast.CreateDatabaseStmt:
			if databases[node.Name.O] {
				return false
			}
		case *ast.DropDatabaseStmt:
			if databases[node.Name.O] {
				return false
			}
		case *ast.AlterDatabaseStmt:
			db := node.Name.O
			if db == "" {
				db = defaultDB
			}
			if db == "" || databases[db] {
				return false
			}
		case *ast.CreateTableStmt, *ast.DropTableStmt, *ast.AlterTableStmt,
			*ast.RenameTableStmt, *ast.TruncateTableStmt,
			*ast.CreateIndexStmt, *ast.DropIndexStmt:
			visitor := &ddlReferences{defaultDB: defaultDB, subscribed: subscribed}
			_, ok := statement.Accept(visitor)
			if !ok || visitor.count == 0 || visitor.affected || visitor.ambiguous {
				return false
			}
		default:
			return false
		}
	}
	return true
}

type ddlReferences struct {
	defaultDB           string
	subscribed          map[string]bool
	count               int
	affected, ambiguous bool
}

func (v *ddlReferences) Enter(node ast.Node) (ast.Node, bool) {
	if table, ok := node.(*ast.TableName); ok {
		v.count++
		db := table.Schema.O
		if db == "" {
			db = v.defaultDB
		}
		if db == "" || table.Name.O == "" {
			v.ambiguous = true
		} else if v.subscribed[db+"."+table.Name.O] {
			v.affected = true
		}
	}
	return node, false
}

func (v *ddlReferences) Leave(node ast.Node) (ast.Node, bool) { return node, true }
