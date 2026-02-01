package sqlite3

import (
	_ "github.com/mattn/go-sqlite3"
	"github.com/oxzjh/sql"
)

func Open(source string) (sql.IDB, error) {
	return sql.Open("sqlite3", source, 0, 0)
}
