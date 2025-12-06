package mysql

import (
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/oxzjh/sql"
)

func Open(source string, pool int, maxIdleTime time.Duration) (sql.IDB, error) {
	return sql.Open("mysql", source, pool, maxIdleTime)
}

func OpenDB(cfg *mysql.Config, pool int, maxIdleTime time.Duration) (sql.IDB, error) {
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB("mysql", connector, pool, maxIdleTime)
}

func OpenSeparated(execSource, querySource string, pool int, maxIdleTime time.Duration) (sql.IDB, error) {
	return sql.OpenSeparated("mysql", execSource, querySource, pool, maxIdleTime)
}
