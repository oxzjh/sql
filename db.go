package sql

import (
	"database/sql"
	"database/sql/driver"
	"time"
)

type IDB interface {
	IBase
	Driver() string
	GetExecDB() *sql.DB
	GetQueryDB() *sql.DB
	Transact(func(IBase) bool)
	OnError(func(error, string, []any))
	Close()
}

type db struct {
	base
	driver string
}

func (d *db) Driver() string {
	return d.driver
}

func (d *db) GetExecDB() *sql.DB {
	return d.e.(*sql.DB)
}

func (d *db) GetQueryDB() *sql.DB {
	return d.q.(*sql.DB)
}

func (d *db) Transact(handler func(IBase) bool) {
	tx, err := d.GetExecDB().Begin()
	if err != nil {
		d.onError(err, "begin", nil)
	}
	if handler(&base{tx, tx, d.onError}) {
		tx.Commit()
	} else {
		tx.Rollback()
	}
}

func (d *db) OnError(onError func(error, string, []any)) {
	if onError == nil {
		d.onError = defaultOnError
	} else {
		d.onError = onError
	}
}

func (d *db) Close() {
	execDB := d.GetExecDB()
	execDB.Close()
	if queryDB := d.GetQueryDB(); queryDB != execDB {
		queryDB.Close()
	}
}

func Open(driver, source string, pool int, maxIdleTime time.Duration) (IDB, error) {
	execDB, err := sql.Open(driver, source)
	if err != nil {
		return nil, err
	}
	if pool > 0 {
		execDB.SetMaxOpenConns(pool)
	}
	if maxIdleTime > 0 {
		execDB.SetConnMaxIdleTime(maxIdleTime)
	}
	return &db{base{execDB, execDB, defaultOnError}, driver}, nil
}

func OpenDB(driver string, connector driver.Connector, pool int, maxIdleTime time.Duration) (IDB, error) {
	execDB := sql.OpenDB(connector)
	if pool > 0 {
		execDB.SetMaxOpenConns(pool)
	}
	if maxIdleTime > 0 {
		execDB.SetConnMaxIdleTime(maxIdleTime)
	}
	return &db{base{execDB, execDB, defaultOnError}, driver}, nil
}

func OpenSeparated(driver, execSource, querySource string, pool int, maxIdleTime time.Duration) (IDB, error) {
	execDB, err := sql.Open(driver, execSource)
	if err != nil {
		return nil, err
	}
	queryDB, err := sql.Open(driver, querySource)
	if err != nil {
		execDB.Close()
		return nil, err
	}
	if pool > 0 {
		execDB.SetMaxOpenConns(pool)
		queryDB.SetMaxOpenConns(pool)
	}
	if maxIdleTime > 0 {
		execDB.SetConnMaxIdleTime(maxIdleTime)
		queryDB.SetConnMaxIdleTime(maxIdleTime)
	}
	return &db{base{execDB, queryDB, defaultOnError}, driver}, nil
}
