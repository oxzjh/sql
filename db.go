package sql

import (
	"database/sql"
	"database/sql/driver"
	"log"
	"time"
)

type IDB interface {
	IBase
	Driver() string
	GetExecDB() *sql.DB
	GetQueryDB() *sql.DB
	Transact(func(IBase) error, func(any)) error
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

func (d *db) Transact(handler func(IBase) error, onPanic func(any)) error {
	tx, err := d.GetExecDB().Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := recover(); err != nil {
			if onPanic != nil {
				onPanic(err)
			} else {
				log.Println(err)
			}
			tx.Rollback()
		}
	}()
	if err := handler(&base{tx, tx}); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
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
	if err = execDB.Ping(); err != nil {
		execDB.Close()
		return nil, err
	}
	if pool > 0 {
		execDB.SetMaxOpenConns(pool)
	}
	if maxIdleTime > 0 {
		execDB.SetConnMaxIdleTime(maxIdleTime)
	}
	return &db{base{execDB, execDB}, driver}, nil
}

func OpenDB(driver string, connector driver.Connector, pool int, maxIdleTime time.Duration) (IDB, error) {
	execDB := sql.OpenDB(connector)
	if err := execDB.Ping(); err != nil {
		execDB.Close()
		return nil, err
	}
	if pool > 0 {
		execDB.SetMaxOpenConns(pool)
	}
	if maxIdleTime > 0 {
		execDB.SetConnMaxIdleTime(maxIdleTime)
	}
	return &db{base{execDB, execDB}, driver}, nil
}

func OpenSeparated(driver, execSource, querySource string, pool int, maxIdleTime time.Duration) (IDB, error) {
	execDB, err := sql.Open(driver, execSource)
	if err != nil {
		return nil, err
	}
	if err = execDB.Ping(); err != nil {
		execDB.Close()
		return nil, err
	}
	queryDB, err := sql.Open(driver, querySource)
	if err != nil {
		execDB.Close()
		return nil, err
	}
	if err = queryDB.Ping(); err != nil {
		execDB.Close()
		queryDB.Close()
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
	return &db{base{execDB, queryDB}, driver}, nil
}
