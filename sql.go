package sql

import (
	"database/sql"
	"log"
)

type ISQL interface {
	GetDB() IDB
	Exec(string, ...any)
	QueryOne(string, []any, ...any)
	QueryRows(string, []any, []any, func())
	AffectedRows(string, ...any) int64
	InsertId(string, ...any) int64
	Driver() string
	GetExecDB() *sql.DB
	GetQueryDB() *sql.DB
	Transact(func(IBase) error)
	Close()
	SetOnError(func(string, []any, error))
}

type wrap struct {
	db      IDB
	onError func(string, []any, error)
}

func (w *wrap) GetDB() IDB {
	return w.db
}

func (w *wrap) Exec(query string, args ...any) {
	if err := w.db.Exec(query, args...); err != nil {
		w.onError(query, args, err)
	}
}

func (w *wrap) QueryOne(query string, args []any, dest ...any) {
	if err := w.db.QueryOne(query, args, dest); err != nil {
		w.onError(query, args, err)
	}
}

func (w *wrap) QueryRows(query string, args []any, dest []any, callback func()) {
	if err := w.db.QueryRows(query, args, dest, callback); err != nil {
		w.onError(query, args, err)
	}
}

func (w *wrap) AffectedRows(query string, args ...any) int64 {
	rows, err := w.db.AffectedRows(query, args...)
	if err != nil {
		w.onError(query, args, err)
	}
	return rows
}

func (w *wrap) InsertId(query string, args ...any) int64 {
	id, err := w.db.InsertId(query, args...)
	if err != nil {
		w.onError(query, args, err)
	}
	return id
}

func (w *wrap) Driver() string {
	return w.db.Driver()
}

func (w *wrap) GetExecDB() *sql.DB {
	return w.db.GetExecDB()
}

func (w *wrap) GetQueryDB() *sql.DB {
	return w.db.GetQueryDB()
}

func (w *wrap) Transact(handler func(IBase) error) {
	if err := w.db.Transact(handler, nil); err != nil {
		w.onError("BEGIN", nil, err)
	}
}

func (w *wrap) Close() {
	w.db.Close()
}

func (w *wrap) SetOnError(onError func(string, []any, error)) {
	if onError == nil {
		w.onError = defaultOnError
	} else {
		w.onError = onError
	}
}

func New(db IDB, onError func(string, []any, error)) ISQL {
	if onError == nil {
		onError = defaultOnError
	}
	return &wrap{db, onError}
}

func defaultOnError(query string, args []any, err error) {
	log.Println(query, args)
	log.Println(err)
}
