package sql

import (
	"database/sql"
	"log"
)

type isql interface {
	Exec(string, ...any) (sql.Result, error)
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
}

type IBase interface {
	Exec(string, ...any)
	QueryObject(string, []any, ...any)
	QueryArray(string, []any, []any, func())
	QueryRow(string, ...any) ([]string, []string)
	Query(string, ...any) ([]string, [][]string)
	AffectedRows(string, ...any) int64
	InsertId(string, ...any) int64
}

type base struct {
	e       isql
	q       isql
	onError func(error, string, []any)
}

func (b *base) Exec(query string, args ...any) {
	if _, err := b.e.Exec(query, args...); err != nil {
		b.onError(err, query, args)
	}
}

func (b *base) QueryRow(query string, args ...any) (keys []string, values []string) {
	rows, err := b.q.Query(query, args...)
	if err != nil {
		b.onError(err, query, args)
		return
	}
	defer rows.Close()
	keys, _ = rows.Columns()
	row, scans := createScans(len(keys))
	rows.Next()
	if err = rows.Scan(scans...); err != nil {
		b.onError(err, query, args)
		return
	}
	values = make([]string, len(keys))
	for i, v := range row {
		values[i] = string(v)
	}
	return
}

func (b *base) Query(query string, args ...any) (keys []string, values [][]string) {
	rows, err := b.q.Query(query, args...)
	if err != nil {
		b.onError(err, query, args)
		return
	}
	defer rows.Close()
	keys, _ = rows.Columns()
	values = make([][]string, 0)
	row, scans := createScans(len(keys))
	for rows.Next() {
		if err = rows.Scan(scans...); err != nil {
			b.onError(err, query, args)
			return
		}
		value := make([]string, len(keys))
		for i, v := range row {
			value[i] = string(v)
		}
		values = append(values, value)
	}
	return
}

func (b *base) QueryObject(query string, args []any, dest ...any) {
	if err := b.q.QueryRow(query, args...).Scan(dest...); err != nil {
		b.onError(err, query, args)
	}
}

func (b *base) QueryArray(query string, args, dest []any, callback func()) {
	rows, err := b.q.Query(query, args...)
	if err != nil {
		b.onError(err, query, args)
		return
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			b.onError(err, query, args)
			return
		}
		callback()
	}
}

func (b *base) AffectedRows(query string, args ...any) int64 {
	result, err := b.e.Exec(query, args...)
	if err != nil {
		b.onError(err, query, args)
		return 0
	}
	id, _ := result.RowsAffected()
	return id
}

func (b *base) InsertId(query string, args ...any) int64 {
	result, err := b.e.Exec(query, args...)
	if err != nil {
		b.onError(err, query, args)
		return 0
	}
	id, _ := result.LastInsertId()
	return id
}

func defaultOnError(err error, query string, args []any) {
	log.Println(query, args)
	log.Println(err)
}

func createScans(n int) (values [][]byte, scans []any) {
	values = make([][]byte, n)
	scans = make([]any, n)
	for i := range values {
		scans[i] = &values[i]
	}
	return
}
