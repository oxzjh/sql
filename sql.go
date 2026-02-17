package sql

import (
	"database/sql"
)

type isql interface {
	Exec(string, ...any) (sql.Result, error)
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
}

type IBase interface {
	Exec(string, ...any) error
	QueryOne(string, []any, ...any) error
	QueryRows(string, []any, []any, func()) error
	AffectedRows(string, ...any) (int64, error)
	InsertId(string, ...any) (int64, error)
}

type base struct {
	e isql
	q isql
}

func (b *base) Exec(query string, args ...any) (err error) {
	_, err = b.e.Exec(query, args...)
	return
}

func (b *base) QueryOne(query string, args []any, dest ...any) error {
	return b.q.QueryRow(query, args...).Scan(dest...)
}

func (b *base) QueryRows(query string, args, dest []any, callback func()) error {
	rows, err := b.q.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Scan(dest...); err != nil {
			return err
		}
		callback()
	}
	return nil
}

func (b *base) AffectedRows(query string, args ...any) (int64, error) {
	result, err := b.e.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (b *base) InsertId(query string, args ...any) (int64, error) {
	result, err := b.e.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}
