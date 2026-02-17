package mysql

import (
	"github.com/oxzjh/sql"
)

type Database struct {
	Name    string `json:"name"`
	Charset string `json:"charset"`
	Collate string `json:"collate"`
}

func CreateDatabase(db sql.IDB, name string) error {
	return db.Exec("CREATE DATABASE " + name + " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
}

func DropDatabase(db sql.IDB, name string) error {
	return db.Exec("DROP DATABASE " + name)
}

func UseDatabase(db sql.IDB, name string) error {
	return db.Exec("USE " + name)
}

func GetDatabases(db sql.IDB) []*Database {
	databases := make([]*Database, 0)
	var (
		name    string
		charset string
		collate string
	)
	db.QueryRows(
		"SELECT SCHEMA_NAME,DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA",
		nil,
		[]any{&name, &charset, &collate},
		func() {
			databases = append(databases, &Database{
				Name:    name,
				Charset: charset,
				Collate: collate,
			})
		},
	)
	return databases
}
