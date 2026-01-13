package mysql

import (
	"github.com/oxzjh/sql"
)

type User struct {
	Host string
	User string
}

func CreateUser(db sql.IDB, host, username, password string) {
	db.Exec("CREATE USER IF NOT EXISTS '" + username + "'@'" + host + "' IDENTIFIED BY '" + password + "'")
}

func AlterPassword(db sql.IDB, host, username, password string) {
	db.Exec("ALTER USER IF EXISTS '" + username + "'@'" + host + "' IDENTIFIED BY '" + password + "'")
}

func DropUser(db sql.IDB, host, username string) {
	db.Exec("DROP USER IF EXISTS '" + username + "'@'" + host + "'")
}

func GrantPrivileges(db sql.IDB, host, username, database string) {
	db.Exec("GRANT ALL PRIVILEGES ON " + database + ".* TO '" + username + "'@'" + host + "'")
}

func RevokePrivileges(db sql.IDB, host, username, database string) {
	db.Exec("REVOKE ALL PRIVILEGES ON " + database + ".* FROM '" + username + "'@'" + host + "'")
}

func GrantPrefix(db sql.IDB, host, username, prefix string) {
	db.Exec("GRANT ALL PRIVILEGES ON `" + prefix + "\\_%`.* TO '" + username + "'@'" + host + "'")
}

func RevokePrefix(db sql.IDB, host, username, prefix string) {
	db.Exec("REVOKE ALL PRIVILEGES ON `" + prefix + "\\_%`.* FROM '" + username + "'@'" + host + "'")
}

func FlushPrivileges(db sql.IDB) {
	db.Exec("FLUSH PRIVILEGES")
}

func GetUsers(db sql.IDB) []*User {
	users := make([]*User, 0)
	var (
		host string
		user string
	)
	db.QueryArray(
		"SELECT Host,User FROM mysql.user",
		nil,
		[]any{&host, &user},
		func() {
			users = append(users, &User{
				Host: host,
				User: user,
			})
		},
	)
	return users
}
