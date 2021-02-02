package sqlx

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	// _ "github.com/lib/pq"
	_ "github.com/go-sql-driver/mysql"
)

type User struct {
	ID        uint
	CreatedAt time.Time    `db:"created_at"`
	UpdatedAt time.Time    `db:"updated_at"`
	DeletedAt sql.NullTime `db:"deleted_at"`
	Name      string
	SchoolID  sql.NullInt64 `db:"school_id"`
}

func insertRows(db *sqlx.DB) {
	var users []User
	for i := 0; i < 200; i++ {
		user := User{Name: "sqlx_student" + strconv.Itoa(i), SchoolID: sql.NullInt64{Int64: int64(i), Valid: true}}
		users = append(users, user)
	}

	for i := 0; i < 5; i++ {
		user := users[i]

		// Single Insert
		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
		now := time.Now()
		query := `INSERT INTO users (name, school_id, created_at, updated_at, deleted_at) VALUES (?, ?, ?, ?, ?)`
		result, err := db.ExecContext(ctx, query, user.Name, user.SchoolID, now, now, nil)

		if err != nil {
			fmt.Printf("Failed to insert: %+v\n", err)
		} else {
			rowsAffected, _ := result.RowsAffected()
			lastId, _ := result.LastInsertId()
			fmt.Printf("Inserted user with id: %d, rows affected: %d, err: %s\n", lastId, rowsAffected)
		}
	}

	// Batch Insert
	sqlStr := "INSERT INTO users(name, school_id, created_at, updated_at, deleted_at) VALUES "
	vals := []interface{}{}
	now := time.Now()

	for _, user := range users[5:] {
		sqlStr += "(?, ?, ?, ?, ?),"
		vals = append(vals, user.Name, user.SchoolID, now, now, nil)
	}

	//trim the last ,
	sqlStr = strings.TrimSuffix(sqlStr, ",")

	//prepare the statement
	stmt, _ := db.Prepare(sqlStr)

	//format all vals at once
	result, err := stmt.Exec(vals...)
	if err != nil {
		fmt.Printf("Failed to batch insert: %+v\n", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		lastId, _ := result.LastInsertId()
		fmt.Printf("Inserted users in batch with id: %d, rows affected: %d, err: %s\n", lastId, rowsAffected)
	}
}

func read(db *sqlx.DB) {
	var users []User
	err := db.Select(&users, "select * from users;")
	fmt.Printf("Selected all users, rows affected: %d, err: %s\n", len(users), err)
	fmt.Printf("Length: %d, First obj: %+v \n", len(users), users[0])

	var user User
	err = db.Get(&user, "select * from users where id = 5;")
	fmt.Printf("Selected user id = 5, err: %s\n", err)
	fmt.Printf("Obj: %+v \n", user)
}

func update(db *sqlx.DB) {

	_, err := db.Exec("UPDATE `users` SET `school_id`=?,`updated_at`=? WHERE id = ?", 2, time.Now(), 5)
	fmt.Printf("Updated user id = 5, err: %s\n", err)

	var user User
	err = db.Get(&user, "select * from users where id = 5;")
	fmt.Printf("Selected user id = 5, err: %s\n", err)
	fmt.Printf("Obj: %+v \n", user)
}

func del(db *sqlx.DB) {
	_, err := db.Exec("DELETE from `users` WHERE id = ?", 5)
	fmt.Printf("Deleted user id = 5, err: %s\n", err)

	var user User
	err = db.Get(&user, "select * from users where id = 5;")
	fmt.Printf("Selected user id = 5, err: %s\n", err)
	fmt.Printf("Obj: %+v \n", user)
}

func truncate(db *sqlx.DB) {
	_, err := db.Exec("TRUNCATE `testdb`.`users`;")
	if err != nil {
		fmt.Printf("Failed to truncate: %+v\n", err)
	}
	fmt.Println("Truncated users")
}

func ExecCommands() {
	dsn, found := os.LookupEnv("TEST_DB_DSN")
	if !found {
		panic("Set TEST_DB_DSN in env vars")
	}
	db := sqlx.MustConnect("mysql", dsn)

	insertRows(db)
	read(db)
	update(db)
	del(db)
	truncate(db)
}
