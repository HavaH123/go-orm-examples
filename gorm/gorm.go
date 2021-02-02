package gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/mysql"
	// "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	Name     string
	SchoolID sql.NullInt64
}

func insertRows(db *gorm.DB) {
	var users []User
	for i := 0; i < 200; i++ {
		user := User{Name: "gorm_student" + strconv.Itoa(i), SchoolID: sql.NullInt64{Int64: int64(i), Valid: true}}
		users = append(users, user)
	}

	for i := 0; i < 5; i++ {
		user := users[i]

		// Single Insert
		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
		result := db.WithContext(ctx).Create(&user)

		fmt.Printf("Inserted user with id: %d, rows affected: %d, err: %s\n", user.ID, result.RowsAffected, result.Error)
	}

	// Batch Insert
	result := db.Create(users[5:])

	fmt.Printf("Inserted users in batch, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)
}

func read(db *gorm.DB) {
	var users []User
	result := db.Find(&users)
	fmt.Printf("Selected all users, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)
	fmt.Printf("Length: %d, First obj: %+v \n", len(users), users[0])

	var user User
	result = db.Where(map[string]interface{}{"id": 5}).First(&user)
	fmt.Printf("Selected user id = 5, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)
	fmt.Printf("Obj: %+v \n", user)
}

func update(db *gorm.DB) {
	result := db.Model(&User{}).Where("id = ?", 5).Update("school_id", 2)
	fmt.Printf("Updated user id = 5, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)

	var user User
	result = db.Where(map[string]interface{}{"id": 5}).First(&user)
	fmt.Printf("Selected user id = 5, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)
	fmt.Printf("Obj: %+v \n", user)
}

func del(db *gorm.DB) {
	var user User
	result := db.Where(map[string]interface{}{"id": 5}).Delete(&user)
	fmt.Printf("Deleted user id = 5, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)

	result = db.Where(map[string]interface{}{"id": 5}).First(&user)
	fmt.Printf("Selected user id = 5, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)
	fmt.Printf("ErrRecordNotFound: %+v \n", errors.Is(result.Error, gorm.ErrRecordNotFound))
}

func truncate(db *gorm.DB) {
	// Execute Raw statement
	result := db.Exec("TRUNCATE `testdb`.`users`;")

	fmt.Printf("Truncated table, rows affected: %d, err: %s\n", result.RowsAffected, result.Error)
}

func ExecCommands() {
	dsn, found := os.LookupEnv("TEST_DB_DSN")
	if !found {
		panic("Set TEST_DB_DSN in env vars")
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})

	if err != nil {
		panic(err)
	}

	insertRows(db)
	read(db)
	update(db)
	del(db)
	truncate(db)
}
