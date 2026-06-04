// Package test contains REST API integration and controller tests.
package test

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load("../.env")
	if err != nil {
		fmt.Printf("[Test Init] Godotenv load error: %v\n", err)
	} else {
		fmt.Printf("[Test Init] Loaded .env. DB_PORT is: %s\n", os.Getenv("DB_PORT"))
	}
}
