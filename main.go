package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
)

const connect = "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
const sqlCreateTable = `CREATE TABLE IF NOT EXISTS messages (
	id UUID PRIMARY KEY, 
	source STRING NOT NULL,
	title STRING NOT NULL,
	body STRING NOT NULL
);`

type Message struct {
	ID     uuid.UUID
	Source string
	Title  string
	Body   string
}

type Event struct {
	Table string
	Keys  []string
	Value Value
}

type Value struct {
	Resolved string   `json:"resolved,omitempty"`
	After    *Message `json:"after,omitempty"`
	Updated  *string  `json:"updated"`
}

var cursor string

func main() {

	flag.StringVar(&cursor, "c", "", "Start changefeed from specfied resolved timestamp (like 1586782034314054700.0000000000)")
	flag.Parse()

	go func() {
		db, err := pgx.Connect(context.Background(), connect)
		if err != nil {
			panic(err)
		}

		if _, err := db.Exec(context.Background(), sqlCreateTable); err != nil {
			panic(err)
		}

		for {
			msg := Message{
				ID:     uuid.New(),
				Source: randomString(10) + "@" + "noemail.com",
				Title:  randomString(20),
				Body:   randomString(20),
			}

			_, err := db.Exec(
				context.Background(),
				`INSERT INTO messages (id, source, title, body) VALUES ($1, $2, $3, $4)`,
				msg.ID, msg.Source, msg.Title, msg.Body)
			if err != nil {
				panic(err)
			}

			time.Sleep(1 * time.Second)
		}
	}()

	config, err := pgx.ParseConfig(connect + "&statement_cache_mode=prepare")
	if err != nil {
		panic(err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	cdcQuery := "EXPERIMENTAL CHANGEFEED FOR messages"

	if cursor != "" {
		cdcQuery = cdcQuery + fmt.Sprintf(" WITH updated, cursor='%s'", cursor)
	} else {
		cdcQuery = cdcQuery + " WITH updated, resolved"
	}

	fmt.Println(cdcQuery)

	rows, err := conn.Query(
		context.Background(),
		cdcQuery)
	if err != nil {
		panic(err)
	}

	for {
		if rows.Next() {

			rawValues := rows.RawValues()

			for i, v := range rawValues {
				fmt.Printf("raw[%d]: '%s'\n", i, v)
			}

			event := Event{
				Table: string(rawValues[0]),
			}

			if len(rawValues[1]) > 0 {
				if err := json.Unmarshal(rawValues[1], &event.Keys); err != nil {
					panic(err)
				}
			}

			if err := json.Unmarshal(rawValues[2], &event.Value); err != nil {
				panic(err)
			}

			fmt.Println(event)
		}
	}

}

const charset = "abcdefghijklmnopqrstuvwxyz"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
