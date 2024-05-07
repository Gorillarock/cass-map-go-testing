package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/osmandi/higor"
)

type connection struct {
	keyspace     string
	session      *gocql.Session
	constistency gocql.Consistency
	store        store
}

func newConnection(keyspace string, consistency gocql.Consistency) (*connection, error) {
	c := &connection{
		keyspace:     keyspace,
		constistency: consistency,
	}
	err := c.Connect()
	return c, err
}

func (c *connection) Close() {
	c.session.Close()
}

func (c *connection) Connect() error {
	var err error
	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = c.keyspace
	cluster.Consistency = c.constistency
	c.session, err = cluster.CreateSession()
	return err
}

type store struct {
	carts []shoppingCart
}

type shoppingCart struct {
	Id    string    `json:"userid"`
	Count int       `json:"item_count"`
	Time  time.Time `json:"last_updated_time"`
}

func main() {
	// Connect to the cluster
	c, err := newConnection("store", gocql.Quorum)
	if err != nil {
		panic(err)
	}
	defer c.session.Close()

	count := c.ReadStore()
	fmt.Printf("Read %d carts\n", count)
	c.store.Print()

	f, err := os.Create("./shopping_cart.csv")
	if err != nil {
		panic(err)
	}

	w := csv.NewWriter(f)
	if err := w.Write([]string{"id", "quantity", "time"}); err != nil {
		panic(err)
	}
	for _, cart := range c.store.carts {
		if err := w.Write([]string{cart.Id, fmt.Sprintf("%d", cart.Count), fmt.Sprintf("%04d%02d%02d@%02d%02d%02d", cart.Time.Year(), cart.Time.Month(), cart.Time.Day(), cart.Time.Local().Hour(), cart.Time.Local().Minute(), cart.Time.Local().Second())}); err != nil {
			panic(err)
		}
	}
	w.Flush()
	f.Close()

	df := higor.ReadCSV("./shopping_cart.csv")

	fmt.Println("\n\n")
	fmt.Println(df.Head(5))

	fmt.Println("\n\n")
	fmt.Println(df.Select("id", "quantity").WhereGreaterOrEqual("quantity", float64(3)))

}

func (c *shoppingCart) Print() {
	fmt.Printf("id: %s, quantity: %d, date: %s, %04d%02d%02d@%02d%02d%02d\n", c.Id, c.Count, c.Time.Weekday(), c.Time.Year(), c.Time.Month(), c.Time.Day(), c.Time.Local().Hour(), c.Time.Local().Minute(), c.Time.Local().Second())
}

func (s *store) Print() {
	for _, cart := range s.carts {
		cart.Print()
		fmt.Println("---------")
	}
}

func (c *connection) InsertCart(cart shoppingCart) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	return c.session.Query(`INSERT INTO store.shopping_Cart (id, quantity, time) VALUES (?, ?, ?)`, id, cart.Count, time.Now()).Exec()
}

func (c *connection) ReadStore() uint64 {
	c.store.carts = make([]shoppingCart, 0) // Refresh the store
	var cart shoppingCart
	var count uint64
	iter := c.session.Query(`SELECT * FROM store.shopping_cart`).Iter()
	for iter.Scan(&cart.Id, &cart.Count, &cart.Time) {
		count++
		c.store.carts = append(c.store.carts, cart)
	}
	return count
}
