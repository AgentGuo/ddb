/*
@author: panfengguo
@since: 2022/11/17
@desc: desc
*/
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	driverType    = "mysql"
	dataBasePath  = "../../data/"
	bookPath      = dataBasePath + "book.tsv"
	customerPath  = dataBasePath + "customer.tsv"
	ordersPath    = dataBasePath + "orders.tsv"
	publisherPath = dataBasePath + "publisher.tsv"
)

const (
	dropPublisherSql   = `DROP TABLE IF EXISTS Publisher;`
	createPublisherSql = `
CREATE TABLE IF NOT EXISTS Publisher(
id INT,
name CHAR(100),
nation CHAR(3),
PRIMARY KEY(id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;`
	dropCustomerSql    = `DROP TABLE IF EXISTS Customer;`
	createCustomerSql1 = `
CREATE TABLE IF NOT EXISTS Customer(
id INT,
name CHAR(25),
PRIMARY KEY(id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;`

	createCustomerSql2 = "CREATE TABLE IF NOT EXISTS Customer(id INT,`rank` INT, PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	dropBookSql        = `DROP TABLE IF EXISTS Book;`
	createBookSql      = `
CREATE TABLE IF NOT EXISTS Book(
id INT,
title CHAR(100),
authors CHAR(200),
publisher_id INT,
copies INT,
PRIMARY KEY(id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;`
	dropOrdersSql   = `DROP TABLE IF EXISTS Orders;`
	createOrdersSql = `
CREATE TABLE IF NOT EXISTS Orders(
customer_id INT,
book_id INT,
quantity INT
)ENGINE=InnoDB DEFAULT CHARSET=utf8;`
)

const (
	insertPublisherSql = `INSERT INTO Publisher VALUES(?, ?, ?);`
	insertCustomerSql  = `INSERT INTO Customer VALUES(?, ?);`
	insertBookSql      = `INSERT INTO Book VALUES(?, ?, ?, ?, ?);`
	insertOrdersSql    = `INSERT INTO Orders VALUES(?, ?, ?);`
)
const (
	SITE1 int = iota // value --> 0
	SITE2            // value --> 1
	SITE3            // value --> 2
	SITE4            // value --> 3
)

var dbAddr = []string{
	"root:foobar@tcp(10.77.50.214:23306)/ddb?charset=utf8&multiStatements=true",
	"root:foobar@tcp(10.77.50.214:33306)/ddb?charset=utf8&multiStatements=true",
	"root:foobar@tcp(10.77.110.228:23306)/ddb?charset=utf8&multiStatements=true",
	"root:foobar@tcp(10.77.110.158:23306)/ddb?charset=utf8&multiStatements=true",
}

func main() {
	var wg sync.WaitGroup
	go func() {
		wg.Add(1)
		err := site1Init()
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		err := site2Init()
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		err := site3Init()
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		err := site4Init()
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()
	time.Sleep(time.Second)
	wg.Wait()
}

// getTuples
//
//	@Description: read tuple from tsv file
//	@param dataPath: the tsv file path
//	@return []string
//	@return error
func getTuples(dataPath string) ([][]string, error) {
	fi, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	defer fi.Close()
	tupleList := make([][]string, 0)
	br := bufio.NewReader(fi)
	for {
		tmp, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		tuple := strings.FieldsFunc(string(tmp), func(r rune) bool {
			return r == '\t'
		})
		if len(tuple) > 0 {
			tupleList = append(tupleList, tuple)
		}
	}
	return tupleList, nil
}

type publisher struct {
	id           int
	name, nation string
}

type customer struct {
	id, rank int
	name     string
}

type book struct {
	id, publisherId, copies int
	title, authors          string
}

type orders struct {
	customerId, quantity, bookId int
}

func site1Init() error {
	fmt.Println("---site1 init---")
	db, err := sql.Open(driverType, dbAddr[SITE1])
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// See "Important settings" section.
	db.SetConnMaxLifetime(-1)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	publisherRawTuples, err := getTuples(publisherPath)
	if err != nil {
		return err
	}
	// publisher slice
	publisherSlice := []publisher{}
	for _, tuple := range publisherRawTuples {
		if len(tuple) != 3 {
			return fmt.Errorf("publisher tuple size != 3")
		}
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("pulisher id convert failed:%v", id)
		}
		tmp := publisher{
			id:     id,
			name:   tuple[1],
			nation: tuple[2],
		}
		if tmp.id < 104000 && tmp.nation == "PRC" {
			publisherSlice = append(publisherSlice, tmp)
		}
	}
	fmt.Printf("site1.pulisherSlice.size = %d\n", len(publisherSlice))
	// customer slice
	customerRawTuples, err := getTuples(customerPath)
	if err != nil {
		return err
	}
	customerSlice := []customer{}
	for _, tuple := range customerRawTuples {
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("customer id convert failed:%v", id)
		}
		rank, err := strconv.Atoi(tuple[2])
		if err != nil {
			return fmt.Errorf("customer rank convert failed:%v", rank)
		}
		customerSlice = append(customerSlice, customer{
			id:   id,
			rank: rank,
			name: tuple[1],
		})
	}
	fmt.Printf("site1.customerSlice.size = %d\n", len(customerSlice))
	// book slice
	bookRawTuples, err := getTuples(bookPath)
	if err != nil {
		return err
	}
	bookSlice := []book{}
	for _, tuple := range bookRawTuples {
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("book id convert failed:%v", id)
		}
		publisherId, err := strconv.Atoi(tuple[3])
		if err != nil {
			return fmt.Errorf("book publisher id convert failed:%v", publisherId)
		}
		copies, err := strconv.Atoi(tuple[4])
		if err != nil {
			return fmt.Errorf("book copies convert failed:%v", copies)
		}
		if id < 205000 {
			bookSlice = append(bookSlice, book{
				id:          id,
				publisherId: publisherId,
				copies:      copies,
				title:       tuple[1],
				authors:     tuple[2],
			})
		}
	}
	fmt.Printf("site1.bookSlice.size = %d\n", len(bookSlice))
	ordersRawTuples, err := getTuples(ordersPath)
	if err != nil {
		return err
	}
	ordersSlice := []orders{}
	for _, tuple := range ordersRawTuples {
		customerId, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("orders customer id convert failed:%v", customerId)
		}
		bookId, err := strconv.Atoi(tuple[1])
		if err != nil {
			return fmt.Errorf("orders book id convert failed:%v", bookId)
		}
		quantity, err := strconv.Atoi(tuple[2])
		if err != nil {
			return fmt.Errorf("orders quantity convert failed:%v", quantity)
		}
		if customerId < 307000 && bookId < 215000 {
			ordersSlice = append(ordersSlice, orders{
				customerId: customerId,
				quantity:   quantity,
				bookId:     bookId,
			})
		}
	}
	fmt.Printf("site1.ordersSlice.size = %d\n", len(ordersSlice))
	// sql exec
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	dropPbTbStmt, err := tx.Prepare(dropPublisherSql)
	if err != nil {
		return err
	}
	_, err = dropPbTbStmt.Exec()
	if err != nil {
		return err
	}
	dropCmTbStmt, err := tx.Prepare(dropCustomerSql)
	if err != nil {
		return err
	}
	_, err = dropCmTbStmt.Exec()
	if err != nil {
		return err
	}
	dropBkTbStmt, err := tx.Prepare(dropBookSql)
	if err != nil {
		return err
	}
	_, err = dropBkTbStmt.Exec()
	if err != nil {
		return err
	}
	dropOdTbStmt, err := tx.Prepare(dropOrdersSql)
	if err != nil {
		return err
	}
	_, err = dropOdTbStmt.Exec()
	if err != nil {
		return err
	}
	createPbTbStmt, err := tx.Prepare(createPublisherSql)
	if err != nil {
		return err
	}
	_, err = createPbTbStmt.Exec()
	if err != nil {
		return err
	}
	createCmTbStmt, err := tx.Prepare(createCustomerSql1)
	if err != nil {
		return err
	}
	_, err = createCmTbStmt.Exec()
	if err != nil {
		return err
	}
	createBkTbStmt, err := tx.Prepare(createBookSql)
	if err != nil {
		return err
	}
	_, err = createBkTbStmt.Exec()
	if err != nil {
		return err
	}
	createOdTbStmt, err := tx.Prepare(createOrdersSql)
	if err != nil {
		return err
	}
	_, err = createOdTbStmt.Exec()
	if err != nil {
		return err
	}
	insertPbStmt, err := tx.Prepare(insertPublisherSql)
	if err != nil {
		return err
	}
	for _, pb := range publisherSlice {
		_, err = insertPbStmt.Exec(pb.id, pb.name, pb.nation)
		if err != nil {
			return err
		}
	}
	insertCmStmt, err := tx.Prepare(insertCustomerSql)
	if err != nil {
		return err
	}
	for _, cm := range customerSlice {
		_, err = insertCmStmt.Exec(cm.id, cm.name)
		if err != nil {
			return err
		}
	}
	insertBkStmt, err := tx.Prepare(insertBookSql)
	if err != nil {
		return err
	}
	for _, bk := range bookSlice {
		_, err = insertBkStmt.Exec(bk.id, bk.title, bk.authors, bk.publisherId, bk.copies)
	}
	insertOdStmt, err := tx.Prepare(insertOrdersSql)
	if err != nil {
		return err
	}
	for _, od := range ordersSlice {
		_, err = insertOdStmt.Exec(od.customerId, od.bookId, od.quantity)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func site2Init() error {
	fmt.Println("---site2 init---")
	db, err := sql.Open(driverType, dbAddr[SITE2])
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// See "Important settings" section.
	db.SetConnMaxLifetime(-1)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	publisherRawTuples, err := getTuples(publisherPath)
	if err != nil {
		return err
	}
	// publisher slice
	publisherSlice := []publisher{}
	for _, tuple := range publisherRawTuples {
		if len(tuple) != 3 {
			return fmt.Errorf("publisher tuple size != 3")
		}
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("pulisher id convert failed:%v", id)
		}
		tmp := publisher{
			id:     id,
			name:   tuple[1],
			nation: tuple[2],
		}
		if tmp.id < 104000 && tmp.nation == "USA" {
			publisherSlice = append(publisherSlice, tmp)
		}
	}
	fmt.Printf("site2.pulisherSlice.size = %d\n", len(publisherSlice))
	// customer slice
	customerRawTuples, err := getTuples(customerPath)
	if err != nil {
		return err
	}
	customerSlice := []customer{}
	for _, tuple := range customerRawTuples {
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("customer id convert failed:%v", id)
		}
		rank, err := strconv.Atoi(tuple[2])
		if err != nil {
			return fmt.Errorf("customer rank convert failed:%v", rank)
		}
		customerSlice = append(customerSlice, customer{
			id:   id,
			rank: rank,
			name: tuple[1],
		})
	}
	fmt.Printf("site2.customerSlice.size = %d\n", len(customerSlice))
	// book slice
	bookRawTuples, err := getTuples(bookPath)
	if err != nil {
		return err
	}
	bookSlice := []book{}
	for _, tuple := range bookRawTuples {
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("book id convert failed:%v", id)
		}
		publisherId, err := strconv.Atoi(tuple[3])
		if err != nil {
			return fmt.Errorf("book publisher id convert failed:%v", publisherId)
		}
		copies, err := strconv.Atoi(tuple[4])
		if err != nil {
			return fmt.Errorf("book copies convert failed:%v", copies)
		}
		if id >= 205000 && id < 210000 {
			bookSlice = append(bookSlice, book{
				id:          id,
				publisherId: publisherId,
				copies:      copies,
				title:       tuple[1],
				authors:     tuple[2],
			})
		}
	}
	fmt.Printf("site2.bookSlice.size = %d\n", len(bookSlice))
	ordersRawTuples, err := getTuples(ordersPath)
	if err != nil {
		return err
	}
	ordersSlice := []orders{}
	for _, tuple := range ordersRawTuples {
		customerId, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("orders customer id convert failed:%v", customerId)
		}
		bookId, err := strconv.Atoi(tuple[1])
		if err != nil {
			return fmt.Errorf("orders book id convert failed:%v", bookId)
		}
		quantity, err := strconv.Atoi(tuple[2])
		if err != nil {
			return fmt.Errorf("orders quantity convert failed:%v", quantity)
		}
		if customerId < 307000 && bookId >= 215000 {
			ordersSlice = append(ordersSlice, orders{
				customerId: customerId,
				quantity:   quantity,
				bookId:     bookId,
			})
		}
	}
	fmt.Printf("site2.ordersSlice.size = %d\n", len(ordersSlice))
	// sql exec
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	dropPbTbStmt, err := tx.Prepare(dropPublisherSql)
	if err != nil {
		return err
	}
	_, err = dropPbTbStmt.Exec()
	if err != nil {
		return err
	}
	dropCmTbStmt, err := tx.Prepare(dropCustomerSql)
	if err != nil {
		return err
	}
	_, err = dropCmTbStmt.Exec()
	if err != nil {
		return err
	}
	dropBkTbStmt, err := tx.Prepare(dropBookSql)
	if err != nil {
		return err
	}
	_, err = dropBkTbStmt.Exec()
	if err != nil {
		return err
	}
	dropOdTbStmt, err := tx.Prepare(dropOrdersSql)
	if err != nil {
		return err
	}
	_, err = dropOdTbStmt.Exec()
	if err != nil {
		return err
	}
	createPbTbStmt, err := tx.Prepare(createPublisherSql)
	if err != nil {
		return err
	}
	_, err = createPbTbStmt.Exec()
	if err != nil {
		return err
	}
	createCmTbStmt, err := tx.Prepare(createCustomerSql2)
	if err != nil {
		return err
	}
	_, err = createCmTbStmt.Exec()
	if err != nil {
		return err
	}
	createBkTbStmt, err := tx.Prepare(createBookSql)
	if err != nil {
		return err
	}
	_, err = createBkTbStmt.Exec()
	if err != nil {
		return err
	}
	createOdTbStmt, err := tx.Prepare(createOrdersSql)
	if err != nil {
		return err
	}
	_, err = createOdTbStmt.Exec()
	if err != nil {
		return err
	}
	insertPbStmt, err := tx.Prepare(insertPublisherSql)
	if err != nil {
		return err
	}
	for _, pb := range publisherSlice {
		_, err = insertPbStmt.Exec(pb.id, pb.name, pb.nation)
		if err != nil {
			return err
		}
	}
	insertCmStmt, err := tx.Prepare(insertCustomerSql)
	if err != nil {
		return err
	}
	for _, cm := range customerSlice {
		_, err = insertCmStmt.Exec(cm.id, cm.rank)
		if err != nil {
			return err
		}
	}
	insertBkStmt, err := tx.Prepare(insertBookSql)
	if err != nil {
		return err
	}
	for _, bk := range bookSlice {
		_, err = insertBkStmt.Exec(bk.id, bk.title, bk.authors, bk.publisherId, bk.copies)
	}
	insertOdStmt, err := tx.Prepare(insertOrdersSql)
	if err != nil {
		return err
	}
	for _, od := range ordersSlice {
		_, err = insertOdStmt.Exec(od.customerId, od.bookId, od.quantity)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func site3Init() error {
	fmt.Println("---site3 init---")
	db, err := sql.Open(driverType, dbAddr[SITE3])
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// See "Important settings" section.
	db.SetConnMaxLifetime(-1)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	publisherRawTuples, err := getTuples(publisherPath)
	if err != nil {
		return err
	}
	// publisher slice
	publisherSlice := []publisher{}
	for _, tuple := range publisherRawTuples {
		if len(tuple) != 3 {
			return fmt.Errorf("publisher tuple size != 3")
		}
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("pulisher id convert failed:%v", id)
		}
		tmp := publisher{
			id:     id,
			name:   tuple[1],
			nation: tuple[2],
		}
		if tmp.id >= 104000 && tmp.nation == "PRC" {
			publisherSlice = append(publisherSlice, tmp)
		}
	}
	fmt.Printf("site3.pulisherSlice.size = %d\n", len(publisherSlice))
	// book slice
	bookRawTuples, err := getTuples(bookPath)
	if err != nil {
		return err
	}
	bookSlice := []book{}
	for _, tuple := range bookRawTuples {
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("book id convert failed:%v", id)
		}
		publisherId, err := strconv.Atoi(tuple[3])
		if err != nil {
			return fmt.Errorf("book publisher id convert failed:%v", publisherId)
		}
		copies, err := strconv.Atoi(tuple[4])
		if err != nil {
			return fmt.Errorf("book copies convert failed:%v", copies)
		}
		if id >= 210000 {
			bookSlice = append(bookSlice, book{
				id:          id,
				publisherId: publisherId,
				copies:      copies,
				title:       tuple[1],
				authors:     tuple[2],
			})
		}
	}
	fmt.Printf("site3.bookSlice.size = %d\n", len(bookSlice))
	ordersRawTuples, err := getTuples(ordersPath)
	if err != nil {
		return err
	}
	ordersSlice := []orders{}
	for _, tuple := range ordersRawTuples {
		customerId, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("orders customer id convert failed:%v", customerId)
		}
		bookId, err := strconv.Atoi(tuple[1])
		if err != nil {
			return fmt.Errorf("orders book id convert failed:%v", bookId)
		}
		quantity, err := strconv.Atoi(tuple[2])
		if err != nil {
			return fmt.Errorf("orders quantity convert failed:%v", quantity)
		}
		if customerId >= 307000 && bookId < 215000 {
			ordersSlice = append(ordersSlice, orders{
				customerId: customerId,
				quantity:   quantity,
				bookId:     bookId,
			})
		}
	}
	fmt.Printf("site3.ordersSlice.size = %d\n", len(ordersSlice))
	// sql exec
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	dropPbTbStmt, err := tx.Prepare(dropPublisherSql)
	if err != nil {
		return err
	}
	_, err = dropPbTbStmt.Exec()
	if err != nil {
		return err
	}
	dropBkTbStmt, err := tx.Prepare(dropBookSql)
	if err != nil {
		return err
	}
	_, err = dropBkTbStmt.Exec()
	if err != nil {
		return err
	}
	dropOdTbStmt, err := tx.Prepare(dropOrdersSql)
	if err != nil {
		return err
	}
	_, err = dropOdTbStmt.Exec()
	if err != nil {
		return err
	}
	createPbTbStmt, err := tx.Prepare(createPublisherSql)
	if err != nil {
		return err
	}
	_, err = createPbTbStmt.Exec()
	if err != nil {
		return err
	}
	createBkTbStmt, err := tx.Prepare(createBookSql)
	if err != nil {
		return err
	}
	_, err = createBkTbStmt.Exec()
	if err != nil {
		return err
	}
	createOdTbStmt, err := tx.Prepare(createOrdersSql)
	if err != nil {
		return err
	}
	_, err = createOdTbStmt.Exec()
	if err != nil {
		return err
	}
	insertPbStmt, err := tx.Prepare(insertPublisherSql)
	if err != nil {
		return err
	}
	for _, pb := range publisherSlice {
		_, err = insertPbStmt.Exec(pb.id, pb.name, pb.nation)
		if err != nil {
			return err
		}
	}
	insertBkStmt, err := tx.Prepare(insertBookSql)
	if err != nil {
		return err
	}
	for _, bk := range bookSlice {
		_, err = insertBkStmt.Exec(bk.id, bk.title, bk.authors, bk.publisherId, bk.copies)
	}
	insertOdStmt, err := tx.Prepare(insertOrdersSql)
	if err != nil {
		return err
	}
	for _, od := range ordersSlice {
		_, err = insertOdStmt.Exec(od.customerId, od.bookId, od.quantity)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func site4Init() error {
	fmt.Println("---site4 init---")
	db, err := sql.Open(driverType, dbAddr[SITE4])
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// See "Important settings" section.
	db.SetConnMaxLifetime(-1)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	publisherRawTuples, err := getTuples(publisherPath)
	if err != nil {
		return err
	}
	// publisher slice
	publisherSlice := []publisher{}
	for _, tuple := range publisherRawTuples {
		if len(tuple) != 3 {
			return fmt.Errorf("publisher tuple size != 3")
		}
		id, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("pulisher id convert failed:%v", id)
		}
		tmp := publisher{
			id:     id,
			name:   tuple[1],
			nation: tuple[2],
		}
		if tmp.id >= 104000 && tmp.nation == "USA" {
			publisherSlice = append(publisherSlice, tmp)
		}
	}
	fmt.Printf("site4.pulisherSlice.size = %d\n", len(publisherSlice))
	ordersRawTuples, err := getTuples(ordersPath)
	if err != nil {
		return err
	}
	ordersSlice := []orders{}
	for _, tuple := range ordersRawTuples {
		customerId, err := strconv.Atoi(tuple[0])
		if err != nil {
			return fmt.Errorf("orders customer id convert failed:%v", customerId)
		}
		bookId, err := strconv.Atoi(tuple[1])
		if err != nil {
			return fmt.Errorf("orders book id convert failed:%v", bookId)
		}
		quantity, err := strconv.Atoi(tuple[2])
		if err != nil {
			return fmt.Errorf("orders quantity convert failed:%v", quantity)
		}
		if customerId >= 307000 && bookId >= 215000 {
			ordersSlice = append(ordersSlice, orders{
				customerId: customerId,
				quantity:   quantity,
				bookId:     bookId,
			})
		}
	}
	fmt.Printf("site4.ordersSlice.size = %d\n", len(ordersSlice))
	// sql exec
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	dropPbTbStmt, err := tx.Prepare(dropPublisherSql)
	if err != nil {
		return err
	}
	_, err = dropPbTbStmt.Exec()
	if err != nil {
		return err
	}
	dropOdTbStmt, err := tx.Prepare(dropOrdersSql)
	if err != nil {
		return err
	}
	_, err = dropOdTbStmt.Exec()
	if err != nil {
		return err
	}
	createPbTbStmt, err := tx.Prepare(createPublisherSql)
	if err != nil {
		return err
	}
	_, err = createPbTbStmt.Exec()
	if err != nil {
		return err
	}
	createOdTbStmt, err := tx.Prepare(createOrdersSql)
	if err != nil {
		return err
	}
	_, err = createOdTbStmt.Exec()
	if err != nil {
		return err
	}
	insertPbStmt, err := tx.Prepare(insertPublisherSql)
	if err != nil {
		return err
	}
	for _, pb := range publisherSlice {
		_, err = insertPbStmt.Exec(pb.id, pb.name, pb.nation)
		if err != nil {
			return err
		}
	}
	insertOdStmt, err := tx.Prepare(insertOrdersSql)
	if err != nil {
		return err
	}
	for _, od := range ordersSlice {
		_, err = insertOdStmt.Exec(od.customerId, od.bookId, od.quantity)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
