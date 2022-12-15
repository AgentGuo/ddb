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
)

const (
	insertPublisherSql = `INSERT INTO Publisher VALUES(?, ?, ?);`
	insertCustomerSql  = `INSERT INTO Customer VALUES(?, ?);`
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
	err := site1Init()
	if err != nil {
		panic(err)
	}
	err = site2Init()
	if err != nil {
		panic(err)
	}
	err = site3Init()
	if err != nil {
		panic(err)
	}
	err = site4Init()
	if err != nil {
		panic(err)
	}
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
	customerId, quantity int
	bookId               string
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
	fmt.Printf("pulisherSlice.size = %d\n", len(publisherSlice))
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
	fmt.Printf("customerSlice.size = %d\n", len(customerSlice))
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
	fmt.Printf("pulisherSlice.size = %d\n", len(publisherSlice))
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
	fmt.Printf("customerSlice.size = %d\n", len(customerSlice))
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
	fmt.Printf("pulisherSlice.size = %d\n", len(publisherSlice))
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
	createPbTbStmt, err := tx.Prepare(createPublisherSql)
	if err != nil {
		return err
	}
	_, err = createPbTbStmt.Exec()
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
	fmt.Printf("pulisherSlice.size = %d\n", len(publisherSlice))
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
	createPbTbStmt, err := tx.Prepare(createPublisherSql)
	if err != nil {
		return err
	}
	_, err = createPbTbStmt.Exec()
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
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
