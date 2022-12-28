mysql -h 10.77.50.214 -uroot -pfoobar -P 13306 -e "
use ddb;
CREATE TABLE IF NOT EXISTS Book(id INT,title CHAR(100),authors CHAR(200),publisher_id INT,copies INT,PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Customer(id INT,name CHAR(25),\`rank\` INT,PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Orders(customer_id INT,book_id INT,quantity INT)ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Publisher(id INT,name CHAR(100),nation CHAR(3),PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
set global local_infile=on;
LOAD DATA LOCAL INFILE './book.tsv' INTO TABLE ddb.Book;
LOAD DATA LOCAL INFILE './customer.tsv' INTO TABLE ddb.Customer;
LOAD DATA LOCAL INFILE './orders.tsv' INTO TABLE ddb.Orders;
LOAD DATA LOCAL INFILE './publisher.tsv' INTO TABLE ddb.Publisher;
"