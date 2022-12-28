mysql -h 10.77.50.214 -uroot -pfoobar -P 23306 -e "
use ddb;
CREATE TABLE IF NOT EXISTS Book(id INT,title CHAR(100),authors CHAR(200),publisher_id INT,copies INT,PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Customer(id INT,name CHAR(25),PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Orders(customer_id INT,book_id INT,quantity INT)ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Publisher(id INT,name CHAR(100),nation CHAR(3),PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
set global local_infile=on;
LOAD DATA LOCAL INFILE './site1/Book.tsv' INTO TABLE ddb.Book;
LOAD DATA LOCAL INFILE './site1/Customer.tsv' INTO TABLE ddb.Customer;
LOAD DATA LOCAL INFILE './site1/Orders.tsv' INTO TABLE ddb.Orders;
LOAD DATA LOCAL INFILE './site1/Publisher.tsv' INTO TABLE ddb.Publisher;
"
mysql -h 10.77.50.214 -uroot -pfoobar -P 33306 -e "
use ddb;
CREATE TABLE IF NOT EXISTS Book(id INT,title CHAR(100),authors CHAR(200),publisher_id INT,copies INT,PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Customer(id INT,\`rank\` INT,PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Orders(customer_id INT,book_id INT,quantity INT)ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Publisher(id INT,name CHAR(100),nation CHAR(3),PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
set global local_infile=on;
LOAD DATA LOCAL INFILE './site2/Book.tsv' INTO TABLE ddb.Book;
LOAD DATA LOCAL INFILE './site2/Customer.tsv' INTO TABLE ddb.Customer;
LOAD DATA LOCAL INFILE './site2/Orders.tsv' INTO TABLE ddb.Orders;
LOAD DATA LOCAL INFILE './site2/Publisher.tsv' INTO TABLE ddb.Publisher;
"
mysql -h 10.77.110.228 -uroot -pfoobar -P 23306 -e "
use ddb;
CREATE TABLE IF NOT EXISTS Book(id INT,title CHAR(100),authors CHAR(200),publisher_id INT,copies INT,PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Orders(customer_id INT,book_id INT,quantity INT)ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Publisher(id INT,name CHAR(100),nation CHAR(3),PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
set global local_infile=on;
LOAD DATA LOCAL INFILE './site3/Book.tsv' INTO TABLE ddb.Book;
LOAD DATA LOCAL INFILE './site3/Orders.tsv' INTO TABLE ddb.Orders;
LOAD DATA LOCAL INFILE './site3/Publisher.tsv' INTO TABLE ddb.Publisher;
"
mysql -h 10.77.110.158 -uroot -pfoobar -P 23306 -e "
use ddb;
CREATE TABLE IF NOT EXISTS Orders(customer_id INT,book_id INT,quantity INT)ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE IF NOT EXISTS Publisher(id INT,name CHAR(100),nation CHAR(3),PRIMARY KEY(id))ENGINE=InnoDB DEFAULT CHARSET=utf8;
set global local_infile=on;
LOAD DATA LOCAL INFILE './site4/Orders.tsv' INTO TABLE ddb.Orders;
LOAD DATA LOCAL INFILE './site4/Publisher.tsv' INTO TABLE ddb.Publisher;
"