mysql -h 10.77.50.214 -uroot -pfoobar -P 23306 -e "
use ddb;
drop table Book, Customer, Orders, Publisher;
"
mysql -h 10.77.50.214 -uroot -pfoobar -P 33306 -e "
use ddb;
drop table Book, Customer, Orders, Publisher;
"
mysql -h 10.77.110.228 -uroot -pfoobar -P 23306 -e "
use ddb;
drop table Book, Orders, Publisher;
"
mysql -h 10.77.110.158 -uroot -pfoobar -P 23306 -e "
use ddb;
drop table Orders, Publisher;
"