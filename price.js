var mysql = require('mysql');
var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : '',
  database : 'bigdata'
});

let dayUnit = 1000*3600*24;

connection.connect();

connection.query("SELECT HOUSE_SRN, DATA_DATE, AVG_PRICE from House_price_data where DATEDIFF(DATA_DATE,'2013-12-31') > 0 AND AVG_PRICE > LOWEST_PRICE AND (AVG_PRICE < HIGHEST_PRICE OR HIGHEST_PRICE = 0)", function (error, results, fields) {
  if (error) throw error;
  console.log('The solution is: ', results[0]);
  // dateTrans('2014-1-1', results[0].DATA_DATE);
});
// dateTrans('2014-1-1', '2014-12-31');


connection.end();


function dateTrans(startTime, target){
    startTime = new Date(startTime);
    target = new Date(target);
    let tmp = Math.ceil((target-startTime)/dayUnit);
    console.log(tmp);
}
