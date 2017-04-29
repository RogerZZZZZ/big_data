var mysql = require('mysql');
var moment = require('moment');
var fs = require('fs');
var outliers = require('outliers')
// var MyStream = require('json2csv-stream');

// create the transform stream
// var parser = new MyStream();

// var connection = mysql.createConnection({
//   host     : 'www.jlzhang.cn',
//   user     : 'root',
//   password : '140283899',
//   database : 'big_data_test'
// });

var connection = mysql.createConnection({host: 'localhost', user: 'root', password: '', database: 'bigdata'});

connection.connect();

let testData = {
    area: 2,
    // ppty: 4,
    // status: 1,
    // owner: 1
}

let query = 'SELECT Date, House_id, Price from combine where (`Area_type` = 1';
for(let i = 2; i <= 16; i++){
    query += ' OR `Area_type` = ' + i;
}

query += ') And (`PPTY_Type` = 2 OR `PPTY_Type` = 4 OR `PPTY_Type` = 13)';

// let query = 'SELECT Date, House_id, Price from combine where `Area_type` = ?';

let [q,
    p] = filter(testData, query);
let startTime = new Date();

console.log(q, p);

// connection.query(q + ' ORDER BY Date', p, function(error, results, fields) {
    connection.query(q + ' ORDER BY Date', function(error, results, fields) {
    if (error)
        throw error;

    // console.log('The solution is: ', results);
    // dateTrans('2014-1-1', results[0].DATA_DATE);
    console.log(results.length);
    results = results.filter(outliers('Price'));
    console.log(results.length);
    let len = results.length;
    let beginTime = new Date('2013-12-31');
    let queryEndTime = new Date();
    console.log('Time spending on querying:' + (queryEndTime - startTime) / (1000) + 'seconds');

    //calculate the max time difference
    let date1 = new Date(results[0].Date),
        date2 = new Date(results[1].Date);
    let max = date2 - date1;
    let tmpdt1 = results[0],
        tmpdt2 = results[1];
    for (let i = 2; i < len; i++) {
        date1 = new Date(results[i - 1].Date);
        date2 = new Date(results[i].Date);
        let tmp = date2 - date1;
        if (max < tmp) {
            max = tmp;
            tmpdt1 = results[i - 1];
            tmpdt2 = results[i];
        }
        //   max = max < tmp ? tmp: max;
    }
    // console.log(max);
    console.log(max / (1000 * 3600 * 24));
    let endTime = new Date();
    console.log(tmpdt1);
    console.log(tmpdt2);
    console.log("------------------");

    let dataIndex = 0;
    let period = 5;
    let resLen = Math.ceil(3*365 / period);
    let res = [];

    for (let i = 1; i < resLen; i++){
        let tmp = 0;
        let count = 0;
        let et = moment(beginTime).add(i*period, 'days');
        for(let j = dataIndex; j < len; j++){
            let tmpData = results[j];
            if(moment(tmpData.Date).isBefore(et)){
                count++;
                tmp += tmpData.Price;
            }else{
                break;
            }
        }
        let tmpRes = count !== 0 ? tmp/count: 0;
        res.push(tmpRes);
        dataIndex += count;
    }

    for(let i = 0; i < resLen; i++){
        if(res[i] === 0){
            if(i === 0){
                let t = i;
                while(res[++t] === 0);
                res[0] = res[t];
            }else{
                let after = i;
                let c = 1;
                while(res[++after] === 0){
                    c++;
                }
                if(after >= resLen - 1){
                    res[after] = res[i-1];
                }
                let k = (res[after]-res[i-1])/c;
                for(let j = 0; j < c; j++){
                    res[i+j] = res[i-1] + j * k;
                }
            }
        }
    }

    console.log(res);
    console.log(resLen);

    let writeStr = '';
    for(let item in res){
        writeStr += res[item];
        if(item !== resLen - 1){
            writeStr += '\n';
        }
    }

    fs.writeFile("./out.csv", writeStr, function(err) {
        if(err) {
            return console.log(err);
        }
        console.log('Time spending on handling data:' + (endTime - queryEndTime) / (1000) + 'seconds');
    });

});

connection.end();

function dateTrans(startTime, target) {
    startTime = new Date(startTime);
    target = new Date(target);
    let tmp = Math.ceil((target - startTime) / dayUnit);
    console.log(tmp);
}

function filter(data, query) {
    let para = [data.area];
    if (data.ppty !== undefined) {
        query += ' And `PPTY_type` = ?';
        para.push(data.ppty);
    }
    if (data.status !== undefined) {
        query += ' And `STATUS_TYPE` = ?';
        para.push(data.status);
    }
    if (data.owner !== undefined) {
        query += ' And `Ownership` = ?';
        para.push(data.owner);
    }

    return [query, para];
}
