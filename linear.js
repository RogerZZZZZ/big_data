var mysql = require('mysql');
var moment = require('moment');
var fs = require('fs');
var outliers = require('outliers')

var debug = false;

let cityDistrict = [
    [1, 16],//beijing
    [17, 31],//shanghai
    [32, 41],//guangzhou
    [42, 49]//shenzhen
];

let houseType = [
    [2, 4, 13, 15, 21],//residential building
    [3, 7],//villa
    [6, 12, 14, 16, 19, 20],//shop
    [18],//office building
    [5, 8, 9, 10, 11, 17]//apartment
]

let startTime = new Date();
var connection = mysql.createConnection({host: 'localhost', user: 'root', password: '', database: 'bigdata'});
connection.connect();

let areaSize = cityDistrict.length;
let typeSize = houseType.length;

let startDate = new Date('2013-9-1');
let startMonth = startDate.getMonth();
let startYear = startDate.getYear();
let beginTime = new Date('2013-9-1');

var innerDataClean = (data, areaType, houseType) => {
    let query = 'SELECT Date, House_id, Price from combine where' ;
    let [q, p] = filter(data, query);

    connection.query(q + ' ORDER BY Date', p, function(error, results, fields) {
        if (error)
            throw error;

        if(data.outlier == 'true' || data.outlier === true){
            results = results.filter(outliers('Price'));
        }
        console.log(results.length, p);
        let len = results.length;
        let queryEndTime = new Date();
        log('Time spending on querying:' + (queryEndTime - startTime) / (1000) + 'seconds');

        //calculate the max time difference
        if(debug){
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
            }
            log(max / (1000 * 3600 * 24));
            let endTime = new Date();
            log(tmpdt1);
            log(tmpdt2);
            log("------------------");
        }

        let dataIndex = 0;
        let res = [];

        for (let i = 1; i < len; i++){
            let tmpData = results[i];
            let tmpDate = new Date(tmpData.Date);
            let dateIndex;
            let tmpMonth = tmpDate.getMonth(),
                tmpYear = tmpDate.getYear();
            if(tmpYear === startYear){
                dateIndex = tmpMonth - startMonth;
            }else{
                dateIndex = (tmpYear - startYear - 1)*12 + tmpMonth + 12 - startMonth;
            }
            let tmp = {
                date: dateIndex,
                price: tmpData.Price
            }
            res.push(tmp);
        }

        //write to the file
        let writeStr = '';
        for(let item in res){
            writeStr += res[item].date + ' ' + res[item].price;
            if(item !== len - 1){
                writeStr += '\n';
            }
        }

        fs.writeFile("./linear/"+areaType+"-"+houseType+".csv", writeStr, function(err) {
            if(err) {
                return log(err);
            }
        });
    });

}

for(let k = 1; k <= 49; k++){
    for(let o = 0; o < typeSize; o++){
        let data = {
            outlier: "true",
            area: k,
            ppty: o,
            period: 5
        }
        innerDataClean(data, k, o);
    }
}

function log(obj){
    if(debug) console.log(obj);
}

function dateTrans(startTime, target) {
    startTime = new Date(startTime);
    target = new Date(target);
    let tmp = Math.ceil((target - startTime) / dayUnit);
    console.log(tmp);
}

function filter(data, query) {
    let para = [];
    if(data.area !== undefined){
        if(data.all){
            let startIndex =  cityDistrict[data.city][0];
            let endIndex = cityDistrict[data.city][1];
            for(let i = startIndex; i <= endIndex; i++){
                if(i === startIndex){
                    query += ' (`Area_type` = ?'
                }else{
                    query += ' OR `Area_type` = ?';
                }
                para.push(i);
            }
            query += ')'
        }else{
            query += ' `Area_type` = ?';
            para.push(data.area);
        }
    }
    if (data.ppty !== undefined) {
        let type = houseType[data.ppty];
        for(let i = 0; i < type.length; i++){
            if(i === 0){
                query += ' And ( `PPTY_type` = ?';
            }else{
                query += ' OR `PPTY_type` = ?';
            }
            para.push(type[i])
        }
        query += ')'
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
