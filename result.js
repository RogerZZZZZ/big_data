var mysql = require('mysql');
var moment = require('moment');
var fs = require('fs');
var outliers = require('outliers')

// var connection = mysql.createConnection({
//   host     : 'www.jlzhang.cn',
//   user     : 'root',
//   password : '140283899',
//   database : 'big_data_test'
// });

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

let dataClean = (connection, req, res, next) => {
    console.log(++global.count);
    let query = req.query,
        outlier = query.outlier,
        area = query.area,
        ppty = query.ppty,
        period = query.period;
    let data = {
        outlier: outlier,
        area: area,
        ppty: ppty,
        period: period
    }
    innerDataClean(connection, data, (data) => {res.json(data)});
}

let innerDataClean = (connection, data, returnFunc) => {

    let query = 'SELECT Date, House_id, Price from combine where' ;
    let [q, p] = filter(data, query);
    log(q);
    log(p);

    connection.query(q + ' ORDER BY Date', p, function(error, results, fields) {
        if (error)
            throw error;

        if(data.outlier === 'true'){
            // console.log("obj");
            results = results.filter(outliers('Price'));
        }
        console.log(results.length);
        let len = results.length;
        let beginTime = new Date('2013-12-31');
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
        let period = parseInt(data.period);
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

        let lastDataIndex;

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
                        // res[after] = res[i-1];
                        lastDataIndex = i;
                        lastDate = moment(beginTime).add(i*period, 'days');
                        break;
                    }
                    let k = (res[after]-res[i-1])/c;
                    for(let j = 0; j < c; j++){
                        res[i+j] = res[i-1] + j * k;
                    }
                }
            }
        }

        res = res.splice(0,lastDataIndex)
        log(res);
        log(resLen);
        let jsonResult = {
            data: res,
            endTime: lastDate
        };
        returnFunc(jsonResult);

        //write to the file
        // let writeStr = '';
        // for(let item in res){
        //     writeStr += res[item];
        //     if(item !== resLen - 1){
        //         writeStr += '\n';
        //     }
        // }
        //
        // fs.writeFile("./out.csv", writeStr, function(err) {
        //     if(err) {
        //         return log(err);
        //     }
        //     console.log('Time spending on handling data:' + (endTime - queryEndTime) / (1000) + 'seconds');
        // });
    });

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

exports.dataClean = dataClean;
