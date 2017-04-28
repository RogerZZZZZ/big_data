var mysql = require('mysql');
var fs = require('fs');

var connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : '',
  database : 'bigdata'
});
var startTime = new Date();
let minuteUnit = 1000;
connection.connect();

connection.query("SELECT info.HOUSE_SRN, A.Id as Area_type, type.id as PPTY_TYPE, s.id as STATUS_TYPE, info.OWNERSHIP_PERIOD as Ownership from House_info_data as info, House_status as s, house_type as type, Area_type as A where info.PPTY_MGMT_TYPE = type.PPTY_MGMT_ AND info.HOUSE_STATUS = s.HOUSE_STAT AND info.AREA = A.Area_type ", function (error, results, fields) {
    if (error) throw error;
    console.log('The solution is: ', results.length);
    let len = results.length;
    console.log(results[0].Ownership);
    for(let i = 0; i < len; i++){
        let os = results[i].Ownership;
        if(os !== null && os !== ""){
            results[i].Ownership = judgeOwnership(os);
        }else{
            results[i].Ownership = 0;
        }
    }
    fs.writeFile("./res.json", JSON.stringify(results), function(err) {
        if(err) {
            return console.log(err);
        }
        let endTime = new Date();
        let duration = parseInt((endTime-startTime)/minuteUnit)
        console.log(duration + 'seconds');
    });
});

connection.end();

function judgeOwnership(owner){
    let tmp = 0, type = 0;
    let reg = /\d+/g;
    //filter the occasion that '70(1992-9-23)年', '70（2006-10-16至2076-10-15）年'. we only wany the number 70
    owner = owner.split('(')[0];
    owner = owner.split('（')[0];
    let res = owner.match(reg);
    // console.log(res);
    if(res === null) return type;
    let arrLen = res.length;
    if(arrLen === 0) return type;
    let min = parseInt(res[0]);
    if(arrLen === 1 ){
        //filter the number that represent the year
        if(min<100){
            let n = parseInt(min);
            return typeCalculator(n);
        }else{
            return type;
        }
    }
    for(let i = 1; i < arrLen; i++){
        let a = parseInt(res[i]);
        if(a >= 100) continue;
        if(a < min) min = a;
    }
    return typeCalculator(min);
}

function typeCalculator(num){
    //type: 0: null,  1: <= 50,  2: >50 <= 70， 3: >70
    //we set num should be bigger than 10.Because there are some data like 9号住宅楼:70，办公楼1-7号楼50年. We should do the filtering.
    if(num <= 50 && num > 10){
        return 1;
    }else if(num > 50 && num <= 70){
        return 2;
    }else if(num > 70){
        return 3;
    }else{
        return 0;
    }
}
