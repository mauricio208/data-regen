const fs = require('fs');
const parse = require('csv-parser');


function readCSVFile(fileName){
  return new Promise((resolve) => {
    let rows = [];
    fs.createReadStream(fileName)
      .pipe(parse())
      .on('data', (data) => rows.push(data))
      .on('end', () => {
        console.log('CSV file successfully processed');
        resolve(rows)
      });
  });
}


module.exports = readCSVFile