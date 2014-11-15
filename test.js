var fs = require('fs');

var file = fs.readFileSync('./somefile');

console.log(typeof file);
console.log(file.toString());