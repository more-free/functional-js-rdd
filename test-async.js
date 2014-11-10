var async = require('async');

async.series(
[ 
function(cb) {
	console.log('1');
	cb(null, 1);
},

function(cb) {
	console.log('2');
	//cb(null, 2);
	cb(new Error('shit happened'));
},

function (cb) {
	console.log('3');
	cb(null, 3);
}
],

function (err, results) {
	if(err) console.log(err);
	else console.log(results);
}
);