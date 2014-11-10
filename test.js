var nutil = require('./functional_util.js'),
	retry = nutil.retry,
	loopBoost = nutil.loopBoost,
	Some = nutil.Some,
	None = nutil.None;

/* test retry */
function checkEven(num, cb) {
	console.log("I am running again");
	if(num % 2 == 0) 
		cb(null, true);
	else
		cb(new Error('not a even number'), false);
}


var cb = function(err, res) {
	if(err)
		console.log('Not even !');
	else
		console.log('Even !');
}


retry(checkEven, 5, 1, cb);
retry(checkEven, 5, 2, cb);


/* test loopBoost */
console.log('\n\ntest loopBoost');
loopBoost(checkEven, 3)(1, cb);


/** test Optional */
console.log('\n\ntest Optional');
console.log(new Some(100).orElse(1));
console.log(new None().orElse(1));