var simpleHashPartition = function(key, dataPartition) {
	var pos = strHashCode(key.toString()) % dataPartition.length;
	if(pos < 0) pos += dataPartition.length;

	return dataPartition[pos];
}

// from http://stackoverflow.com/questions/7616461/generate-a-hash-from-string-in-javascript-jquery
var strHashCode = function(str) {
    return str.split("").reduce(function(a,b){a=((a<<5)-a)+b.charCodeAt(0);return a&a},0);               
}

exports.simpleHashPartition = simpleHashPartition;