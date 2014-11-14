var _ = require("underscore")._;
var futil = require('./functional_util.js');

var array = ["some thing", " more things ", " oh my god"];

var newArray = futil.flatMap(array, function(s) {
	return s.split(/\s+/);
} );

console.log(newArray);