/**
* @author morefree
*/
var _ = require('underscore')._;


function existy(x) {
	return x != null;
}

function truthy(x) {
	return (x !== false) && existy(x);
}

/** run f with given times instantly */
function loop(f, times, predicate) {
	// args for f
	var args = _.toArray(arguments).slice(3);
	for(var i = 0; i < times && predicate(); i++) 
		f.apply(null, args);
}

/** return a boosted function which loops f for given times */
function loopBoost(f, times) {
	return function() {
		var args = _.toArray(arguments);
		for(var i = 0; i < times; i++)
			f.apply(null, args);
	}
}


/**
* @param f ,  original function to call, f(arg1, arg2, ..., argN, cb),
*			  where cb has the form callback(err, res)
* @param times, max times for retry
* pass in all arguments of f behind these two params
*/
function retry(f, times) {
	var tried = 0,
		args = _.toArray(arguments).slice(2),
		cb = last(args),
		ncb = function(err, res) {
			if(!existy(err) || tried >= times - 1) 
				cb(err, res);
			else {
				tried ++;
				f.apply(null, setLast(args, ncb));
			}
		};
	
	f.apply(null, setLast(args, ncb));
}

/** immutable utils for basic collections */
function exceptLast(array) {
	return array.slice(0, array.length - 1);
}

function exceptFirst(array) {
	return array.slice(1);
}

function last(array) {
	return array[array.length - 1];
}

function first(array) {
	return array[0];
}

function setLast(array, newLast) {
	var arrayCopy = clone(array);
	arrayCopy[arrayCopy.length - 1] = newLast;
	return arrayCopy;
}

function clone(array) {
	return array.slice(0);
}

/** exports */
exports.retry = retry;
exports.loopBoost = loopBoost;
