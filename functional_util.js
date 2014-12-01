/**
* @author morefree
*/
var _ = require('underscore')._,
	util = require('util'),
	stream = require('stream');


function existy(x) {
	return x != null;
}

function truthy(x) {
	return (x !== false) && existy(x);
}

function loop(f, times, predicate) {
	// args for f
	var args = _.toArray(arguments).slice(3);
	for(var i = 0; i < times && predicate(); i++) 
		f.apply(null, args);
}

/** boost a function f to a looped one */
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
* @param ... pass in all arguments of f behind these f and times
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

/** functional wrappers */
function Optional() {

}

Optional.prototype.get = function() { 
	if(existy(this.value)) 
		return this.value;
	else
		throw "NoSuchElement";
}

Optional.prototype.orElse = function(initial) {
	if(existy(this.value))
		return this.value;
	else 
		return initial;
}


function Some(any) {
	this.value = any;
}
util.inherits(Some, Optional);


function None() {

}
util.inherits(None, Optional);


/** some high-order functions missing in underscore.js */
function flatMap(array, f) {
	return _.flatten(array.map(f));
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

/** 
* some other utils that might be moved to separate modules eventually 
* http://strongloop.com/strongblog/practical-examples-of-the-new-node-js-streams-api/
*/
function Liner() {
	var liner = new stream.Transform( { objectMode: true } );

	liner._transform = function (chunk, encoding, done) {
     	var data = chunk.toString()
     	if (this._lastLineData) data = this._lastLineData + data
 
     	var lines = data.split('\n')
     	this._lastLineData = lines.splice(lines.length - 1, 1)[0]
 
     	lines.forEach(this.push.bind(this))
     	done()
	}
 
	liner._flush = function (done) {
     	if (this._lastLineData) this.push(this._lastLineData)
     	this._lastLineData = null
     	done()
	}

	return liner;
}
 



/** exports */
exports.retry = retry;
exports.loopBoost = loopBoost;
exports.Optional = Optional;
exports.Some = Some;
exports.None = None;
exports.flatMap = flatMap;
exports.existy = existy;
exports.truthy = truthy;
exports.setLast = setLast;
exports.Liner = Liner;
