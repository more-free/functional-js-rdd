var util = require('util'),
	_ = require('underscore')._,
	http = require('http'),
	futil = require('./functional_util.js'),
	fs = require('fs');

/**
* Transformations : map, filter, sort
* Actions : count, save, collect, reduce
*/


/* @param config  config file path in local file system */
function RDD (config) {
	// parent data set, typically contains file path and partition
	this.dataPartition = {};

	// in-memory data, can be any type. most likely it's an array or string
	this.data = [];   

	// pending transformations, to write to a log
	this.transformations = [];

	// finished transformations
	this.finishedTrans = [];
}

RDD.prototype.load = function(path) {
	this.dataPartition.url = path;
}

RDD.prototype.map = function(f) {
	this.transformations.push({ type : 'map', func : this.restoreFunc(f) });
}

RDD.prototype.flatMap = function(f) {
	this.transformations.push( {type : 'flatMap', func : this.restoreFunc(f) });
}

RDD.prototype.filter = function(f) {
	this.transformations.push({type : 'filter', func : this.restoreFunc(f) });
}

RDD.prototype.sort = function(f) {
	this.transformations.push({type : 'sort', func : this.restoreFunc(f) });
}

RDD.prototype.count = function(cb) {
	this.applyTransformations(cb);
}

RDD.prototype.collect = function(cb) {
	this.applyTransformations(cb);
}

/** private functions */
RDD.prototype.restoreFunc = function(f) {
	return eval('(' + f + ')');
}


RDD.prototype.applyTransformations = function(cb) {
	var obj = this;
	this.loadData(function() {
		obj.transformations.forEach(function(t) {
			obj.applyTransformation(t);
			obj.finishedTrans.push(t);
		});

		obj.transformations = [];
		cb(obj);
	});
}


RDD.prototype.loadData = function(cb) {
	var obj = this;
	if(this.data.length == 0) {
		fs.readFile(this.dataPartition.url, function (err, data) {
        	if (err) {
            	throw err;
        	}

        	obj.data.push(data.toString());
        	cb();
    	});
	} else {
		cb();
	}
}

RDD.prototype.applyTransformation = function(t) {
	switch(t.type) {
		case 'map' :
			this.applyMap(t.func);
			break;
		case 'filter' :
			this.applyFilter(t.func);
			break;
		case 'flatMap' :
			this.applyFlatMap(t.func);
			break;
		case 'sort' :
			this.applySort(t.func);
			break;
	}
}

RDD.prototype.applyMap = function(f) {
	this.data = this.data.map(f);
} 

RDD.prototype.applyFilter = function(f) {
	this.data = this.data.filter(f);
}

RDD.prototype.applyFlatMap = function(f) {
	console.log(this.data);
	this.data = futil.flatMap(this.data, f);
	console.log(this.data);
}

RDD.prototype.applySort = function(f) {

}

exports.RDD = RDD;

