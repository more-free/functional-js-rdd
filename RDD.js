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
	// parent data set
	this.dataPartition = { 
		url : "some file path", 
		lines : { from : 10, to : 20 }
	};

	// in-memory data, can be any type. most likely it's an array or string
	this.data = '';   

	// pending transformations, to write to a log
	this.transformations = [
		{
		 type : 'map', 
		 func : function (t) { return t.toUpperCase(); } 
		},

		{
		 type : 'filter',
		 func : function (t) { return t[0] === 'A'; }
		}
	}];

	// finished transformations
	this.finishedTrans = [];
}

RDD.prototype.fromText = function(path) {
	this.dataPartition.url = path;
}

RDD.prototype.map = function(f) {
	this.transformations.push({ type : 'map', func : f.toString() });
}

RDD.prototype.flatMap = function(f) {
	this.transformations.push( {type : 'flatMap', func : f.toString() });
}

RDD.prototype.filter = function(f) {
	this.transformations.push({type : 'filter', func : f.toString()});
}

RDD.prototype.sort = function(f) {
	this.transformations.push({type : 'sort', func : f.toString() });
}

RDD.prototype.count = function() {

}

RDD.prototype.collect = function() {

}

/** private functions */
RDD.prototype.applyTransformations = function() {
	this.loadData(function() {
		this.transformations.forEach(function(t) {
			this.applyTransformation(t);
			this.finishedTrans.push(t);
		});

		this.transformations = [];
	});
}

RDD.prototype.loadData = function(cb) {
	if(!futil.existy(this.data)) {
		fs.readFile(this.dataPartition.url, function (err, data) {
        	if (err) {
            	throw err;
        	}

        	this.data = data;
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

} 

RDD.prototype.applyFilter = function(f) {

}

RDD.prototype.applyFlatMap = function(f) {

}

RDD.prototype.applySort = function(f) {

}



