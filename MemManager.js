var _ = require("underscore")._;
var LRU = require("lru-cache");
var memwatch = require('memwatch');

/** LRU based key-value in-memory storage, a naive implementation. */
function MemManager() {
	this.cache = new LRU();
}

/**
* @param value UTF8 string-only
*/
MemManager.prototype.put = function(key, value) {

}

MemManager.prototype.get = function(key) {

}

MemManager.prototype.size = function(key) {

}

MemManager.prototype.list = function() {

}

MemManager.prototype.sizeOfUsedMem = function() {

}

MemManager.prototype.sizeOfAvailableMem = function() {

}

MemManager.prototype.exists = function(key) {
	return this.get(key) == null;
}