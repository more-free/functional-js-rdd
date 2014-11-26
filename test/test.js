var expect = require("chai").expect;

describe("some suite", function(){
	before(function() {
		console.log("before, fuck you");
	});

	after(function() {
		console.log("after, fuck you again");
	});


	describe("chilren suite", function() { 

	it("test case 1", function() {
		var t = "ss";
		expect(typeof t).to.be.a('string');	
	});

	it("nothing", function() {
			
	});

	});
})
