var http = require('http');


function foo(arg) {
	return arg * 1000;
}

/**
* @param Function
* @return Function
*/
function closureCleaner(f) {
	return f;
} 

console.log(foo(30));
var post_data = closureCleaner(foo).toString();

function when_done(str) {
	console.log("res : \n" + str);

	try {
		var f = eval('(' + str + ')');
		console.log(f(9));
	} catch(err) {
		console.log(err);
	}
}

// An object of options to indicate where to post to
var post_options = {
      host: 'localhost',
      port: '3000',
      path: '/',
      method: 'POST',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': post_data.length
      }
};

// Set up the request
var post_req = http.request(post_options, function(res) {
	var str = '';
    res.setEncoding('utf8');
    res.on('data', function (chunk) {
        str += chunk;
    });
    res.on('end', function(){
    	when_done(str);
    });
});

// post the data
post_req.write(post_data);
post_req.end();