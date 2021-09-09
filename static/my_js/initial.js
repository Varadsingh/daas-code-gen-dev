$(document).ready(function () {
	
	var output = document.getElementById("output1");
	output.textContent = "-- INSERT CODE HERE --"
	Prism.highlightElement(output);
	
	var output = document.getElementById("output2");
	output.textContent = ""
	Prism.highlightElement(output);
	
	// var batchrun = document.getElementById("batch-run");
	// var singlerun = document.getElementById("single-run");
	// var batchbtn = document.getElementById("batchBtn");
	// var singlebtn = document.getElementById("singleBtn");
	
	// batchrun.style.display = "block";
	// singlebtn.style.display = "block";
	
	// singlerun.style.display = "none";
	// batchbtn.style.display = "none";
	
	$("#copyBtn1").on('click', function (event) {		
		var output = document.getElementById("output1");
		navigator.clipboard.writeText(output.textContent);
		alert("Result Has Been Copied");
	});

	$("#copyBtn2").on('click', function (event) {
		var output = document.getElementById("output2");
		navigator.clipboard.writeText(output.textContent);
		alert("Result Has Been Copied");
	});
	
	$("#clearBtn1").on('click', function (event) {
		var output = document.getElementById("output1");
		output.textContent = "-- INSERT CODE HERE --"
		Prism.highlightElement(output);
	});

	$("#clearBtn2").on('click', function (event) {
		var output = document.getElementById("output2");
		output.textContent = ""
		Prism.highlightElement(output);
	});
	
	// $("#batchBtn").on('click', function (event) {
		// var batchrun = document.getElementById("batch-run");
		// var singlerun = document.getElementById("single-run");
		// var batchbtn = document.getElementById("batchBtn");
		// var singlebtn = document.getElementById("singleBtn");
		
		// batchrun.style.display = "block";
		// singlebtn.style.display = "block";
		
		// singlerun.style.display = "none";
		// batchbtn.style.display = "none";
		
	// });
	
	// $("#singleBtn").on('click', function (event) {
		// var batchrun = document.getElementById("batch-run");
		// var singlerun = document.getElementById("single-run");
		// var batchbtn = document.getElementById("batchBtn");
		// var singlebtn = document.getElementById("singleBtn");
		
		// singlerun.style.display = "block";
		// batchbtn.style.display = "block";
		
		// batchrun.style.display = "none";
		// singlebtn.style.display = "none";
		
	// });


});