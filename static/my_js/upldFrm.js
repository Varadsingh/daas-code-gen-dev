$(document).ready(function () {

	$("#upldBtn1").on('click', function (event) {
		event.preventDefault();
		
		var output = document.getElementById("output1");
		var user_val = output.textContent
		
		var matip = document.getElementById("matip1").value;
		var matport = document.getElementById("matport1").value;
		var matuser = document.getElementById("matuser1").value;
		var matpass = document.getElementById("matpass1").value;
		var matdb = document.getElementById("matdb1").value;
		
		var user_val_for_file = output.textContent.replace(new RegExp('\r?\n','g'), '%0D');
		output.textContent = ''
		Prism.highlightElement(output);
		
		var output = document.getElementById("output2");
		output.textContent = ''
		Prism.highlightElement(output);
		
		var form = $('#upldFrm1')[0];
		var data = new FormData(form);
		$("#upldBtn1").prop("disabled", true);

		$.ajax({
			type: "POST",
			enctype: 'multipart/form-data',
			url: "http://localhost:8080/uploadFile?user_val="+user_val_for_file+"&matip="+matip+"&matport="+matport+"&matuser="+matuser+"&matpass="+matpass+"&matdb="+matdb,
			data: data,
			dataType: 'json',
			processData: false,
			contentType: false,
			cache: false,
			timeout: 600000,
			success: function (response) {
				
				var result = user_val.replace("-- INSERT CODE HERE --", response.status);
				
				var output = document.getElementById("output1");
				output.textContent = result
				Prism.highlightElement(output);
				
				var output = document.getElementById("output2");
				output.textContent = response.status2
				Prism.highlightElement(output);

				var input_file = document.getElementById("file1");
	    		input_file.value = ''

				$("#upldBtn1").prop("disabled", false);
			},
			error: function (e) {
				console.log("ERROR : ", e);
				$("#upldBtn1").prop("disabled", false);
			}
		});
	});
});