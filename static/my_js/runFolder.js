$(document).ready(function() {
    var queryString = new Array();
    if (queryString.length == 0) {
        if (window.location.search.split('?').length > 1) {
            var params = window.location.search.split('?')[1].split('&');
            for (var i = 0; i < params.length; i++) {
                var key = params[i].split('=')[0];
                var value = decodeURIComponent(params[i].split('=')[1]);
                if (value == "batch-run") {
                    var matip = document.getElementById("matip").value;
                    var matport = document.getElementById("matport").value;
                    var matuser = document.getElementById("matuser").value;
                    var matpass = document.getElementById("matpass").value;
                    var matdb = document.getElementById("matdb").value;
                    var foldname = document.getElementById("foldname").value;
                    var bridge = document.getElementById("bridge").value;
                    var kafka = document.getElementById("kafka").value;
                    var materialize = document.getElementById("materialize").value;
                    var sandbox = document.getElementById("sandbox").value;

                    var fd = new FormData();

                    fd.append('matip', matip);
                    fd.append('matport', matport);
                    fd.append('matuser', matuser);
                    fd.append('matpass', matpass);
                    fd.append('matdb', matdb);
                    fd.append('foldname', foldname);
                    fd.append('bridge', bridge);
                    fd.append('kafka', kafka);
                    fd.append('materialize', materialize);
                    fd.append('sandbox', sandbox);

                    $("#upldBtn2").prop("disabled", true);

                    $.ajax({
                        type: "POST",
                        enctype: 'multipart/form-data',
                        url: "http://localhost:8080/runFolder",
                        data: fd,
                        dataType: 'json',
                        processData: false,
                        contentType: false,
                        cache: false,
                        timeout: 600000,
                        success: function(response) {
                            alert(response.status);
                            $("#upldBtn2").prop("disabled", false);
                        },
                        error: function(e) {
                            console.log("ERROR : ", e);
                            $("#upldBtn2").prop("disabled", false);
                        }
                    });

                }
            }
        }
    }

    $("#upldBtn2").on('click', function(event) {
        event.preventDefault();

        // var form = $('#upldFrm2')[0];
        var matip = document.getElementById("matip").value;
        var matport = document.getElementById("matport").value;
        var matuser = document.getElementById("matuser").value;
        var matpass = document.getElementById("matpass").value;
        var matdb = document.getElementById("matdb").value;
        var foldname = document.getElementById("foldname").value;
        var bridge = document.getElementById("bridge").value;
        var kafka = document.getElementById("kafka").value;
        var materialize = document.getElementById("materialize").value;
        var sandbox = document.getElementById("sandbox").value;
		var run_status = document.getElementById("run_status");
		
		run_status.innerHTML = "<pre>" + " -- processing started -- " + "</pre>"; // response.status
		
        var fd = new FormData();

        fd.append('matip', matip);
        fd.append('matport', matport);
        fd.append('matuser', matuser);
        fd.append('matpass', matpass);
        fd.append('matdb', matdb);
        fd.append('foldname', foldname);
        fd.append('bridge', bridge);
        fd.append('kafka', kafka);
        fd.append('materialize', materialize);
        fd.append('sandbox', sandbox);


        $("#upldBtn2").prop("disabled", true);
		
		var running_batch = 1
        $.ajax({
            type: "POST",
            enctype: 'multipart/form-data',
            url: "http://localhost:8080/runFolder",
            data: fd,
            dataType: 'json',
            processData: false,
            contentType: false,
            cache: false,
            timeout: 600000,
            success: function(response) {
                // alert(response.status);
				running_batch = -1;
                $("#upldBtn2").prop("disabled", false);
            },
            error: function(e) {
                console.log("ERROR : ", e);
				running_batch = -1;
                $("#upldBtn2").prop("disabled", false);
            }
        });
		console.log(running_batch);
		
		var processed_file = 0;
		var total_files = 0;
		
		while ( ( processed_file != total_files ) || ( processed_file == 0 && total_files == 0 )) {
			$.ajax({
				type: "POST",
				enctype: 'multipart/form-data',
				url: "http://localhost:8080/runStatus",
				async: false,
				data: fd,
				dataType: 'json',
				processData: false,
				contentType: false,
				cache: false,
				timeout: 600000,
				success: function(response) {
					var myArr = response.status_count.split("/");
					processed_file = myArr[0];
					total_files = myArr[1];
					console.log(processed_file);
					console.log(total_files);
					console.log(response.status);
					run_status.innerHTML = "<pre>" + response.status + "</pre>"; // response.status
				},
				error: function(e) {
					console.log("ERROR : ", e);
					run_status.innerText = e
				}
			});
		}
		
		if (processed_file == total_files ){
			run_status.innerHTML = run_status.innerHTML + "<pre>" + " processing completed " + "</pre>"
		}

    });
});