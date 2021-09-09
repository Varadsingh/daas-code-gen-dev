$(document).ready(function () {
	
	var bridgeip_elem = document.getElementById("bridgeip");
	var bridgeport_elem = document.getElementById("bridgeport");
	var bridgeuser_elem = document.getElementById("bridgeuser");
	var bridgepass_elem = document.getElementById("bridgepass");
	var bridgedb_elem = document.getElementById("bridgedb");
	var operation_elem = document.getElementById("operation");
	var table_elem = document.getElementById("table");
	var reftable_elem = document.getElementById("reftable");
	var op_type_elem = document.getElementById('op_type');
	var drpdata = [];
	
	var fd = new FormData();

	var table_columns_div = document.getElementById("table_columns_div");
	var ref_table_columns_div = document.getElementById("ref_table_columns_div");
	var table_names = document.getElementById("table_names");
	var ref_table_names = document.getElementById("ref_table_names");
	var loader = document.getElementById("loader");
	var sql_ops = document.getElementById("sql_ops");
	
	
	table_columns_div.style.display = "none";
	ref_table_columns_div.style.display = "none";
	table_names.style.display = "none";
	ref_table_names.style.display = "none";
	loader.style.display = "none";
	sql_ops.style.display = "block";
	
	function dbDetails() {
		bridgeip = bridgeip_elem.value;
		bridgeport = bridgeport_elem.value
		bridgeuser = bridgeuser_elem.value
		bridgepass = bridgepass_elem.value
		bridgedb = bridgedb_elem.value
		operation = operation_elem.value
		table = table_elem.value
		reftable = reftable_elem.value
		
		fd = new FormData();
		
		fd.append( 'bridgeip', bridgeip );
		fd.append( 'bridgeport', bridgeport );
		fd.append( 'bridgeuser', bridgeuser );
		fd.append( 'bridgepass', bridgepass );
		fd.append( 'bridgedb', bridgedb );
		fd.append( 'operation', operation );
		
	}

	$("#op_submit").on('click', function (event) {
		event.preventDefault();
		dbDetails();

		var selected_table_columns = $('#table_columns').select2('data');
		var col_names = [];
		for (x in selected_table_columns){
			col_names.push(selected_table_columns[x]['text']);
		}
		
		if (operation == 'create_primary_key'){
			var op_name = table+'_pkey';			
		}
		
		if (operation == 'create_foreign_key'){
			var t = document.getElementById("fk");
			var op_name = t.value;

			var selected_ref_table_columns = $('#ref_table_columns').select2('data');
			var ref_col_names = [];
			for (x in selected_ref_table_columns){
				ref_col_names.push(selected_ref_table_columns[x]['text']);
			}
			
			fd.append( 'ref_table_name', reftable );
			fd.append( 'ref_col_names', ref_col_names );
		}
		
		fd.append( 'table', table );
		fd.append( 'col_names', col_names );
		fd.append( 'op_name', op_name );
		
		loader.style.display = "block";
		sql_ops.style.display = "none";
		
		$.ajax({
			url: "http://localhost:8080/runOp",
			data: fd,
			processData: false,
			contentType: false,
  			type: 'POST',
			timeout: 600000,
			success: function (response) {
				loader.style.display = "none";
				sql_ops.style.display = "block";
				alert(response.status);
			},
			error: function (e) {
				console.log("ERROR : ", e);
			}
		});
		
	});
	
	$("#table_operations").on('change', function (event) {
		event.preventDefault();
		
		if (table_columns_div.style.display == "block"){
			table_columns_div.style.display = "none";
		}
		
		if (ref_table_columns_div.style.display == "block"){
			ref_table_columns_div.style.display = "none";
		}
	
		dbDetails();
		$("#table_columns").html("");
		$("#ref_table_columns").html("");
	
		loader.style.display = "block";
		sql_ops.style.display = "none";
		
		$.ajax({
			url: "http://localhost:8080/getTables",
			data: fd,
			processData: false,
			contentType: false,
  			type: 'POST',
			timeout: 600000,
			success: function (response) {
				table_elem.innerHTML = ''
				reftable_elem.innerHTML = ''
				
				var opt1 = '<option value="" default selected disabled>Select your option</option>'
				$("#table").append(opt1);
				$("#reftable").append(opt1);
				
				for (x in response.rows) {
					var tab_name = response.rows[x][0];
					var opt = "<option value=" + tab_name + ">" + tab_name + "</option>"
					$("#table").append(opt);
					$("#reftable").append(opt);
				}
				loader.style.display = "none";
				sql_ops.style.display = "block";
			},
			error: function (e) {
				console.log("ERROR : ", e);
			}
		});
		
		table_names.style.display = "block";
		
		var fk_name = '<label for="fk" class="item-row">FK Name:</label><input id="fk" type="text" class="item-row">';
		
		if (operation == 'create_primary_key'){
			op_type_elem.innerHTML = '';
			if (ref_table_names.style.display == "block"){
				ref_table_names.style.display = "none";
			}
		}
		
		if (operation == 'create_foreign_key'){
			$('#op_type').append(fk_name);
			ref_table_names.style.display = "block";
		}
		
	});
	
	$("#table").on('change', function (event) {
		event.preventDefault();
		drpdata = [];
		
		dbDetails();
		$("#table_columns").html("");
		
		fd.append( 'table', table );
		
		loader.style.display = "block";
		sql_ops.style.display = "none";
		
		$.ajax({
			url: "http://localhost:8080/getCols",
			data: fd,
			processData: false,
			contentType: false,
  			type: 'POST',
			timeout: 600000,
			success: function (response) {
				
				loader.style.display = "none";
				sql_ops.style.display = "block";

				for (x in response.rows) {
					var col_name = response.rows[x][0];
					drpdata.push({id: col_name,text: col_name});
				}
				
				$("#table_columns").select2({
					theme: "classic",
					placeholder: "Select a column",
					width: 'resolve',
					multiple: true,
				  	data: drpdata
				});

				table_columns_div.style.display = "block";

			},
			error: function (e) {
				console.log("ERROR : ", e);

			}
		});
	});
	
	$("#reftable").on('change', function (event) {
		event.preventDefault();
		drpdata = [];
		
		dbDetails();
		$("#ref_table_columns").html("");
		
		fd.append( 'table', reftable );
		
		loader.style.display = "block";
		sql_ops.style.display = "none";
		
		$.ajax({
			url: "http://localhost:8080/getCols",
			data: fd,
			processData: false,
			contentType: false,
  			type: 'POST',
			timeout: 600000,
			success: function (response) {
				loader.style.display = "none";
				sql_ops.style.display = "block";
				
				for (x in response.rows) {
					var col_name = response.rows[x][0];
					drpdata.push({id: col_name,text: col_name});
				}
				$("#ref_table_columns").select2({
					theme: "classic",
					placeholder: "Select a column",
					width: 'resolve',
					multiple: true,
					data: drpdata
				})
				
				ref_table_columns_div.style.display = "block";

			},
			error: function (e) {
				console.log("ERROR : ", e);

			}
		});
	});
	
});