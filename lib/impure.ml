let readCsvToString filename = 
  let ic = open_in filename in
  let len = in_channel_length ic in
  let content = really_input_string ic len in
  close_in ic;
  content

let splitCsvString rawCsv = 
    rawCsv
    |> String.split_on_char '\n'
    |> List.map (function x -> String.split_on_char ',' x)

let read_csv filename =
  readCsvToString filename
  |> splitCsvString

let write_csv filename contents =
  let oc = open_out filename in 
  List.iter (fun p ->
    Printf.fprintf oc "%s" p ;
  ) contents;
  close_out oc

let rec print_list = function 
  [] -> ()
  | h::t -> print_string h ; print_string "," ; print_list t

let rec print_intermediate = function
  [] -> ()
  | h::t-> print_list h; print_char '\n' ; print_intermediate t 

type transformed_records = {order_id: int; total_amount:float; total_taxes:float}

let print_order order =
  Printf.printf "{ order_id = %d; total_amount = %.2f; total_taxes = %.2f }\n"
    order.order_id order.total_amount order.total_taxes

let http_get url =
  let buffer = Buffer.create 16384 in
  let connection = Curl.init () in
  Curl.set_url connection url;
  Curl.set_writefunction connection (fun s ->
    Buffer.add_string buffer s;
    String.length s  (* Return the number of bytes processed *)
  );
  Curl.perform connection;  (* This call blocks until the request is complete *)
  Curl.cleanup connection;
  Buffer.contents buffer

let read_csv_from_http url = 
  http_get url
  |> splitCsvString

let open_sqlite_db db_name = 
  db_name ^ ".db3"
  |> Sqlite3.db_open


let close_sql_db db =
  Sqlite3.db_close db

let remove_trailing_newline s = 
  let n = String.length s in
  if n > 0 && s.[n-1] = '\n' then
    String.sub s 0 (n-1)
  else
    s

let write_output_to_sqlite db (res : string list list)=
  Sqlite3.exec db "CREATE TABLE IF NOT EXISTS results (order_id INTEGER PRIMARY KEY, price FLOAT, tax FLOAT)"|> ignore;
  List.map (fun x -> "INSERT INTO results VALUES (" ^ (List.nth x 0) ^ "," ^ (List.nth x 1) ^ "," ^ ( List.nth x 2 |> remove_trailing_newline)  ^ ")") res
  |> List.iter (fun x -> print_string x; Sqlite3.exec db x |> ignore)
