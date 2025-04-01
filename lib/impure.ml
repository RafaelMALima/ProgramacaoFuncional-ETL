(** CSV Processing and I/O Utilities
    
    This module provides functions for reading, processing, and writing CSV data,
    as well as utilities for HTTP requests and SQLite database operations.
    
    The module supports various data sources including:
    - Local CSV files
    - HTTP-based CSV data
    - SQLite databases
    
    It also includes utility functions for printing data structures and
    processing string content.
*)

(** Reads the entire contents of a file into a string
    @param filename Path to the file to read
    @return The file contents as a string
*)
let readCsvToString filename = 
  let ic = open_in filename in
  let len = in_channel_length ic in
  let content = really_input_string ic len in
  close_in ic;
  content

(** Splits a CSV string into a list of lists, with each inner list representing a row
    @param rawCsv The raw CSV string
    @return A list of lists, where each inner list contains the values of a row
*)
let splitCsvString rawCsv = 
    rawCsv
    |> String.split_on_char '\n'
    |> List.map (function x -> String.split_on_char ',' x)

(** Reads a CSV file and returns its contents as a list of lists
    @param filename Path to the CSV file
    @return A list of lists representing the CSV data
*)
let read_csv filename =
  readCsvToString filename
  |> splitCsvString

(** Writes a list of strings to a CSV file
    @param filename Path to the output file
    @param contents List of strings to write to the file
*)
let write_csv filename contents =
  let oc = open_out filename in 
  List.iter (fun p ->
    Printf.fprintf oc "%s" p ;
  ) contents;
  close_out oc

(** Prints a list of strings to stdout, separated by commas
    @param list The list of strings to print
*)
let rec print_list = function 
  [] -> ()
  | h::t -> print_string h ; print_string "," ; print_list t

(** Prints a list of lists to stdout, with each inner list on a new line
    @param list The list of lists to print
*)
let rec print_intermediate = function
  [] -> ()
  | h::t-> print_list h; print_char '\n' ; print_intermediate t 

(** Record type for summarized order information after processing *)
type transformed_records = {
  order_id: int;        (** Order identifier *)
  total_amount: float;  (** Total amount for the order (sum of prices) *)
  total_taxes: float    (** Total taxes for the order (sum of taxes) *)
}

(** Prints a transformed_records record to stdout in a formatted way
    @param order The transformed_records record to print
*)
let print_order order =
  Printf.printf "{ order_id = %d; total_amount = %.2f; total_taxes = %.2f }\n"
    order.order_id order.total_amount order.total_taxes

(** Performs an HTTP GET request and returns the response body
    @param url The URL to request
    @return The response body as a string
    @requires The Curl library
*)
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

(** Reads CSV data from a URL and returns it as a list of lists
    @param url The URL pointing to CSV data
    @return A list of lists representing the CSV data
*)
let read_csv_from_http url = 
  http_get url
  |> splitCsvString

(** Opens an SQLite database file
    @param db_name The base name of the database (without extension)
    @return The SQLite database connection
    @requires The Sqlite3 library
*)
let open_sqlite_db db_name = 
  db_name ^ ".db3"
  |> Sqlite3.db_open

(** Closes an SQLite database connection
    @param db The SQLite database connection to close
*)
let close_sql_db db =
  Sqlite3.db_close db

(** Removes a trailing newline character from a string if present
    @param s The input string
    @return The string without trailing newline
*)
let remove_trailing_newline s = 
  let n = String.length s in
  if n > 0 && s.[n-1] = '\n' then
    String.sub s 0 (n-1)
  else
    s

(** Writes the processed results to an SQLite database
    @param db The SQLite database connection
    @param res The list of lists containing order results (order_id, price, tax)
    
    This function creates a 'results' table if it doesn't exist and inserts
    the provided data as rows. Each row in res should contain exactly three values
    corresponding to order_id, price, and tax.
*)
let write_output_to_sqlite db (res : string list list)=
  Sqlite3.exec db "CREATE TABLE IF NOT EXISTS results (order_id INTEGER PRIMARY KEY, price FLOAT, tax FLOAT)"|> ignore;
  List.map (fun x -> "INSERT INTO results VALUES (" ^ (List.nth x 0) ^ "," ^ (List.nth x 1) ^ "," ^ ( List.nth x 2 |> remove_trailing_newline)  ^ ")") res
  |> List.iter (fun x -> Sqlite3.exec db x |> ignore)
