(**
  @file impure.ml
  @author Rafael Lima
  @since 2025-03-23
  @summary Implements impure operations for the ETL process, including CSV file I/O and console printing.

  This module provides functions that perform side-effecting operations needed by the ETL pipeline.
  It includes utilities for reading and writing CSV files, splitting CSV content into lists of strings,
  and printing lists or aggregated records to the console.

  ## Functions

  - [readCsvToString : string -> string]  
    **Description:**  
      Opens the specified file, reads its entire content as a string, and then closes the file.
    **Parameters:**  
      - filename: The path to the CSV file to be read.
    **Returns:**  
      - A string containing the complete contents of the file.
  
  - [splitCsvString : string -> string list list]  
    **Description:**  
      Splits a raw CSV string into a list of rows, where each row is a list of column strings.
      The function first splits the input by newline characters and then splits each line by commas.
    **Parameters:**  
      - rawCsv: The raw CSV data as a string.
    **Returns:**  
      - A list of rows, with each row represented as a list of strings.
  
  - [read_csv : string -> string list list]  
    **Description:**  
      Combines [readCsvToString] and [splitCsvString] to read a CSV file and return its contents
      as a list of rows (each row is a list of strings).
    **Parameters:**  
      - filename: The path to the CSV file.
    **Returns:**  
      - A list of rows extracted from the CSV file.
  
  - [write_csv : string -> string list -> unit]  
    **Description:**  
      Writes a list of CSV-formatted strings to a file. Each string is written sequentially,
      creating or overwriting the file specified.
    **Parameters:**  
      - filename: The destination file path.
      - contents: A list of strings, where each string represents a line in the CSV file.
    **Side Effects:**  
      - Creates or overwrites the target file with the provided contents.
  
  - [print_list : string list -> unit]  
    **Description:**  
      Recursively prints the elements of a string list, separating each element with a comma.
    **Parameters:**  
      - A list of strings to be printed.
  
  - [print_intermediate : string list list -> unit]  
    **Description:**  
      Recursively prints each row of strings (using [print_list]) and then outputs a newline after each row.
    **Parameters:**  
      - A list of rows, where each row is a list of strings.
  
  - [print_order : transformed_records -> unit]  
    **Description:**  
      Prints a [transformed_records] record in a formatted string that displays the order ID,
      total amount, and total taxes.
    **Parameters:**  
      - order: A record of type [transformed_records].
    **Output Format:**  
      - The record is printed in the format:  
        `{ order_id = <id>; total_amount = <amount>; total_taxes = <tax> }`

  ## Types

  - **transformed_records:**  
    A record type representing aggregated data for an order.  
    Fields:
      - `order_id` : An integer representing the order's unique identifier.
      - `total_amount` : A float representing the total price summed over the order items.
      - `total_taxes` : A float representing the total taxes summed over the order items.
*)

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
