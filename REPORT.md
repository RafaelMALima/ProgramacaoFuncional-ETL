# ETL Project Report

## Introduction

This report details the implementation of an Extract, Transform, Load (ETL) pipeline in OCaml. The project processes order data from CSV files, applies transformations, and outputs the results to both a CSV file and an SQLite database. The following sections describe each phase of the ETL process and how they were implemented using functional programming principles.

## Command-Line Argument Processing

The program begins by processing command-line arguments to determine filtering parameters. This is handled by the `check_sysargs` function in `helper.ml`:

```ocaml
let check_sysargs args =
  (*The return values indicate 1 if all filters are used, 2 if only the status is used, 3 if only the origin type is used, and 4 if none are used*)
  match args with
  | [| _ ; a; b |] -> (match_status_type a, match_origin_type b , 1)
  | [| _ ; ("Pending" | "Complete" | "Cancelled") as a |] -> (match_status_type a , match_origin_type "O", 2)
  | [| _ ; ("P" | "O") as a |] -> (match_status_type "Complete" , match_origin_type a, 3)
  | [| _ |] -> (match_status_type "Complete" , match_origin_type "O", 4)
  | _ -> failwith "Something went wrong when treating input arguments. Check number of arguments provided to program"
```

This function analyzes the command-line arguments (`Sys.argv`) and returns a tuple containing:
1. A status filter (Pending, Complete, or Cancelled)
2. An origin filter (O for Online or P for Phone)
3. A flag indicating which filters to apply (1-4)

### Example Usage

```bash
# Run with default filters (status=Complete, origin=O)
dune exec ETL

# Filter by status only
dune exec ETL -- Pending

# Filter by origin only
dune exec ETL -- P

# Filter by both status and origin
dune exec ETL -- Complete P
```

## Data Extraction via HTTP

The extraction phase uses the `Curl` library to fetch CSV data from remote URLs. This functionality is implemented in the `impure.ml` file, keeping I/O operations separate from pure data processing:

```ocaml
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
```

The `http_get` function uses the Curl library to perform an HTTP GET request and collects the response in a buffer. The `read_csv_from_http` function then splits this response into a list of lists, where each inner list represents a row of CSV data.

In `main.ml`, these functions are used to fetch the order and order item data:

```ocaml
let order_records = Impure.read_csv_from_http "https://raw.githubusercontent.com/RafaelMALima/ProgramacaoFuncional-ETL/refs/heads/main/order.csv"
  |> Helper.convert_to_recordlist Helper.convert_order

let orderItem_records = Impure.read_csv_from_http "https://raw.githubusercontent.com/RafaelMALima/ProgramacaoFuncional-ETL/refs/heads/main/order_item.csv"
  |> Helper.convert_to_recordlist Helper.convert_orderItem
```

## Data Loading into Records

The CSV data is loaded into strongly-typed OCaml records. The record structures are defined in `helper.ml`:

```ocaml
type statusType = Pending | Complete | Cancelled

type originType = O | P

type order = {
  id: int;          (** Unique identifier for the order *)
  _client_id: int;  (** Identifier for the client who placed the order *)
  _datetime: string; (** Timestamp when the order was placed *)
  status: statusType; (** Current status of the order *)
  origin: originType  (** Channel through which the order was placed *)
}

type orderItem = {
  order_id: int;     (** Reference to the order this item belongs to *)
  _product_id: int;  (** Identifier for the product *)
  _quantity: int;    (** Quantity of the product ordered *)
  price: float;      (** Price per unit *)
  tax: float         (** Tax amount for this item *)
}
```

Conversion functions transform the raw CSV data into these record types:

```ocaml
let convert_order l = 
  match l with
  | [a; b; c; d; e] -> { id = int_of_string a; _client_id = int_of_string b; _datetime = c; status = match_status_type d; origin = match_origin_type e }
 | _a -> failwith "Wrong number of elements in list in convert order"

let convert_orderItem l =
  match l with
  | [a; b; c; d; e] -> { order_id = int_of_string a; _product_id = int_of_string b; _quantity = int_of_string c; price = float_of_string d; tax = float_of_string e }
  | _a -> failwith "Wrong number of elements in list in convert order item"
```

The `convert_to_recordlist` function is a higher-order function that applies the appropriate conversion function to each row:

```ocaml
let convert_to_recordlist f l =
  cut_first_element l
  |> cut_last_element
  |> List.map (fun a -> f a)
```

This function first removes the header row using `cut_first_element` and any potential empty row at the end using `cut_last_element`, then maps the conversion function `f` over each remaining row.

## Performing the Inner Join

The inner join operation combines the order records with their corresponding order items based on matching IDs. This is implemented using higher-order functions in `helper.ml`:

```ocaml
let inner_join orders order_items =
  List.fold_left(fun acc order ->
      List.filter (fun order_item -> order_item.order_id = order.id) order_items
      |> List.map (fun order_item -> (order, order_item))
      |> List.append acc 
  ) [] orders 
```

This function uses:
1. `List.fold_left` to accumulate results from processing each order
2. `List.filter` to find all order items that match the current order's ID
3. `List.map` to create pairs of (order, order_item)
4. `List.append` to combine the new pairs with the accumulator

The result is a list of tuples, where each tuple contains an order and one of its associated order items.

## Filtering the Data

After joining the data, filters are applied based on the command-line arguments. The filtering is also implemented using higher-order functions:

```ocaml
let filter_by_status status joined_items = 
  List.filter (fun (order, _orderItem) -> order.status == status) joined_items

let filter_by_origin origin joined_items =
  List.filter (fun (order, _orderItem) -> order.origin == origin) joined_items

let apply_filters records status origin flag = 
  match flag with
  | 1 -> filter_by_status status records |> filter_by_origin origin
  | 2 -> filter_by_status status records
  | 3 -> filter_by_origin origin records
  | 4 -> records
  | _ -> failwith "Could not filter records properly, as flag provided does not exist."
```

The `filter_by_status` and `filter_by_origin` functions use `List.filter` to keep only the records that match the specified criteria. The `apply_filters` function selects which filters to apply based on the flag value.

## Data Transformation

After filtering, the data is transformed into the final output format. First, unique order IDs are extracted:

```ocaml
module IntSet = Set.Make(struct
  type t = int
  let compare = compare
end)

let rec get_unique_ids joined_records record_id_set = 
  match joined_records with
    | [] -> record_id_set
    | (a,_)::t -> if IntSet.mem a.id record_id_set then get_unique_ids t record_id_set 
                  else IntSet.add a.id record_id_set |> get_unique_ids t
```

Then, for each unique order ID, the total amount and taxes are calculated:

```ocaml
type transformed_records = {
  order_id: int;        (** Order identifier *)
  total_amount: float;  (** Total amount for the order (sum of prices) *)
  total_taxes: float    (** Total taxes for the order (sum of taxes) *)
}

let process_joined_records l unique_ids = 
  IntSet.fold (fun x acc -> 
    let sum_tuple = 
      List.filter (fun (a, _) -> a.id = x) l 
      |> List.fold_left (fun (price_sum, tax_sum) (_, b) -> 
           (b.price +. price_sum, b.tax +. tax_sum)
         ) (0., 0.)
    in
    (tuple_to_record x sum_tuple) :: acc
  ) unique_ids []
```

This function:
1. Uses `IntSet.fold` to process each unique order ID
2. For each ID, filters the joined records to get only those belonging to the current order
3. Uses `List.fold_left` to accumulate the total price and tax
4. Converts the result to a `transformed_records` record and adds it to the accumulator

## Data Loading to SQLite

Finally, the transformed data is loaded into an SQLite database. This is handled by functions in `impure.ml`:

```ocaml
let open_sqlite_db db_name = 
  db_name ^ ".db3"
  |> Sqlite3.db_open

let write_output_to_sqlite db (res : string list list) =
  Sqlite3.exec db "CREATE TABLE IF NOT EXISTS results (order_id INTEGER PRIMARY KEY, price FLOAT, tax FLOAT)" |> ignore;
  List.map (fun x -> "INSERT INTO results VALUES (" ^ (List.nth x 0) ^ "," ^ (List.nth x 1) ^ "," ^ (List.nth x 2 |> remove_trailing_newline) ^ ")") res
  |> List.iter (fun x -> Sqlite3.exec db x |> ignore)
```

The `write_output_to_sqlite` function:
1. Creates a `results` table if it doesn't exist
2. Maps over the list of results to create SQL INSERT statements
3. Executes each INSERT statement using the SQLite library

In `main.ml`, these functions are used as follows:

```ocaml
let list_of_str_of_str = Helper.joined_records_to_strlist processed_joined_records |> List.map (fun s -> String.split_on_char ',' s)
let a = Impure.open_sqlite_db "output";;
let _res = Impure.write_output_to_sqlite a list_of_str_of_str
let _ = Impure.close_sql_db a
```

The processed records are converted to strings, split into lists of values, and then inserted into the SQLite database.

## Appendix: Project Structure using Dune

The project is structured using the Dune build system, which is a modern build system for OCaml. The structure is organized as follows:

### dune-project

The `dune-project` file defines project-level metadata:

```
(lang dune 3.17)
(name ETL)
(generate_opam_files true)
(source (github username/reponame))
(authors "Author Name <author@example.com>")
(maintainers "Maintainer Name <maintainer@example.com>")
(license LICENSE)
(documentation https://url/to/documentation)

(package
 (name ETL)
 (synopsis "A short synopsis")
 (description "A longer description")
 (depends ocaml ocurl sqlite3)
 (tags ("add topics" "to describe" your project)))
```

### lib/dune

The `lib/dune` file configures the core library, which contains the pure and impure functionality:

```
(library
 (name lib)
 (modules helper impure)
 (libraries curl sqlite3))
```

This defines a library named `lib` that consists of two modules (`helper` and `impure`) and depends on the `curl` and `sqlite3` libraries.

### bin/dune

The `bin/dune` file configures the executable:

```
(executable
 (public_name ETL)
 (name main)
 (libraries lib curl sqlite3))
```

This defines an executable named `ETL` with the main entry point in `main.ml`, which depends on the `lib` library as well as `curl` and `sqlite3`.

### test/dune

The `test/dune` file configures the test suite:

```
(test
 (name test_ETL)
 (modules test_ETL)
 (libraries lib ounit2))
```

This defines a test executable named `test_ETL` that depends on the `lib` library and the `ounit2` testing framework.

This Dune structure separates concerns:
- Pure functions for data processing are in `helper.ml`
- Impure I/O operations are in `impure.ml`
- The main program logic is in `main.ml`
- Tests for the pure functions are in `test_ETL.ml`

This separation makes the code more modular, easier to test, and adheres to functional programming principles by isolating side effects.

Generated with the assistance of aritificial intelligence tools.
