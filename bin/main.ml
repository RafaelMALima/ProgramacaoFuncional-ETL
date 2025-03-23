(**
  @file main.ml
  @author Rafael Lima
  @since 2025-03-23
  @summary ETL process for orders and order items: reading CSV files, transforming data, joining records, filtering, aggregating, and writing the output.

  This module implements an ETL (Extract, Transform, Load) process for order-related data stored in CSV files.
  The workflow is as follows:
  
  1. **Data Loading:**  
     Reads CSV files "order.csv" and "order_item.csv" via impure operations provided by the [Impure] module.
     The CSV contents are converted to lists of typed records (orders and order items) after removing headers and any extraneous
     trailing elements.
  
  2. **Data Joining:**  
     Performs an inner join on the orders and order items based on order IDs using [inner_join]. This produces a list
     of tuples where each tuple contains an order and its associated order item.
  
  3. **Filtering:**  
     Command-line arguments are parsed by [check_sysargs] to determine filtering criteria:
       - Flag 1: Both status and origin filters are provided.
       - Flag 2: Only the status filter is provided.
       - Flag 3: Only the origin filter is provided.
       - Flag 4: No filters provided (defaults applied).
     The filters are then applied on the joined records using [apply_filters], which delegates to [filter_by_status] and/or [filter_by_origin].
  
  4. **Aggregation:**  
     Unique order IDs are extracted from the filtered records using [get_unique_ids]. Then,
     for each unique order, [process_joined_records] aggregates the total order amount and total taxes by summing up the
     corresponding values from each order item.
  
  5. **Output Generation:**  
     The aggregated records are converted to CSV-formatted strings via [joined_records_to_strlist] and written to "output.csv"
     using [Impure.write_csv].

  ## Types

  - **statusType:**  
    Represents the status of an order.  
    Variants: `Pending`, `Complete`, `Cancelled`.

  - **originType:**  
    Represents the origin of an order.  
    Variants: `O`, `P`.

  - **order:**  
    A record type with fields:
      - `id`: Order identifier.
      - `_client_id`: Client identifier.
      - `_datetime`: Timestamp for the order.
      - `status`: Order status (of type [statusType]).
      - `origin`: Order origin (of type [originType]).

  - **orderItem:**  
    A record type with fields:
      - `order_id`: Identifier linking the item to an order.
      - `_product_id`: Product identifier.
      - `_quantity`: Quantity of the product ordered.
      - `price`: Price per unit.
      - `tax`: Tax associated with the order item.

  - **transformed_records:**  
    Represents the aggregated result for an order, with fields:
      - `order_id`: Order identifier.
      - `total_amount`: Total sum of item prices for the order.
      - `total_taxes`: Total sum of taxes for the order.

  ## Functions

  - **match_status_type : string -> statusType**  
    **Description:** Converts a status string (e.g., "Pending", "Complete", "Cancelled") into the corresponding [statusType] variant.  
    **Raises:** Fails with an error if the input string is not recognized.

  - **match_origin_type : string -> originType**  
    **Description:** Converts an origin string ("O" or "P") into the corresponding [originType] variant.  
    **Raises:** Fails with an error if the input string is not recognized.

  - **convert_order : string list -> order**  
    **Description:** Transforms a list of 5 strings (representing CSV fields) into an [order] record by converting
    string representations into integers and matching strings to variants using [match_status_type] and [match_origin_type].  
    **Raises:** Fails if the list does not contain exactly 5 elements.

  - **convert_orderItem : string list -> orderItem**  
    **Description:** Transforms a list of 5 strings into an [orderItem] record. It converts strings into the required
    integer and float types.  
    **Raises:** Fails if the list does not contain exactly 5 elements.

  - **cut_first_element : 'a list -> 'a list**  
    **Description:** Returns the input list without its first element.  
    **Note:** If the list is empty, returns an empty list.

  - **cut_last_element : 'a list -> 'a list**  
    **Description:** Returns the input list without its last element by reversing the list, dropping the head, and reversing back.  
    **Note:** If the list is empty, returns an empty list.

  - **convert_to_recordlist : (string list -> 'a) -> string list list -> 'a list**  
    **Description:** Converts CSV row data into a list of records:
      1. Removes the first element (typically the header) and the last element from the list of rows.
      2. Maps the provided conversion function (e.g., [convert_order] or [convert_orderItem]) over the remaining rows.
  
  - **inner_join : order list -> orderItem list -> (order * orderItem) list**  
    **Description:** Performs an inner join between the list of orders and the list of order items.
    For each order, it finds all order items with matching `order_id` and returns a list of (order, orderItem) pairs.

  - **filter_by_status : statusType -> (order * orderItem) list -> (order * orderItem) list**  
    **Description:** Filters the joined order pairs by comparing the order's status to the specified [statusType].

  - **filter_by_origin : originType -> (order * orderItem) list -> (order * orderItem) list**  
    **Description:** Filters the joined order pairs by comparing the order's origin to the specified [originType].

  - **check_sysargs : string array -> (statusType * originType * int)**  
    **Description:** Parses the command-line arguments to determine filtering criteria.  
    **Behavior:**
      - If two filters are provided (status and origin), returns flag 1.
      - If only a status filter is provided, returns flag 2 (with a default origin).
      - If only an origin filter is provided, returns flag 3 (with a default status).
      - If no filters are provided, returns flag 4 with default values (status "Complete" and origin "O").  
    **Raises:** Fails if the arguments do not meet the expected patterns.

  - **apply_filters : (order * orderItem) list -> statusType -> originType -> int -> (order * orderItem) list**  
    **Description:** Applies filtering to the joined records based on the flag determined by [check_sysargs]:
      - Flag 1: Apply both status and origin filters.
      - Flag 2: Apply only the status filter.
      - Flag 3: Apply only the origin filter.
      - Flag 4: Apply no filtering.
    **Raises:** Fails if an invalid flag is provided.

  - **get_unique_ids : (order * orderItem) list -> IntSet.t -> IntSet.t**  
    **Description:** Recursively collects unique order IDs from the list of joined records into a set.
    Uses [IntSet] (a set of integers) as the accumulator.

  - **tuple_to_record : int -> (float * float) -> transformed_records**  
    **Description:** Constructs a [transformed_records] record from an order ID and a tuple containing the aggregated total amount and total taxes.

  - **process_joined_records : (order * orderItem) list -> IntSet.t -> transformed_records list**  
    **Description:** Aggregates data for each unique order ID:
      - Filters joined records for a specific order.
      - Sums the prices and taxes from all associated order items.
      - Converts the results into a [transformed_records] record.
    Returns a list of aggregated records for output.

  - **joined_records_to_strlist : transformed_records list -> string list**  
    **Description:** Converts each [transformed_records] record into a CSV-formatted string ("order_id,total_amount,total_taxes\n").

  ## Program Execution Flow

  - **Loading Data:**  
    Reads CSV files "order.csv" and "order_item.csv" via [Impure.read_csv] and converts them into lists of records using [convert_to_recordlist] with [convert_order] and [convert_orderItem].

  - **Joining Records:**  
    Uses [inner_join] to merge orders with corresponding order items into (order, orderItem) pairs.

  - **Filtering:**  
    Parses command-line arguments with [check_sysargs] and applies the appropriate filters on the joined records via [apply_filters].

  - **Aggregation:**  
    Extracts unique order IDs using [get_unique_ids] and aggregates the total amounts and taxes per order using [process_joined_records].

  - **Output:**  
    Formats the aggregated records into CSV strings with [joined_records_to_strlist] and writes the result to "output.csv" using [Impure.write_csv].

  ## Dependencies

  This module relies on impure functions provided by the [Impure] module, such as:
    - [Impure.read_csv]: For reading CSV files.
    - [Impure.write_csv]: For writing CSV data.
    - [Impure.print_order]: (Commented out) Intended for printing records during debugging.

  The design cleanly separates pure data transformation (in this module) from side-effecting operations (handled by the [Impure] module),
  improving testability and maintainability of the ETL process.

These docs have benn written with the assistance of Artificial Intelligence tools.
*)

type statusType = Pending | Complete | Cancelled
type originType = O | P
type order = {id: int; _client_id: int; _datetime: string; status: statusType; origin: originType}
type orderItem = {order_id: int; _product_id: int; _quantity: int; price: float; tax: float}

let match_status_type s = 
  match s with
  | "Pending"  -> Pending
  | "Complete" -> Complete
  | "Cancelled" -> Cancelled
  | _ -> failwith ("Unrecognized string: " ^ s)

let match_origin_type s = 
  match s with
  | "O" -> O
  | "P" -> P
  | _ -> failwith ("Unrecognized string: " ^ s)

let convert_order l = 
  match l with
  | [a; b; c; d; e] -> { id = int_of_string a; _client_id = int_of_string b; _datetime = c; status = match_status_type d; origin = match_origin_type e }
  | _a -> failwith "Wrong number of elements in list in convert order"

let convert_orderItem l =
  match l with
  | [a; b; c; d; e] -> { order_id = int_of_string a; _product_id = int_of_string b; _quantity = int_of_string c; price = float_of_string d; tax = float_of_string e }
  | _a -> failwith "Wrong number of elements in list in convert order item"

let cut_first_element l = 
  match l with
  | [] -> []
  | _::t -> t

let cut_last_element l =
  match List.rev l with
  | [] -> []               (* Return empty list if input is empty *)
  | _ :: rest -> List.rev rest

let convert_to_recordlist f l =
  cut_first_element l
  |> cut_last_element
  |> List.map (fun a -> f a)

let inner_join orders order_items=
  List.fold_left(fun acc order ->
      List.filter (fun order_item -> order_item.order_id = order.id) order_items
      |> List.map (fun order_item -> (order, order_item))
      |> List.append acc 
  ) [] orders 

let filter_by_status status joined_items = 
  List.filter (fun (order, _order_item) -> order.status == status) joined_items

let filter_by_origin origin joined_items =
  List.filter (fun (order, _order_item) -> order.origin == origin) joined_items

let check_sysargs args =
  (*The return values indicate 1 if all filteres are used, 2 if only the status is used, 3 if only the origin type is used, and 4 if none are used*)
  match args with
  | [| _ ; a; b |] -> (match_status_type a, match_origin_type b , 1)
  | [| _ ; ("Pending" | "Complete" | "Cancelled") as a |] -> (match_status_type a , match_origin_type "O", 2)
  | [| _ ; ("P" | "O") as a |] -> (match_status_type "Complete" , match_origin_type a, 3)
  | [| _ |] -> (match_status_type "Complete" , match_origin_type "O", 4)
  | _ -> failwith "Something went wrong when trating input arguments. Check number of arguments provided to program"

let apply_filters records status origin flag= 
  match flag with
  | 1 -> filter_by_status status records |> filter_by_origin origin
  | 2 -> filter_by_status status records
  | 3 -> filter_by_origin origin records
  | 4 -> records
  | _ -> failwith "Could not filter records properly, as flag provided does not exist."  

type transformed_records = {order_id: int; total_amount:float; total_taxes:float}

module IntSet = Set.Make(struct
  type t = int
  let compare = compare
end)

let rec get_unique_ids joined_records record_id_set = 
  match joined_records with
    | [] -> record_id_set
    | (a,_)::t -> if IntSet.mem a.id record_id_set then get_unique_ids t record_id_set 
                  else IntSet.add a.id record_id_set |> get_unique_ids t

let tuple_to_record x d = 
  match d with
  | (a, b) -> {order_id=x; total_amount=a; total_taxes=b}


let process_joined_records l unique_ids = 
  IntSet.fold (fun x acc -> 
    let sum_tuple = 
      List.filter (fun (a, _) -> a.id = x) l 
      |> List.fold_left (fun (price_sum, tax_sum) (_, b) -> 
           (b.price +. price_sum, b.tax +. tax_sum)
         ) (0., 0.)
    in
    (tuple_to_record x sum_tuple) :: acc  (* Wrap the tuple in a list and prepend it *)
  ) unique_ids []


let joined_records_to_strlist recs = 
  List.map (fun r -> Printf.sprintf "%d,%.2f,%.2f\n" r.order_id r.total_amount r.total_taxes) recs

(* Start of program*)

(*Load data from the CSVs into lists of records*)
let order_records = Impure.read_csv "order.csv"
  |> convert_to_recordlist convert_order

let orderItem_records = Impure.read_csv "order_item.csv"
  |> convert_to_recordlist convert_orderItem


(*Join the records into a single, unified table*)
let joined_records = inner_join order_records orderItem_records

let status, origin, flag  = check_sysargs Sys.argv
let filter_records = apply_filters joined_records status origin flag

let unique_ids = get_unique_ids filter_records IntSet.empty

let processed_joined_records = process_joined_records filter_records unique_ids
(*let () = List.iter Impure.print_order processed_joined_records*)

let content = joined_records_to_strlist processed_joined_records |> List.append ["order_id,total_amount,total_taxes\n"] ;;
Impure.write_csv "output.csv" content
