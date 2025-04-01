(** Order Processing System
    
    This module processes order and orderItem records, applying filters based on status and origin,
    and calculates totals for each order.
    
    The system handles two primary record types:
    - orders: Contains information about each order, including ID, client ID, datetime, status, and origin
    - orderItems: Contains the items in each order, including quantity, price, and tax
    
    The workflow:
    1. Convert CSV data into OCaml records
    2. Join orders with their corresponding items
    3. Apply filters based on command-line arguments (status and/or origin)
    4. Calculate total amounts and taxes for each order
    5. Format the results for output
*)

(** Status type representing the possible states of an order *)
type statusType = Pending | Complete | Cancelled

(** Origin type representing the channel through which the order was placed:
    O: Online, P: Phone *)
type originType = O | P

(** Order record type containing order metadata *)
type order = {
  id: int;          (** Unique identifier for the order *)
  _client_id: int;  (** Identifier for the client who placed the order *)
  _datetime: string; (** Timestamp when the order was placed *)
  status: statusType; (** Current status of the order *)
  origin: originType  (** Channel through which the order was placed *)
}

(** OrderItem record type containing details of items in an order *)
type orderItem = {
  order_id: int;     (** Reference to the order this item belongs to *)
  _product_id: int;  (** Identifier for the product *)
  _quantity: int;    (** Quantity of the product ordered *)
  price: float;      (** Price per unit *)
  tax: float         (** Tax amount for this item *)
}

(** Converts a string representation of status to the statusType variant
    @param s String representation of status
    @return The corresponding statusType
    @raise Failure if the string doesn't match any known status
*)
let match_status_type s = 
  match s with
  | "Pending"  -> Pending
  | "Complete" -> Complete
  | "Cancelled" -> Cancelled
  | _ -> failwith ("Unrecognized string: " ^ s)

(** Converts a string representation of origin to the originType variant
    @param s String representation of origin
    @return The corresponding originType
    @raise Failure if the string doesn't match any known origin
*)
let match_origin_type s = 
  match s with
  | "O" -> O
  | "P" -> P
  | _ -> failwith ("Unrecognized string: " ^ s)

(** Converts a list of strings to an order record
    @param l List of strings representing order fields
    @return An order record
    @raise Failure if the list doesn't have exactly 5 elements
*)
let convert_order l = 
  match l with
  | [a; b; c; d; e] -> { id = int_of_string a; _client_id = int_of_string b; _datetime = c; status = match_status_type d; origin = match_origin_type e }
 | _a -> failwith "Wrong number of elements in list in convert order"

(** Converts a list of strings to an orderItem record
    @param l List of strings representing orderItem fields
    @return An orderItem record
    @raise Failure if the list doesn't have exactly 5 elements
*)
let convert_orderItem l =
  match l with
  | [a; b; c; d; e] -> { order_id = int_of_string a; _product_id = int_of_string b; _quantity = int_of_string c; price = float_of_string d; tax = float_of_string e }
  | _a -> failwith "Wrong number of elements in list in convert order item"

(** Removes the first element from a list
    @param l The input list
    @return The list without its first element
*)
let cut_first_element l = 
  match l with
  | [] -> []
  | _::t -> t

(** Removes the last element from a list
    @param l The input list
    @return The list without its last element
*)
let cut_last_element l =
  match List.rev l with
  | [] -> []
  | _ :: rest -> List.rev rest

(** Converts a list of CSV rows to a list of records using the provided conversion function
    Removes the first row (assumed to be headers) and the last row (assumed to be empty)
    @param f The conversion function to apply to each row
    @param l The list of CSV rows
    @return A list of records
*)
let convert_to_recordlist f l =
  cut_first_element l
  |> cut_last_element
  |> List.map (fun a -> f a)

(** Performs an inner join between orders and order_items based on order ID
    @param orders List of order records
    @param order_items List of orderItem records
    @return List of (order, orderItem) pairs where order_item.order_id = order.id
*)
let inner_join orders order_items=
  List.fold_left(fun acc order ->
      List.filter (fun order_item -> order_item.order_id = order.id) order_items
      |> List.map (fun order_item -> (order, order_item))
      |> List.append acc 
  ) [] orders 

(** Filters joined records by order status
    @param status The status to filter by
    @param joined_items List of (order, orderItem) pairs
    @return Filtered list containing only orders with the specified status
*)
let filter_by_status status joined_items = 
  List.filter (fun (order, _orderItem) -> order.status == status) joined_items

(** Filters joined records by order origin
    @param origin The origin to filter by
    @param joined_items List of (order, orderItem) pairs
    @return Filtered list containing only orders with the specified origin
*)
let filter_by_origin origin joined_items =
  List.filter (fun (order, _orderItem) -> order.origin == origin) joined_items

(** Processes command-line arguments to determine which filters to apply
    @param args Command-line arguments array
    @return A tuple (status, origin, flag) where flag indicates which filters to apply:
            1: Both status and origin filters
            2: Only status filter
            3: Only origin filter
            4: No filters
    @raise Failure if arguments are invalid
*)
let check_sysargs args =
  (*The return values indicate 1 if all filteres are used, 2 if only the status is used, 3 if only the origin type is used, and 4 if none are used*)
  match args with
  | [| _ ; a; b |] -> (match_status_type a, match_origin_type b , 1)
  | [| _ ; ("Pending" | "Complete" | "Cancelled") as a |] -> (match_status_type a , match_origin_type "O", 2)
  | [| _ ; ("P" | "O") as a |] -> (match_status_type "Complete" , match_origin_type a, 3)
  | [| _ |] -> (match_status_type "Complete" , match_origin_type "O", 4)
  | _ -> failwith "Something went wrong when trating input arguments. Check number of arguments provided to program"

(** Applies filters to the joined records based on the flag from check_sysargs
    @param records List of (order, orderItem) pairs
    @param status Status to filter by
    @param origin Origin to filter by
    @param flag Indicates which filters to apply (1-4)
    @return Filtered list of records
    @raise Failure if flag is invalid
*)
let apply_filters records status origin flag= 
  match flag with
  | 1 -> filter_by_status status records |> filter_by_origin origin
  | 2 -> filter_by_status status records
  | 3 -> filter_by_origin origin records
  | 4 -> records
  | _ -> failwith "Could not filter records properly, as flag provided does not exist."  

(** Record type for summarized order information after processing *)
type transformed_records = {
  order_id: int;        (** Order identifier *)
  total_amount: float;  (** Total amount for the order (sum of prices) *)
  total_taxes: float    (** Total taxes for the order (sum of taxes) *)
}

(** Module for maintaining a set of unique integer IDs *)
module IntSet = Set.Make(struct
  type t = int
  let compare = compare
end)

(** Extracts unique order IDs from the joined records
    @param joined_records List of (order, orderItem) pairs
    @param record_id_set Set of already found unique IDs
    @return Set of all unique order IDs
*)
let rec get_unique_ids joined_records record_id_set = 
  match joined_records with
    | [] -> record_id_set
    | (a,_)::t -> if IntSet.mem a.id record_id_set then get_unique_ids t record_id_set 
                  else IntSet.add a.id record_id_set |> get_unique_ids t

(** Converts a tuple of totals with an ID to a transformed_records record
    @param x Order ID
    @param d Tuple of (total_amount, total_taxes)
    @return transformed_records record
*)
let tuple_to_record x d = 
  match d with
  | (a, b) -> {order_id=x; total_amount=a; total_taxes=b}

(** Processes joined records to calculate totals for each unique order
    @param l List of (order, orderItem) pairs
    @param unique_ids Set of unique order IDs
    @return List of transformed_records with calculated totals
*)
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

(** Converts a list of transformed_records to a list of formatted strings
    @param recs List of transformed_records
    @return List of formatted strings representing the records
*)
let joined_records_to_strlist recs = 
  List.map (fun r -> Printf.sprintf "%d,%.2f,%.2f\n" r.order_id r.total_amount r.total_taxes) recs
