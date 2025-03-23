let rec print_list = function 
  [] -> ()
  | h::t -> print_string h ; print_string "," ; print_list t

let rec _print_intermediate = function
  [] -> ()
  | h::t-> print_list h; print_char '\n' ; _print_intermediate t 

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
  | a -> print_list a; failwith "Wrong number of elements in list in convert order"

let convert_orderItem l =
  match l with
  | [a; b; c; d; e] -> { order_id = int_of_string a; _product_id = int_of_string b; _quantity = int_of_string c; price = float_of_string d; tax = float_of_string e }
  | b -> print_list b; failwith "Wrong number of elements in list in convert order item"

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
  |> List.map (fun a -> print_list a; f a)

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

(* Function to print a single order *)
let print_order order =
  Printf.printf "{ order_id = %d; total_amount = %.2f; total_taxes = %.2f }\n"
    order.order_id order.total_amount order.total_taxes

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
let () = List.iter print_order processed_joined_records

let content = joined_records_to_strlist processed_joined_records |> List.append ["order_id,total_amount,total_taxes\n"] ;;
Impure.write_csv "output.csv" content
