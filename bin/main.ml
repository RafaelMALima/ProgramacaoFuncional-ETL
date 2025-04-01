open Lib

let order_records = Impure.read_csv_from_http "https://raw.githubusercontent.com/RafaelMALima/ProgramacaoFuncional-ETL/refs/heads/main/order.csv"
  |> Helper.convert_to_recordlist Helper.convert_order

let orderItem_records = Impure.read_csv_from_http "https://raw.githubusercontent.com/RafaelMALima/ProgramacaoFuncional-ETL/refs/heads/main/order_item.csv"
  |> Helper.convert_to_recordlist Helper.convert_orderItem


(*Join the records into a single, unified table*)
let joined_records = Helper.inner_join order_records orderItem_records

let status, origin, flag  = Helper.check_sysargs Sys.argv
let filter_records = Helper.apply_filters joined_records status origin flag

let unique_ids = Helper.get_unique_ids filter_records Helper.IntSet.empty

let processed_joined_records = Helper.process_joined_records filter_records unique_ids
(*let () = List.iter Impure.print_order processed_joined_records*)

let content = Helper.joined_records_to_strlist processed_joined_records |> List.append ["order_id,total_amount,total_taxes\n"] ;;
Impure.write_csv "output.csv" content

let list_of_str_of_str = Helper.joined_records_to_strlist processed_joined_records |> List.map (fun s -> String.split_on_char ',' s)
let a = Impure.open_sqlite_db "output";;
let _res = Impure.write_output_to_sqlite a list_of_str_of_str
let _ = Impure.close_sql_db a
