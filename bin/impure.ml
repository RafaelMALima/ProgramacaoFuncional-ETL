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

