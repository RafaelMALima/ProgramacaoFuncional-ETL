open OUnit2
open Lib.Helper (* Assuming your main program is in a module called ETL *)

let test_match_status_type _ =
  assert_equal Pending (match_status_type "Pending");
  assert_equal Complete (match_status_type "Complete");
  assert_equal Cancelled (match_status_type "Cancelled");
  assert_raises (Failure "Unrecognized string: Invalid") (fun () -> match_status_type "Invalid")

let test_match_origin_type _ =
  assert_equal O (match_origin_type "O");
  assert_equal P (match_origin_type "P");
  assert_raises (Failure "Unrecognized string: Invalid") (fun () -> match_origin_type "Invalid")

let test_convert_order _ =
  let expected = { id = 1; _client_id = 2; _datetime = "2023-01-01"; status = Pending; origin = O } in
  let actual = convert_order ["1"; "2"; "2023-01-01"; "Pending"; "O"] in
  assert_equal expected actual;
  assert_raises (Failure "Wrong number of elements in list in convert order") 
    (fun () -> convert_order ["1"; "2"; "2023-01-01"; "Pending"])

let test_convert_orderItem _ =
  let expected = { order_id = 1; _product_id = 2; _quantity = 3; price = 10.5; tax = 2.0 } in
  let actual = convert_orderItem ["1"; "2"; "3"; "10.5"; "2.0"] in
  assert_equal expected actual;
  assert_raises (Failure "Wrong number of elements in list in convert order item") 
    (fun () -> convert_orderItem ["1"; "2"; "3"; "10.5"])

let test_cut_first_element _ =
  assert_equal [] (cut_first_element []);
  assert_equal [] (cut_first_element [1]);
  assert_equal [2; 3] (cut_first_element [1; 2; 3])

let test_cut_last_element _ =
  assert_equal [] (cut_last_element []);
  assert_equal [] (cut_last_element [1]);
  assert_equal [1; 2] (cut_last_element [1; 2; 3])

let test_convert_to_recordlist _ =
  let identity x = x in
  let input = ["header"; "1"; "2"; "footer"] in
  let expected = ["1"; "2"] in
  assert_equal expected (convert_to_recordlist identity input)

let test_inner_join _ =
  let orders = [
    { id = 1; _client_id = 100; _datetime = "2023-01-01"; status = Pending; origin = O };
    { id = 2; _client_id = 101; _datetime = "2023-01-02"; status = Complete; origin = P }
  ] in
  let order_items : orderItem list = [
    { order_id = 1; _product_id = 200; _quantity = 2; price = 10.0; tax = 1.0 };
    { order_id = 1; _product_id = 201; _quantity = 1; price = 20.0; tax = 2.0 };
    { order_id = 2; _product_id = 202; _quantity = 3; price = 15.0; tax = 1.5 }
  ] in
  let result = inner_join orders order_items in
  
  (* Just test basic properties of the join without trying to compare whole tuples *)
  assert_equal 3 (List.length result);
  
  (* Check that each tuple has matching IDs between order and order_item *)
  List.iter (fun ((order : order), (item : orderItem)) ->
    assert_equal order.id item.order_id
  ) result;
  
  (* Check that specific combinations exist in the result *)
  assert_bool "Should find order 1 with product 200" (
    List.exists (fun (o, i) -> o.id = 1 && i._product_id = 200) result
  );
  assert_bool "Should find order 1 with product 201" (
    List.exists (fun (o, i) -> o.id = 1 && i._product_id = 201) result
  );
  assert_bool "Should find order 2 with product 202" (
    List.exists (fun (o, i) -> o.id = 2 && i._product_id = 202) result
  )

let test_filter_by_status _ =
  let orders = [
    { id = 1; _client_id = 100; _datetime = "2023-01-01"; status = Pending; origin = O };
    { id = 2; _client_id = 101; _datetime = "2023-01-02"; status = Complete; origin = P }
  ] in
  let order_items = [
    { order_id = 1; _product_id = 200; _quantity = 2; price = 10.0; tax = 1.0 };
    { order_id = 2; _product_id = 202; _quantity = 3; price = 15.0; tax = 1.5 }
  ] in
  let joined = [
    (List.nth orders 0, List.nth order_items 0);
    (List.nth orders 1, List.nth order_items 1)
  ] in
  let filtered = filter_by_status Pending joined in
  assert_equal 1 (List.length filtered);
  let (order, _) = List.hd filtered in
  assert_equal Pending order.status

let test_filter_by_origin _ =
  let orders = [
    { id = 1; _client_id = 100; _datetime = "2023-01-01"; status = Pending; origin = O };
    { id = 2; _client_id = 101; _datetime = "2023-01-02"; status = Complete; origin = P }
  ] in
  let order_items = [
    { order_id = 1; _product_id = 200; _quantity = 2; price = 10.0; tax = 1.0 };
    { order_id = 2; _product_id = 202; _quantity = 3; price = 15.0; tax = 1.5 }
  ] in
  let joined = [
    (List.nth orders 0, List.nth order_items 0);
    (List.nth orders 1, List.nth order_items 1)
  ] in
  let filtered = filter_by_origin O joined in
  assert_equal 1 (List.length filtered);
  let (order, _) = List.hd filtered in
  assert_equal O order.origin

let test_check_sysargs _ =
  assert_equal (Complete, O, 1) (check_sysargs [|"program"; "Complete"; "O"|]);
  assert_equal (Pending, O, 2) (check_sysargs [|"program"; "Pending"|]);
  assert_equal (Complete, P, 3) (check_sysargs [|"program"; "P"|]);
  assert_equal (Complete, O, 4) (check_sysargs [|"program"|]);
  assert_raises (Failure "Something went wrong when trating input arguments. Check number of arguments provided to program") 
    (fun () -> check_sysargs [|"program"; "arg1"; "arg2"; "arg3"|])

let test_apply_filters _ =
  let orders = [
    { id = 1; _client_id = 100; _datetime = "2023-01-01"; status = Pending; origin = O };
    { id = 2; _client_id = 101; _datetime = "2023-01-02"; status = Complete; origin = P };
    { id = 3; _client_id = 102; _datetime = "2023-01-03"; status = Complete; origin = O };
    { id = 4; _client_id = 103; _datetime = "2023-01-04"; status = Cancelled; origin = P }
  ] in
  let order_items = [
    { order_id = 1; _product_id = 200; _quantity = 2; price = 10.0; tax = 1.0 };
    { order_id = 2; _product_id = 201; _quantity = 3; price = 15.0; tax = 1.5 };
    { order_id = 3; _product_id = 202; _quantity = 1; price = 20.0; tax = 2.0 };
    { order_id = 4; _product_id = 203; _quantity = 4; price = 25.0; tax = 2.5 }
  ] in
  let joined = inner_join orders order_items in
  
  (* Test flag 1: both status and origin filter *)
  let result1 = apply_filters joined Complete O 1 in
  assert_equal 1 (List.length result1);
  let (order, _) = List.hd result1 in
  assert_equal 3 order.id;
  
  (* Test flag 2: only status filter *)
  let result2 = apply_filters joined Complete O 2 in
  assert_equal 2 (List.length result2);
  
  (* Test flag 3: only origin filter *)
  let result3 = apply_filters joined Complete O 3 in
  assert_equal 2 (List.length result3);
  
  (* Test flag 4: no filter *)
  let result4 = apply_filters joined Complete O 4 in
  assert_equal 4 (List.length result4);
  
  (* Test invalid flag *)
  assert_raises (Failure "Could not filter records properly, as flag provided does not exist.") 
    (fun () -> apply_filters joined Complete O 5)

let test_get_unique_ids _ =
  let orders = [
    { id = 1; _client_id = 100; _datetime = "2023-01-01"; status = Pending; origin = O };
    { id = 1; _client_id = 100; _datetime = "2023-01-01"; status = Pending; origin = O };
    { id = 2; _client_id = 101; _datetime = "2023-01-02"; status = Complete; origin = P }
  ] in
  let order_items = [
    { order_id = 1; _product_id = 200; _quantity = 2; price = 10.0; tax = 1.0 };
    { order_id = 1; _product_id = 201; _quantity = 1; price = 20.0; tax = 2.0 };
    { order_id = 2; _product_id = 202; _quantity = 3; price = 15.0; tax = 1.5 }
  ] in
  let joined = [
    (List.nth orders 0, List.nth order_items 0);
    (List.nth orders 1, List.nth order_items 1);
    (List.nth orders 2, List.nth order_items 2)
  ] in
  let result = get_unique_ids joined IntSet.empty in
  assert_equal 2 (IntSet.cardinal result);
  assert_bool "Set should contain ID 1" (IntSet.mem 1 result);
  assert_bool "Set should contain ID 2" (IntSet.mem 2 result)

let test_tuple_to_record _ =
  let expected = { order_id = 1; total_amount = 30.0; total_taxes = 3.0 } in
  let actual = tuple_to_record 1 (30.0, 3.0) in
  assert_equal expected actual

let test_process_joined_records _ =
  let orders = [
    { id = 1; _client_id = 100; _datetime = "2023-01-01"; status = Pending; origin = O };
    { id = 2; _client_id = 101; _datetime = "2023-01-02"; status = Complete; origin = P }
  ] in
  let order_items = [
    { order_id = 1; _product_id = 200; _quantity = 2; price = 10.0; tax = 1.0 };
    { order_id = 1; _product_id = 201; _quantity = 1; price = 20.0; tax = 2.0 };
    { order_id = 2; _product_id = 202; _quantity = 3; price = 15.0; tax = 1.5 }
  ] in
  let joined = [
    (List.nth orders 0, List.nth order_items 0);
    (List.nth orders 0, List.nth order_items 1);
    (List.nth orders 1, List.nth order_items 2)
  ] in
  let unique_ids = IntSet.of_list [1; 2] in
  let result = process_joined_records joined unique_ids in
  assert_equal 2 (List.length result);
  
  (* Find and check record for order_id 1 *)
  let order1 = List.find (fun r -> r.order_id = 1) result in
  assert_equal 30.0 order1.total_amount;
  assert_equal 3.0 order1.total_taxes;
  
  (* Find and check record for order_id 2 *)
  let order2 = List.find (fun r -> r.order_id = 2) result in
  assert_equal 15.0 order2.total_amount;
  assert_equal 1.5 order2.total_taxes

let test_joined_records_to_strlist _ =
  let records = [
    { order_id = 1; total_amount = 30.0; total_taxes = 3.0 };
    { order_id = 2; total_amount = 15.0; total_taxes = 1.5 }
  ] in
  let expected = ["1,30.00,3.00\n"; "2,15.00,1.50\n"] in
  let result = joined_records_to_strlist records in
  assert_equal expected result

let suite =
  "suite" >::: [
    "test_match_status_type" >:: test_match_status_type;
    "test_match_origin_type" >:: test_match_origin_type;
    "test_convert_order" >:: test_convert_order;
    "test_convert_orderItem" >:: test_convert_orderItem;
    "test_cut_first_element" >:: test_cut_first_element;
    "test_cut_last_element" >:: test_cut_last_element;
    "test_convert_to_recordlist" >:: test_convert_to_recordlist;
    "test_inner_join" >:: test_inner_join;
    "test_filter_by_status" >:: test_filter_by_status;
    "test_filter_by_origin" >:: test_filter_by_origin;
    "test_check_sysargs" >:: test_check_sysargs;
    "test_apply_filters" >:: test_apply_filters;
    "test_get_unique_ids" >:: test_get_unique_ids;
    "test_tuple_to_record" >:: test_tuple_to_record;
    "test_process_joined_records" >:: test_process_joined_records;
    "test_joined_records_to_strlist" >:: test_joined_records_to_strlist
  ]

let () = run_test_tt_main suite
