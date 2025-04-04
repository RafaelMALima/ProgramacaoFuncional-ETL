# OCaml ETL Project

This project implements an Extract, Transform, Load (ETL) pipeline in OCaml. It processes order data from CSV files, applies various filters and transformations, and outputs the results to both a CSV file and an SQLite database.

## Overview

The ETL pipeline:
1. **Extracts** data from CSV files hosted on GitHub via HTTP
2. **Transforms** the data by:
   - Converting CSV rows to OCaml records
   - Joining orders with their corresponding items
   - Filtering based on command-line arguments (status and origin)
   - Calculating total amounts and taxes for each order
3. **Loads** the processed data into:
   - A CSV file (`output.csv`)
   - An SQLite database (`output.db3`)

## Project Structure

```
.
├── bin/             # Command-line executable
│   ├── dune         # Dune configuration for executable
│   └── main.ml      # Main entry point
├── lib/             # Core library
│   ├── dune         # Dune configuration for library
│   ├── helper.ml    # Pure functions for data processing
│   └── impure.ml    # I/O operations (CSV, HTTP, SQLite)
├── test/            # Unit tests
│   ├── dune         # Dune configuration for tests
│   └── test_ETL.ml  # Test suite
└── dune-project     # Project configuration
```

## Features

- Functional programming approach using `map`, `filter`, and `reduce` operations
- Separation of pure and impure functions
- Modular code organization using dune
- Command-line filtering options:
  - Filter by order status (Pending, Complete, Cancelled)
  - Filter by order origin (Online, Phone)
- Comprehensive unit tests for pure functions
- Documented code with docstrings

## Dependencies

- OCaml 
- Dune (build system)
- ocurl (for HTTP requests)
- sqlite3 (for database operations)
- OUnit2 (for testing)

## Installation

Add these dependencies to your opam environment:

```bash
opam install dune ocurl sqlite3 ounit2
```

## Building the Project

```bash
dune build
```

## Running the ETL Pipeline

Basic usage:

```bash
dune exec ETL
```

With filters:

```bash
# Filter by status
dune exec ETL -- Pending

# Filter by origin
dune exec ETL -- P

# Filter by both status and origin
dune exec ETL -- Complete O
```

Status options:
- `Pending`
- `Complete`
- `Cancelled`

Origin options:
- `O` (Online)
- `P` (Phone)

## Running Tests

```bash
dune test
```

## Input Data Format

The ETL process expects two input CSV files:

1. **Order CSV**:
   - `id`: Order identifier
   - `client_id`: Client identifier
   - `datetime`: Timestamp when the order was placed
   - `status`: Order status (Pending, Complete, Cancelled)
   - `origin`: Channel through which the order was placed (O, P)

2. **Order Item CSV**:
   - `order_id`: Reference to the order
   - `product_id`: Product identifier
   - `quantity`: Quantity of the product ordered
   - `price`: Price per unit
   - `tax`: Tax amount for this item

## Output Format

The output is a CSV file and SQLite table containing:
- `order_id`: Order identifier
- `total_amount`: Total amount for the order (sum of prices)
- `total_taxes`: Total tax amount for the order (sum of taxes)

## Requirements Checklist

### Core Requirements

- [x] 1. Project implemented in OCaml
- [x] 2. Uses map, reduce, and filter functions for calculations
- [x] 3. Contains functions for reading and writing CSV files
- [x] 4. Separates pure functions from impure functions
- [x] 5. Loads input data into a list of Records structure
- [x] 6. Uses Helper Functions to load fields into Records
- [x] 7. Project report describing the construction process (not found in the repository)

### Additional Requirements

- [x] 1. Reads input data from a file hosted on the internet via HTTP
- [x] 2. Saves output data to an SQLite database
- [x] 3. Processes input tables together via inner join operation
- [x] 4. Project organized using dune build system
- [x] 5. All functions documented with docstrings
- [ ] 6. Additional output with average revenue and taxes grouped by month and year
- [x] 7. Complete test files for pure functions

## Code Examples

### Loading CSV Data from HTTP
```ocaml
let order_records = Impure.read_csv_from_http "https://raw.githubusercontent.com/RafaelMALima/ProgramacaoFuncional-ETL/refs/heads/main/order.csv"
  |> Helper.convert_to_recordlist Helper.convert_order
```

### Performing Inner Join
```ocaml
let joined_records = Helper.inner_join order_records orderItem_records
```

### Applying Filters
```ocaml
let status, origin, flag = Helper.check_sysargs Sys.argv
let filter_records = Helper.apply_filters joined_records status origin flag
```

### Processing Records
```ocaml
let processed_joined_records = Helper.process_joined_records filter_records unique_ids
```

### Writing Output to SQLite
```ocaml
let a = Impure.open_sqlite_db "output"
let _res = Impure.write_output_to_sqlite a list_of_str_of_str
```

README file generated with assistance of AI tools
