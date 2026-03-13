# Easy Multiprocessing (ezmp)

`ezmp` is a beginner-friendly Python package that makes concurrent execution (multiprocessing and multithreading) incredibly simple. It removes the boilerplate associated with `concurrent.futures`, and adds safe error handling so that a single failing item doesn't crash your entire script.
**Recently updated for Massive Data Processing:** Effortlessly parse gigabyte-sized server logs and stream multi-million cell boolean matrices without crashing your system's RAM.

---

## 🚀 Features

- **Zero Boilerplate**: Run CPU-heavy tasks or Network I/O tasks instantly.
- **Ordered/Unordered**: Choose between strict order preservation or maximum speed.
- **Multi-arguments**: Pass tuples of arguments seamlessly (`run_multi`), avoiding lambda wrappers.
- **Massive File Chunking (No OOM)**: Stream gigabyte-sized Log files or Excel Matrices into manageable chunks.
- **Custom Block Extraction**: Inject your own Python parsing logic into log readers to extract multi-line blocks dynamically.
- **Stream Output**: Pipe results directly to output files line-by-line using generators (`write_stream`).
- **Robust Error Handling**: Wraps exceptions in `ErrorResult` instead of crashing the pool.

---

## 📦 Installation

Install the base package:
```bash
pip install ezmp
```

**Optional Dependencies:**
If you want to use the `data` (Excel/CSV) or `net` modules, you can install the optional dependencies:
```bash
pip install "ezmp[data]"  # Installs pandas, openpyxl
pip install "ezmp[net]"   # Installs requests
pip install "ezmp[all]"   # Installs everything
```

---

## 📖 Code Examples: Basic Workflows

These fundamental examples cover the core daily use-cases for `ezmp`.

### 1. ⚡ Basic CPU Multiprocessing (`run`)
The simplest way to use `ezmp`. By default, it uses Python processes to max out your CPU cores, replacing standard `map()` loops.

```python
import ezmp
import time

def heavy_computation(x):
    time.sleep(1) # simulate work
    return x * x

items = [1, 2, 3, 4, 5, 6, 7, 8]
# Returns quickly across all cores!
results = ezmp.run(heavy_computation, items)
print(results) 
```

### 2. 🔀 Guaranteeing Result Order (`run_ordered`)
Standard parallel execution returns results in the order they complete to maximize speed. If your downstream logic requires the output list to perfectly match the input list's order, use `run_ordered`.

```python
import ezmp

items = [10, 1, 5, 3]

# The output list strictly maintains the [10, 1, 5, 3] order mapping
results = ezmp.core.run_ordered(lambda x: x*2, items)
```

### 3. 📂 Processing Entire Directories (`map_dir`)
Need to process hundreds of image files or JSONs? Use `map_dir` with Threads to breeze through disk I/O without manual `os.listdir` loops.

```python
import ezmp
import os

def process_image(filepath):
    # e.g., Read file, resize, save
    size = os.path.getsize(filepath)
    return f"{filepath}: {size} bytes"

# Uses ThreadPool natively since Disk IO is the bottleneck
results = ezmp.files.map_dir(
    process_image,
    dir_path="images/",
    pattern="*.png",
    use_threads=True
)
```

### 4. 💽 Streaming Results Directly to Disk (`write_stream`)
If your parallel workers are generating massive amounts of data, keeping the `results` list in RAM will crash your machine. Pipe a Generator directly into `write_stream` to save it line-by-line.

```python
import ezmp

# Returns a lazy Generator instead of a List
generator = ezmp.core.run_stream(lambda x: x*2, range(1000000))

# Consumes the generator and writes to disk safely with near-zero RAM
ezmp.files.write_stream(
    results_generator=generator, 
    output_path="results.txt",
    transform=lambda item: f"Result: {item}" 
)
```

### 5. 🛡️ Safe Nested Execution (Auto-Fallback)
Running multiprocessing pools inside other multiprocessing pools causes severe bugs (like `daemonic processes are not allowed to have children`). `ezmp` detects these nested calls automatically and gracefully downshifts the inner function to a sequential loop, shielding you from crashes.

```python
import ezmp

def process_file(file_path):
    # ezmp automatically detects it's inside a Worker and runs sequentially!
    lines = open(file_path).readlines()
    return ezmp.run(lambda x: x.upper(), lines) # No crashes here

folders = ["dir1/", "dir2/"]
# The outer pool runs fine across all cores
ezmp.run(process_file, folders)
```

---

## 💡 Code Examples: The "Mega" Pro Patterns

These 6 high-value use cases demonstrate how `ezmp` transforms complex Data Engineering into simple one-liners.

### 1. 🐼 Fast-Track Pandas DataFrames (CPU Bound)
`df.apply()` is notoriously single-core and slow. Use `map_df` to obliterate calculation times across millions of rows by utilizing 100% of your CPU cores.

```python
import ezmp
import pandas as pd

def heavy_financial_calc(row_dict):
    # E.g., Risk scoring algorithm, regex matches, string ops
    return row_dict['Price'] * row_dict['Volume'] / 1.5

df = pd.DataFrame({"Price": [10, 20, 30], "Volume": [100, 200, 300]})

# Spins up all CPU cores to crunch the DataFrame concurrently!
# The returned DataFrame has a brand new column: 'ezmp_result'
result_df = ezmp.dataframe.map_df(heavy_financial_calc, df)
print(result_df)
```

### 2. 🌐 High-Speed API Scraping (IO-Bound)
Stop waiting for Network requests. Use Threads (`use_threads=True`) to make 1000s of API or database calls simultaneously without burning CPU.

```python
import ezmp
import requests

def scrape_title(url):
    response = requests.get(url, timeout=5)
    return response.text.split("<title>")[1].split("</title>")[0]

urls = ["https://example.com", "https://python.org", "https://github.com"]

# Spawns a massive ThreadPool. Ideal for Network/Disk IO.
titles = ezmp.net.map_urls(scrape_title, urls, max_workers=50)
print(titles)
```

### 3. 📜 Saving Servers from 100GB Kaggle CSVs (Chunking)
Normal `pd.read_csv()` will crash your machine (OOM) on a 100GB file. `map_csv` chunks the file lazily, capping RAM at ~50MB.

```python
import ezmp

def analyze_row(row_dict):
    if row_dict['Status'] == 'ERROR':
        return 1
    return 0

# Lazily reads via chunks. It yields a generator!
# Your RAM usage will sit perfectly at 0%.
results_gen = ezmp.csv.map_csv(
    analyze_row, 
    "massive_data_100GB.csv", 
    chunksize=10000 
)

total_errors = sum(results_gen)
print(f"Millions of rows analyzed. Total Errors: {total_errors}")
```

### 4. 🪵 Multi-Line Server Error Log Parsing
Extracting stack traces from 50GB Apache/Docker text logs is brutal. Use `logs.parse_blocks` to segment custom blocks and process them concurrently.

```python
import ezmp

# 1. Define how to detect a chunk boundary in your custom log
def stack_trace_extractor(chunk_lines):
    # E.g., Group lines that start with [ERROR] and following traces
    return [block for block in chunk_lines if "[ERROR]" in block]

# 2. Process the extracted blocks
def analyze_stack_trace(error_block):
    return "Database Timeout" if "Connection refused" in error_block else "Unknown"

# Tears through gigantic text files block-by-block using Regex/Extractors
results = ezmp.logs.parse_blocks(
    analyze_stack_trace,
    "production_server.log",
    block_extractor=stack_trace_extractor
)
```

### 5. 🛠 Multi-Arguments (`run_multi`)
Want to map an image-resizing function that requires varying width and height per file? Pack your parameters into tuples.

```python
import ezmp

def crop_video(file_path, start_time, end_time):
    # Pretend we are calling FFMPEG here
    return f"Cropped {file_path} from {start_time}s to {end_time}s."

tasks = [
    ("vid1.mp4", 10, 50),
    ("vid2.mp4", 0, 15),
    ("vid3.mp4", 120, 200)
]

# `run_multi` automatically unpacks each tuple and feeds it to `crop_video`
results = ezmp.core.run_multi(crop_video, tasks)
print(results)
```

### 6. 🗄️ Parallel Processing Multiple Massive Excel Files
If you have **100s of massive Excel matrices** (e.g., 100 Million cells each), attempting to load them all into RAM concurrently using `pd.read_excel` will destroy your system (OOM). 

Instead, harness the power of `ezmp`'s Auto-Fallback feature by nesting a chunked reader inside a parallel directory scanner:

```python
import ezmp

# 1. Row processor (Runs Sequentially on each chunk row inside the file worker)
def count_true_clocks(row_dict):
    return sum(1 for val in row_dict.values() if val is True)

# 2. File processor (The parent target function)
def process_massive_file(file_path):
### 6. 🗄️ The Multi-Mega File Scraping Engine
Wait, you have 50 Excel files scattered across 12 nested folders and you want to extract `df` chunks from all of them at once?
`files.map_dir()` handles the OS traversal, and assigns chunks of specific files to different CPU cores automatically.
```python
import ezmp

def extract_revenue(df):
    # Process the dataframe chunk
    return df['Revenue'].sum()

results = ezmp.excel.map_excel_files(
    target_func=extract_revenue,
    directory="./financial_reports_2024",
    recursive=True,            # Deep dive into nested folders
    use_threads=False,         # Use Processes for CPU-heavy pandas aggregations
    max_workers=8              # 8 cores tearing through 50 files
)
total_revenue = sum(results)
print(f"Total Combined Revenue: {total_revenue}")
```

### 7. 🚀 Native Multi-Core Excel Formula Evaluation
When system-generated Excel files contain raw formulas (e.g., `=VLOOKUP(A2, Sheet2!A:Z, 2)`) that haven't been opened and pre-calculated by Microsoft Excel, standard Python libraries return unreadable strings or `None`.
`ezmp` solves this by boasting a **Native Multi-Process AST Formula Engine**.
Just pass `evaluate_formulas=True`, and `ezmp` will build an Abstract Syntax Tree, compute the math, logic, text, or lookups perfectly, and **globally cache** massive lookup tables across your 24 CPU cores so performance never drops!
```python
import ezmp

def process_evaluated_row(df):
    # 'Price' now contains the perfectly evaluated VLOOKUP float, not the raw "=VLOOKUP(...)" string!
    return df[df['Price'] > 100]

computed_chunks = ezmp.excel.map_excel_chunks(
    target_func=process_evaluated_row,
    file_path="massive_uncached_invoice.xlsx",
    chunksize=5000,
    use_processes=True,  
    max_workers=10,
    evaluate_formulas=True   # 🚀 The magic flag! Parses and Computes ASTs natively!
)

for clean_df in computed_chunks:
    print(clean_df.head())
```

### 8. 🧠 Auto-Memoization & Multiprocess Shared State
When parsing 10 million rows, you often hit duplicated items, or you need to tally errors globally without corrupting counts across threads.
By passing `memoize=True`, `ezmp` internally spins up an Inter-Process Communication (IPC) dictionary. Duplicate rows skip execution natively in `O(1)` time.
By passing `shared_state=manager.dict()`, you inject a globally shared object accessible instantly from any worker core.

```python
import ezmp
import multiprocessing
import hashlib

def crunch_massive_data(row, shared_state=None):
    # Safely aggregate cross-process states WITHOUT race conditions
    if "ERROR" in str(row) and shared_state is not None:
        shared_state["errors"] = shared_state.get("errors", 0) + 1
        
    # Massive CPU bottleneck
    return hashlib.sha256(str(row).encode()).hexdigest()

# Spin up a global Multiprocessing tracker
manager = multiprocessing.Manager()
global_tracker = manager.dict({'errors': 0})

results = ezmp.dataframe.map_df(
    crunch_massive_data,
    df=massive_pandas_df,
    use_threads=False,    
    max_workers=24,       
    memoize=True,         # ✨ Magic 1: Skips execution if the target row is identical to a previous one!
    shared_state=global_tracker  # ✨ Magic 2: Aggregates global error tallies synchronously!
)

print(f"Total distributed errors caught: {global_tracker['errors']}")
```

---

## 4. Full API Reference

### `ezmp.core`
- `run(func, items, use_threads=False, max_workers=None)`: Core map. CPU bound by default.
- `run_ordered(...)`: Same as `run`, but guarantees order.
- `run_stream(func, items, ...)`: Returns a lazy-evaluated Generator. Yields results one-by-one as they complete. Ideal for saving RAM.
- `run_multi(func, items_tuples)`: Accepts an iterable of tuples and unpacks them `func(*item)`.
- `run_multi_ordered(...)`: Same as multi, but strict order.

### `ezmp.files`
- `map_files(func, file_list, use_threads=True)`
- `map_dir(func, directory, pattern, ...)`
- `write_stream(results_generator, output_path, transform=None)`: Safely writes generated items to a file sequentially.

### `ezmp.dataframe`
- `map_df(func, df)`: Process a DataFrame row-by-row concurrently.

### `ezmp.csv`
- `map_csv(func, file_path, chunksize=None)`: Specify `chunksize` to return a Generator for low-RAM streaming.

### `ezmp.excel`
- `map_excel(...)`: Read & process single Excel file (High RAM usage).
- `map_excel_files(func, directory)`: Process all excel files concurrently.
- `map_excel_chunks(func, file_path, chunksize=1000)`: Read a massive Excel file lazily and process chunk-by-chunk (Low RAM footprint).

### `ezmp.net`
- `map_urls(func, urls)`: Downloads/processes URLs using threads.

### `ezmp.logs`
- `read_chunks(file_path, chunk_size=1000)`: Yields raw lists of text lines.
- `parse_blocks(file_path, block_extractor, chunk_size)`: Feeds text lines to your custom `block_extractor` function for complex stateful parsing.
