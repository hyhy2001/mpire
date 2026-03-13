import ezmp # type: ignore
import pytest # type: ignore

def square(x):
    return x * x

def double(x):
    return x * 2

def check_even(num):
    return num % 2 == 0

def test_core_unordered():
    items = [1, 2, 3]
    res = ezmp.run(square, items)
    assert set(res) == {1, 4, 9}

def test_core_ordered():
    items = [1, 2, 3]
    res = ezmp.run_ordered(double, items)
    assert res == [2, 4, 6]

def add(x, y):
    return x + y

def test_core_multi_unordered():
    items = [(1, 2), (3, 4), (5, 6)]
    res = ezmp.run_multi(add, items)
    assert set(res) == {3, 7, 11}

def test_core_multi_ordered():
    items = [(1, 2), (3, 4), (5, 6)]
    res = ezmp.run_multi_ordered(add, items)
    assert res == [3, 7, 11]

def test_net_ordered():
    res = ezmp.net.map_urls(check_even, [1, 2, 3, 4, 5])
    assert res == [False, True, False, True, False]

def test_logs_chunk_reading(tmp_path):
    # Create a dummy log file
    log_file = tmp_path / "huge_log.txt"
    log_file.write_text("line1\nline2\nline3\nline4\nline5\n")
    
    chunks = list(ezmp.logs.read_chunks(str(log_file), chunk_size=2))
    assert len(chunks) == 3
    assert chunks[0] == ["line1\n", "line2\n"]
    assert chunks[1] == ["line3\n", "line4\n"]
    assert chunks[2] == ["line5\n"]

import typing

def dummy_sta_extractor(chunk: typing.List[str], state: typing.Dict[str, typing.Any]) -> typing.Generator[typing.Dict[str, typing.Any], None, None]:
    if not state:
        state.update({'in_path': False, 'current_start': None})
        
    for line in chunk:
        if line.startswith("Startpoint:"):
            state['in_path'] = True
            state['current_start'] = line.split()[1]
        elif line.startswith("slack (VIOLATED)") and state['in_path']:
            val = float(line.split()[-1])
            yield {'start': state['current_start'], 'slack': val}
            state['in_path'] = False
            state['current_start'] = None

def test_logs_parse_blocks(tmp_path):
    log_file = tmp_path / "sta_log.rpt"
    log_data = """
Startpoint: reg_A
some random
timing matrix data
slack (VIOLATED) -0.5
Startpoint: reg_B
more data
slack (VIOLATED) -1.2
Startpoint: reg_C
split across chunk
"""
    log_file.write_text(log_data.strip() + "\nslack (VIOLATED) -0.1\n")
    
    # Intentionally use a tiny chunk size of 3 lines to force state carrying across chunks
    results = list(ezmp.logs.parse_blocks(str(log_file), dummy_sta_extractor, chunk_size=3, initial_state={}))
    
    assert len(results) == 3
    assert results[0] == {'start': 'reg_A', 'slack': -0.5}
    assert results[1] == {'start': 'reg_B', 'slack': -1.2}
    assert results[2] == {'start': 'reg_C', 'slack': -0.1}


def sum_col_a(df):
    return df['A'].sum()

def test_data_map_excel_files(tmp_path):
    pd = pytest.importorskip("pandas")
    
    # Create two dummy excel files
    df1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    df2 = pd.DataFrame({'A': [10, 20], 'B': [30, 40]})
    
    file1 = tmp_path / "data1.xlsx"
    file2 = tmp_path / "data2.xlsx"
    df1.to_excel(str(file1), index=False)
    df2.to_excel(str(file2), index=False)
        
    res = ezmp.excel.map_excel_files(sum_col_a, str(tmp_path))
    
    # Ordered is not guaranteed, but we can check the set
    assert set(res) == {3, 30}

def process_chunk(row):
    return row['val'] + 0

def test_data_map_csv_chunks(tmp_path):
    pd = pytest.importorskip("pandas")
    
    # Create a large dummy CSV
    df = pd.DataFrame({'val': range(10)})
    csv_file = tmp_path / "large.csv"
    df.to_csv(str(csv_file), index=False)
    
    # Read in chunks of 5
    chunks_gen = ezmp.csv.map_csv(process_chunk, str(csv_file), chunksize=5)
    
    # We should get a generator, not a dataframe directly
    import types
    assert isinstance(chunks_gen, types.GeneratorType)
    
    # Evaluate it
    res = list(chunks_gen)
    assert len(res) == 2
    
    # Chunk 1 (0,1,2,3,4) sums to 10
    # Chunk 2 (5,6,7,8,9) sums to 35
    assert sum(res[0]['ezmp_result'].values) == 10
    assert sum(res[1]['ezmp_result'].values) == 35


def count_true(row_dict):
    # row_dict will look like {'A': True, 'B': False}
    return sum(1 for val in row_dict.values() if val is True)

def test_data_map_excel_chunks(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("openpyxl")
    
    # Create a dummy Excel matrix
    df = pd.DataFrame({
        'A': [True, False, True, False, True],
        'B': [True, True, False, False, True]
    })
    
    excel_file = tmp_path / "matrix_chunk.xlsx"
    df.to_excel(str(excel_file), index=False)
    
    import types
    # Read in chunks of 2 rows
    chunks_gen = ezmp.excel.map_excel_chunks(count_true, str(excel_file), chunksize=2)
    assert isinstance(chunks_gen, types.GeneratorType)
    
    res = list(chunks_gen)
    
    # 5 rows total / 2 = 3 chunks
    assert len(res) == 3
    
    # Chunk 1:
    # Row 0: True + True = 2
    # Row 1: False + True = 1
    assert list(res[0]['ezmp_result'].values) == [2, 1]
    
    # Chunk 2:
    # Row 2: True + False = 1
    # Row 3: False + False = 0
    assert list(res[1]['ezmp_result'].values) == [1, 0]
    
    # Chunk 3:
    # Row 4: True + True = 2
    assert list(res[2]['ezmp_result'].values) == [2]

def test_files_write_stream(tmp_path):
    output_file = tmp_path / "stream_output.csv"
    
    # A generator simulating a massive data stream
    def huge_stream():
        for i in range(5):
            yield {"id": i, "val": i * 10}
            
    # Custom transformer
    def to_csv_line(item):
        return f"{item['id']},{item['val']}"
        
    ezmp.files.write_stream(
        results_generator=huge_stream(),
        output_path=str(output_file),
        transform=to_csv_line
    )
    
    lines = output_file.read_text().strip().split('\n')
    assert len(lines) == 5
    assert lines[0] == "0,0"
    assert lines[-1] == "4,40"

