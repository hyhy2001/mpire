from typing import Callable, Optional, Any, List, Iterator
from .dataframe import map_df, _check_pandas, DataFrame, pd # type: ignore
import typing

def map_excel(
    target_func: Callable,
    file_path: str,
    output_path: Optional[str] = None,
    use_threads: bool = False,
    max_workers: Optional[int] = None,
    desc: str = "Processing Excel rows"
) -> DataFrame:
    """
    Reads an Excel file, processes its rows concurrently, and optionally saves the result.
    """
    _check_pandas()
    
    df = pd.read_excel(file_path)
    result_df = map_df(
        target_func=target_func, 
        df=df, 
        use_threads=use_threads, 
        max_workers=max_workers, 
        desc=desc
    )
    
    if output_path is not None:
        result_df.to_excel(output_path, index=False)
        
    return result_df

def _process_single_excel(file_path, target_func, read_kwargs):
    df = pd.read_excel(file_path, **read_kwargs)
    return target_func(df)

def map_excel_chunks(
    target_func: Callable,
    file_path: str,
    chunksize: int = 1000,
    use_threads: bool = False,
    max_workers: Optional[int] = None,
    desc: str = "Processing Excel chunks"
) -> Iterator[DataFrame]:
    """
    Reads an Excel file lazily in chunks, to prevent Out-Of-Memory (OOM) crashes
    on massive data matrices (e.g., millions of cells).
    
    Returns a Generator yielding processed DataFrame chunks.
    Dependencies: openpyxl
    """
    _check_pandas()
    import openpyxl # type: ignore
    
    def chunk_generator():
        # Use read_only=True for streaming, drastically reducing memory
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        ws = wb.active
        
        # Get headers
        rows_iter = ws.iter_rows(values_only=True) # type: ignore
        headers = next(rows_iter)
        
        chunk_data = []
        chunk_index = 1
        
        for row in rows_iter:
            chunk_data.append(row)
            if len(chunk_data) >= chunksize:
                # Convert this chunk to a DataFrame
                df_chunk = pd.DataFrame(chunk_data, columns=headers)
                
                # Apply the function concurrently to the rows in this chunk
                processed_df = map_df(
                    target_func=target_func,
                    df=df_chunk,
                    use_threads=use_threads,
                    max_workers=max_workers,
                    desc=f"{desc} (chunk {chunk_index})"
                )
                yield processed_df
                
                chunk_data = []
                chunk_index = chunk_index + 1 # type: ignore
                
        # Process the final remaining chunk if any
        if chunk_data:
            df_chunk = pd.DataFrame(chunk_data, columns=headers)
            yield map_df(
                target_func=target_func,
                df=df_chunk,
                use_threads=use_threads,
                max_workers=max_workers,
                desc=f"{desc} (chunk {chunk_index})"
            )
            
        wb.close()
        
    return chunk_generator()

def map_excel_files(
    target_func: Callable[[DataFrame], Any],
    directory: str,
    recursive: bool = False,
    use_threads: bool = False,
    max_workers: Optional[int] = None,
    desc: str = "Scraping Excel files",
    **read_excel_kwargs
) -> List[Any]:
    """
    Finds all Excel (.xlsx, .xls) files in a directory and applies `target_func`
    to each loaded DataFrame concurrently.
    
    Args:
        target_func: Function to apply to each loaded pd.DataFrame.
        directory: The directory containing Excel files.
        recursive: Whether to search subdirectories.
        use_threads: If True, uses threads; otherwise processes.
        max_workers: Max workers for concurrent execution.
        desc: Progress bar description.
        **read_excel_kwargs: Additional arguments passed to `pd.read_excel`.
        
    Returns:
        A list of results from `target_func`.
    """
    from . import files # type: ignore
    import functools
    
    wrapper = functools.partial(
        _process_single_excel, 
        target_func=target_func, 
        read_kwargs=read_excel_kwargs
    )

    # Use files.map_dir to prevent glob logic duplication
    return files.map_dir(
        target_func=wrapper,
        dir_path=directory,
        pattern="*.xls*" if not recursive else "**/*.xls*",
        recursive=recursive,
        use_threads=use_threads,
        max_workers=max_workers,
        desc=desc
    )
