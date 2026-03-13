from typing import Callable, Optional, Any
from .core import run_ordered # type: ignore
import typing

try:
    import pandas as pd # type: ignore
    DataFrame = pd.DataFrame
except ImportError:
    pd = typing.cast(typing.Any, None)
    DataFrame = typing.Any

def _check_pandas():
    if pd is None:
        raise ImportError(
            "Pandas is required for data helpers. Install ezmp with 'pip install ezmp[data]' "
            "or 'pip install pandas'."
        )

def map_df(
    target_func: Callable,
    df: DataFrame,
    use_threads: bool = False,
    max_workers: Optional[int] = None,
    desc: str = "Processing DataFrame rows"
) -> DataFrame:
    """
    Applies a function to each row of a pandas DataFrame concurrently.
    Because applying heavy transformations is usually CPU bound, defaults to ProcessPool.
    Returns a new DataFrame with a new column 'ezmp_result' containing the return values.
    """
    _check_pandas()
    
    # We pass rows as dictionaries to the target function to make it easy to use
    rows_as_dicts = df.to_dict('records')
    
    # Run concurrently (ordered so we can just append it back as a column)
    results = run_ordered(
        target_func=target_func,
        items=rows_as_dicts,
        use_threads=use_threads,
        max_workers=max_workers,
        desc=desc
    )
    
    # Return a copy of the dataframe with the results appended
    result_df = df.copy()
    result_df['ezmp_result'] = results # type: ignore
    return result_df
