from typing import Callable, Optional, Any
from .dataframe import map_df, _check_pandas, DataFrame, pd  # type: ignore


def map_csv(
    target_func: Callable,
    file_path: str,
    output_path: Optional[str] = None,
    use_threads: bool = False,
    max_workers: Optional[int] = None,
    desc: str = "Processing CSV rows",
    chunksize: Optional[int] = None,
    memoize: bool = False,
    shared_state: Optional[Any] = None,
    **read_csv_kwargs,
) -> DataFrame:
    """
    Reads a CSV, processes its rows concurrently, and optionally saves the result.
    If `chunksize` is provided, it returns a generator that yields processed chunk DataFrames.
    This allows massive CSV processing without hitting RAM limits.
    """
    _check_pandas()

    if chunksize is not None:

        def chunk_generator():
            for i, chunk in enumerate(
                pd.read_csv(file_path, chunksize=chunksize, **read_csv_kwargs)
            ):
                yield map_df(
                    target_func=target_func,
                    df=chunk,
                    use_threads=use_threads,
                    max_workers=max_workers,
                    desc=f"{desc} (chunk {i + 1})",
                    memoize=memoize,
                    shared_state=shared_state,
                )

        return chunk_generator()
    else:
        df = pd.read_csv(file_path, **read_csv_kwargs)
        result_df = map_df(
            target_func=target_func,
            df=df,
            use_threads=use_threads,
            max_workers=max_workers,
            desc=desc,
            memoize=memoize,
            shared_state=shared_state,
        )

        if output_path is not None:
            result_df.to_csv(output_path, index=False)

        return result_df
