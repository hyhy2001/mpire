from pathlib import Path
from typing import Callable, Union, List, Any, Optional
import typing
from .core import run, run_ordered  # type: ignore


def map_files(
    target_func: Callable,
    file_paths: List[Union[str, Path]],
    ordered: bool = True,
    use_threads: bool = True,  # Default to threads for I/O
    max_workers: Optional[int] = None,
    desc: str = "Processing files",
    memoize: bool = False,
    shared_state: Optional[Any] = None,
) -> List[Any]:
    """
    Applies a function to a list of files concurrently.
    Defaults to ordered ThreadPool processing, because file operations are typically I/O bound.
    """
    runner = run_ordered if ordered else run
    return runner(
        target_func=target_func,
        items=file_paths,
        use_threads=use_threads,
        max_workers=max_workers,
        desc=desc,
        memoize=memoize,
        shared_state=shared_state,
    )


def map_dir(
    target_func: Callable,
    dir_path: Union[str, Path],
    pattern: str = "*",
    recursive: bool = False,
    ordered: bool = True,
    use_threads: bool = True,
    max_workers: Optional[int] = None,
    desc: str = "Processing directory files",
    memoize: bool = False,
    shared_state: Optional[Any] = None,
) -> List[Any]:
    """
    Applies a function to all files matching a pattern in a directory.
    """
    dir_path = Path(dir_path)

    # Gather files
    if recursive:
        files = list(dir_path.rglob(pattern))
    else:
        files = list(dir_path.glob(pattern))

    # Filter out directories
    valid_files: List[Union[str, Path]] = [f for f in files if f.is_file()]

    if not valid_files:
        from .utils import logger  # type: ignore

        logger.warning(f"No files found in {dir_path} matching pattern '{pattern}'")
        return []

    return map_files(
        target_func=target_func,
        file_paths=valid_files,
        ordered=ordered,
        use_threads=use_threads,
        max_workers=max_workers,
        desc=desc,
        memoize=memoize,
        shared_state=shared_state,
    )


def write_stream(
    results_generator: typing.Iterable[Any],
    output_path: Union[str, Path],
    mode: str = "w",
    encoding: str = "utf-8",
    transform: Any = None,
) -> None:
    """
    Safely writes a large stream (generator) of results to a file, row-by-row.
    This prevents Out-Of-Memory (OOM) errors when processing millions of items.

    Args:
        results_generator: An iterable or generator yielding results.
        output_path: The file to write to.
        mode: File open mode ('w' for overwrite, 'a' for append).
        encoding: File encoding.
        transform: An optional function to convert each yielded item into a string
                   before writing. If None, `str(item)` is used.
    """
    output_path = Path(output_path)

    # Ensure parent directories exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode, encoding=encoding) as f:
        for item in results_generator:
            if transform is not None:
                line = transform(item)  # type: ignore
            else:
                line = str(item)

            if not line.endswith("\n"):
                line += "\n"

            f.write(line)
