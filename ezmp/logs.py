from typing import Iterator, List, Callable, Any, Generator

def read_chunks(file_path: str, chunk_size: int = 1000) -> Iterator[List[str]]:
    """
    Reads a massive text or log file in chunks.
    Yields a list of strings (lines) up to `chunk_size` length.
    This acts as a Generator to ensure memory usage stays extremely low.
    
    Args:
        file_path: Path to the log file.
        chunk_size: Number of lines to yield at once.
        
    Yields:
        A list of strings representing the lines in the chunk.
    """
    chunk = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
                
        # Yield any remaining lines
        if chunk:
            yield chunk

def parse_blocks(
    file_path: str, 
    block_extractor: Callable[[List[str], Any], Generator[Any, None, Any]], 
    chunk_size: int = 5000,
    initial_state: Any = None
) -> Iterator[Any]:
    """
    Reads a massive log file in chunks and passes each chunk of lines to a custom
    `block_extractor` function. The extractor function can yield parsed items (like 
    dictionaries or objects) representing multi-line blocks (e.g., Stack traces, Error blocks).
    
    This allows parsing multi-line blocks that span across raw lines natively, without 
    loading the entire file into memory. A state object can be passed and modified 
    across chunks to handle blocks that get split between two chunks.
    
    Args:
        file_path: Path to the log file.
        block_extractor: A generator function that takes `(chunk_lines: List[str], state: Any)` 
                         and `yields` parsed blocks. `state` can be modified in-place 
                         to carry information between chunks.
        chunk_size: Number of lines to pass to the extractor at once. Default 5000.
        initial_state: An optional starting state object (e.g., a dictionary mapping 
                       current block variables) to pass to the extractor.
                       
    Yields:
        Items yielded by the user's `block_extractor` function.
    """
    state = initial_state
    for chunk in read_chunks(file_path, chunk_size):
        # The user's extractor function should yield the processed blocks
        # and optionally modify the `state` object for the next chunk if a block is cut off.
        yield from block_extractor(chunk, state)
