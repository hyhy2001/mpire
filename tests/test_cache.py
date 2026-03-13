import time
import multiprocessing
import ezmp


# -- Auto-Memoization Suite --
def slow_square(x):
    # Increased to 0.2s to cleanly override the massive 1s Windows overhead
    time.sleep(0.2)
    return x * x


def test_auto_memoize_efficiency():
    # 20 identical inputs
    inputs = [5] * 20

    start = time.time()
    # 2 workers processing 20 tasks of 0.2s would normally take 2.0s overall without caching
    results = ezmp.run(
        slow_square, inputs, use_threads=False, max_workers=2, memoize=True
    )
    duration = time.time() - start

    # Process startup on Windows takes ~1s.
    # If caching fails, duration >> 2.0s. If caching works, duration is ~1.2s.
    assert duration < 2.0, f"Memoization failed, duration took {duration}s"
    assert all(r == 25 for r in results)


# -- Shared State Injection Suite --
def state_aggregator(item, shared_state=None):
    if shared_state is not None:
        # Multiprocess dictionary injection!
        # Assigning unique keys entirely avoids the += race condition.
        shared_state[item] = "PROCESSED"
    return item * 2


def test_shared_state_multiprocess():
    manager = multiprocessing.Manager()
    global_tracker = manager.dict()

    inputs = ["apple", "banana", "cherry", "dragonfruit", "elderberry"]
    results = (
        ezmp.run_ordered(  # run_ordered guarantees predictable list assertion matching
            state_aggregator,
            inputs,
            use_threads=False,
            max_workers=3,
            shared_state=global_tracker,
        )
    )

    # Verify core results
    expected = [item * 2 for item in inputs]
    assert results == expected

    # Verify multiprocessing state was mutated globally across isolated CPU cores!
    for item in inputs:
        assert global_tracker.get(item) == "PROCESSED"
