"""
File locking functions for controlling concurrency among processes.

Usage:
    from FileLock import acquire, release

    def someCriticalSection () :
        lock = acquire("mylockfile.txt", timeout=300)
        if lock is None:
            raise RuntimeException(f"Could not get lock. Timed out after {timeout} seconds.")
        #
        # critical section here
        #

        release(lock)

This code was obtained from the following location and has been minimally changed for our use.

Source: https://gist.github.com/jirihnidek/430d45c54311661b47fb45a3a7846537
"""

import os
import sys
import fcntl
import time

# Acquire an exclusive lock on the named lock_file.
# Returns lock file descriptor, or None if the lock could not be acquired within timeout seconds.
def acquire(lock_file, timeout=60):
    open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
    fd = os.open(lock_file, open_mode)

    pid = os.getpid()
    lock_file_fd = None
    
    start_time = current_time = time.time()
    while current_time < start_time + timeout:
        try:
            # The LOCK_EX means that only one process can hold the lock
            # The LOCK_NB means that the fcntl.flock() is not blocking
            # and we are able to implement termination of while loop,
            # when timeout is reached.
            # More information here:
            # https://docs.python.org/3/library/fcntl.html#fcntl.flock
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            pass
        else:
            lock_file_fd = fd
            break
        time.sleep(1.0)
        current_time = time.time()
    #
    if lock_file_fd is None:
        os.close(fd)
    return lock_file_fd


# Frees the lock. Argument is the descriptor returned by acquire().
# Returns nothing.
def release(lock_file_fd):
    # Do not remove the lockfile:
    #
    #   https://github.com/benediktschmitt/py-filelock/issues/31
    #   https://stackoverflow.com/questions/17708885/flock-removing-locked-file-without-race-condition
    fcntl.flock(lock_file_fd, fcntl.LOCK_UN)
    os.close(lock_file_fd)
    return None



# You can run it using:
#    python ./flock_example.py &
#    python ./flock_example.py &
#    ...
if __name__ == '__main__':
    pid = os.getpid()
    print(f'{pid} is waiting for lock')
    
    fd = acquire('/tmp/myfile.lock', timeout=10)

    if fd is None:
        print(f'ERROR: {pid} lock NOT acquired')
        sys.exit(-1)

    print(f"{pid} lock acquired...")
    time.sleep(2.0)
    release(fd)
    print(f"{pid} lock released")    

