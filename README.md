# mp_manager
multiprocess pool manager:  suspend,  resume,  cancel


# Usage
```python
import process import ProcessPool
p = ProcessPool(max_workers=3)
for i in range(5):
    f = p.submit(work, i)
    if i == 1:
        time.sleep(1)
        f.cancel()
    if i == 2:
        time.sleep(1)
        f.suspend()
        time.sleep(3)
        f.resume()
```