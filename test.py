import unittest
from process import ProcessPool
import time

def work(i):
    for _ in range(5):
        print("------work:", i)
        import time
        time.sleep(1)
    return i

class TestAll(unittest.TestCase):

    def test_all(self):
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

if __name__ == '__main__':
    unittest.main()