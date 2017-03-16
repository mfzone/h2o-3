from builtins import range
import sys
sys.path.insert(1,"../../../")
import h2o
from tests import pyunit_utils


import numpy as np

def distance_test():

    iris_h2o = h2o.import_file(path=pyunit_utils.locate("smalldata/iris/iris.csv"))
    iris_np = np.genfromtxt(pyunit_utils.locate("smalldata/iris/iris.csv"),
                            delimiter=',',
                            skip_header=1,
                            usecols=(0, 1, 2, 3))

    references = iris_h2o[10:150,0:4]
    queries    = iris_h2o[0:10,0:4]
    A = references.distance(queries, "euclidean")
    print(A)

    B = references.distance(queries, "cosine")
    print(B)

    a = queries.distance(references, "euclidean")
    print(a)

    b = queries.distance(references, "cosine")
    print(b)

    assert(all(A.transpose() == a))
    assert(all(B.transpose() == b))

if __name__ == "__main__":
    pyunit_utils.standalone_test(distance_test)
else:
    distance_test()
