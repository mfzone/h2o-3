setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../../scripts/h2o-r-test-setup.R")

test.dist <- function() {
  iris_h2o <- as.h2o(iris)

  references = iris_h2o[11:150,1:4]
  queries    = iris_h2o[1:10,  1:4]

  A = h2o.distance(references, queries, "euclidean")
  print(A)

  B = h2o.distance(references, queries, "cosine")
  print(B)

  a = h2o.distance(queries, references, "euclidean")
  print(a)

  b = h2o.distance(queries, references, "cosine")
  print(b) 

  expect_true(all(t(A) == a))
  expect_true(all(t(B) == b))
}

doTest("Test out the distance() functionality", test.dist)
