package water.rapids.ast.prims.advmath;

import water.MRTask;
import water.fvec.*;
import water.rapids.Env;
import water.rapids.Val;
import water.rapids.ast.AstPrimitive;
import water.rapids.ast.AstRoot;
import water.rapids.vals.ValFrame;
import water.util.Log;

/**
 * Calculate Distance Metric between pairs of rows
 */
public class AstDistance extends AstPrimitive {
  @Override
  public String[] args() {
    return new String[]{"ary", "x", "y", "metric"};
  }

  @Override
  public int nargs() {
    return 1 + 3; /* (distance X Y metric) */
  }

  @Override
  public String str() {
    return "distance";
  }

  @Override
  public Val apply(Env env, Env.StackHelp stk, AstRoot asts[]) {
    Frame frx = stk.track(asts[1].exec(env)).getFrame();
    Frame fry = stk.track(asts[2].exec(env)).getFrame();
    String metric = stk.track(asts[3].exec(env)).getStr();
    return computeCosineDistances(frx, fry, metric);
  }

  public Val computeCosineDistances(Frame references, Frame queries, String distanceMetric) {
    Log.info("Number of references: " + references.numRows());
    Log.info("Number of queries   : " + queries.numRows());
    if (references.numCols() != queries.numCols())
      throw new IllegalArgumentException("Frames must have the same number of cols, found " + references.numCols() + " and " + queries.numCols());
    if (queries.numRows() > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Queries can't be larger than 2 billion rows.");
    if (queries.numCols() != references.numCols())
      throw new IllegalArgumentException("Queries and References must have the same dimensionality");
    if (!distanceMetric.toLowerCase().equals("cosine") && !distanceMetric.toLowerCase().equals("euclidean"))
      throw new IllegalArgumentException("Only support 'cosine' or 'euclidean' metrics.");
    for (int i=0;i<queries.numCols();++i) {
      if (!references.vec(i).isNumeric())
        throw new IllegalArgumentException("References column " + references.name(i) + " is not numeric.");
      if (!queries.vec(i).isNumeric())
        throw new IllegalArgumentException("Queries column " + references.name(i) + " is not numeric.");
      if (references.vec(i).naCnt()>0)
        throw new IllegalArgumentException("References column " + references.name(i) + " contains missing values.");
      if (queries.vec(i).naCnt()>0)
        throw new IllegalArgumentException("Queries column " + references.name(i) + " contains missing values.");
    }
    return new ValFrame(new DistanceComputer(queries, distanceMetric).doAll((int)queries.numRows(), Vec.T_NUM, references).outputFrame());
  }

  static public class DistanceComputer extends MRTask<DistanceComputer> {
    Frame _queries;
    String _metric;

    DistanceComputer(Frame queries, String metric) {
      _queries = queries;
      _metric = metric;
    }

    @Override
    public void map(Chunk[] cs, NewChunk[] ncs) {
      int p = cs.length; //dimensionality
      int Q = (int) _queries.numRows();
      int R = cs[0]._len;
      Vec.Reader[] Qs = new Vec.Reader[p];
      for (int i = 0; i < p; ++i) {
        Qs[i] = _queries.vec(i).new Reader();
      }
      double[] denomR = null;
      double[] denomQ = null;

      final boolean cosine = _metric.toLowerCase().equals("cosine");
      final boolean l2 = _metric.toLowerCase().equals("euclidean");

      if (cosine) {
        denomR = new double[R];
        denomQ = new double[Q];
        for (int r = 0; r < R; ++r) { // Reference row (chunk-local)
          for (int c = 0; c < p; ++c) { //cols
            denomR[r] += Math.pow(cs[c].atd(r), 2);
          }
        }
        for (int q = 0; q < Q; ++q) { // Query row (global)
          for (int c = 0; c < p; ++c) { //cols
            denomQ[q] += Math.pow(Qs[c].at(q), 2);
          }
        }
      }

      for (int r = 0; r < cs[0]._len; ++r) { // Reference row (chunk-local)
        for (int q = 0; q < Q; ++q) { // Query row (global)
          double distRQ = 0;
          if (l2) {
            for (int c = 0; c < p; ++c) { //cols
              distRQ += Math.pow(cs[c].atd(r) - Qs[c].at(q), 2);
            }
          } else if (cosine) {
            for (int c = 0; c < p; ++c) { //cols
              distRQ += cs[c].atd(r) * Qs[c].at(q);
            }
            distRQ /= denomR[r] * denomQ[q];
          }
          ncs[q].addNum(distRQ); // one Q distance per Reference
        }
      }
    }
  }
}