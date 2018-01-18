using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MapReduceList
{
  public static class HigherOrderFunctions
  {
    public static List<U> Map<T, U>(List<T> inputList, Func<T, U> mapper)
    {
      List<U> outputList = new List<U>(inputList.Count);
      for (int i = 0; i < inputList.Count; i++)
      {
        outputList.Add(mapper(inputList[i]));
      }
      return outputList;
    }

    public static State Reduce<T, State>(List<T> inputList, Func<State, T, State> accumulate)
    {
      State res = default(State);
      for (int  i = 0; i < inputList.Count; i++)
      {
        res = accumulate(res, inputList[i]);
      }
      return res;
    }
  }
}
