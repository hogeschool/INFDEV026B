using System;
using System.Collections.Generic;
using System.Linq;

namespace MapReduce
{
  public static class MapReduceExtension
  {
    public static IEnumerable<U> Map<T, U>(this IEnumerable<T> table, Func<T, U> mapper)
    {
      U[] result = new U[table.Count()];
      for (int i = 0; i < table.Count(); i++)
      {
        result[i] = mapper(table.ElementAt(i));
      }
      return result;
    }

    public static State Reduce<T, State>(this IEnumerable<T> table, State init, Func<State, T, State> operation)
    {
      State accumulator = init;
      for (int i = 0; i < table.Count(); i++)
      {
        accumulator = operation(accumulator, table.ElementAt(i));
      }
      return accumulator;
    }

    public static IEnumerable<Tuple<T1, T2>> Join<T1, T2>(this IEnumerable<T1> table1, IEnumerable<T2> table2, Func<Tuple<T1, T2>, bool> filter)
    {
      return
        table1.Reduce(new List<Tuple<T1, T2>>(),
          (l, row1) =>
              {
                List<Tuple<T1, T2>> combinations =
                  table2.Reduce(new List<Tuple<T1, T2>>(),
                    (l1, row2) =>
                      {
                        Tuple<T1, T2> combination = new Tuple<T1, T2>(row1, row2);
                        if (filter(combination))
                          l1.Add(combination);
                        return l1;
                      });
                l.AddRange(combinations);
                return l;
              });
    }
  }
}
