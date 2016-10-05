using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Data;
using MapReduceList;

namespace Main
{
  class Program
  {
    public static void TestDB()
    {
			
      Func<Value[], Value[]> testMap =
        row =>
        {
          Value[] mapping = new Value[2];
          DBReal salary = (DBReal)row[3];
          DBString nationality = (DBString)row[5];
          if (nationality.Value == "USA")
          {
            mapping[0] = salary;
            mapping[1] = nationality;
          }
          else
          {
            mapping[0] = new EmptyResult();
            mapping[1] = new EmptyResult();
          }
          return mapping;
        };
      Func<Value[], Value[], Value[]> testReduce =
        (state, row) =>
        {
          DBReal salary = (DBReal)row[0];
          state[0] = (DBReal)state[0] + salary;
          state[1] = ((DBInteger)state[1]) + new DBInteger(1);
          return state;
        };
      Table testTable = new Table("table.csv");
      Table mapTest = testTable.Map(testMap);
      Value[] reduceTest = mapTest.Reduce(testReduce, new Value[] { new DBReal(0), new DBInteger(0) });
      List<Value[]> resultRows = new List<Value[]>();
      resultRows.Add(reduceTest);
      const string lineSep = "\n\n-----------\n\n";
      Console.WriteLine(testTable + lineSep + mapTest + lineSep + (new Table(resultRows)));
    }

    public static void TestList()
    {
      List<int> l = new List<int>(new int[] { 1, 5, 6, 10, -2 });
      Func<int, int> testMap = x => x * x;
      Func<int, int, int> testReduce = (x, y) => x + y;
      List<int> m = HigherOrderFunctions.Map(l, testMap);
      int r = HigherOrderFunctions.Reduce(l, testReduce);
    }
    public static void Main(string[] args)
    {
      TestList();
    }
  }
}
