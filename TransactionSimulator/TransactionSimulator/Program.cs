using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionSimulator
{
  class Program
  {

    static void Test4()
    {
      Operation[] op1 = new Operation[]
      {
        new Read("A", 1000),
        new Write("A", 2000),
        new Write("A", 8000),
        new Commit(9000)
      };

      Operation[] op2 = new Operation[]
      {
        new Read("B", 3000),
        new Read("A", 4000),
        new Read("C", 10000),
        new Commit(13000)
      };

      Operation[] op3 = new Operation[]
      {
        new Write("C", 5000),
        new Commit(11000)
      };

      Operation[] op4 = new Operation[]
      {
        new Read("A", 6000),
        new Write("C", 7000),
        new Commit(12000)
      };

      Transaction t1 = new Transaction(op1);
      Transaction t2 = new Transaction(op2);
      Transaction t3 = new Transaction(op3);
      Transaction t4 = new Transaction(op4);

      Scheduler scheduler = new Scheduler(new Transaction[] { t1, t2, t3, t4 });
      scheduler.Run();
    }

    static void Test3()
    {
      Operation[] op1 = new Operation[]
      {
        new Read("A", 1000),
        new Write("A", 6000),
        new Read("B", 7000),
        new Commit(10000)
      };

      Operation[] op2 = new Operation[]
      {
        new Read("C", 2000),
        new Commit(5000)
      };

      Operation[] op3 = new Operation[]
      {
        new Write("C", 3000),
        new Read("B", 8000),
        new Write("B", 9000),
        new Commit(11000)
      };

      Transaction t1 = new Transaction(op1);
      Transaction t2 = new Transaction(op2);
      Transaction t3 = new Transaction(op3);

      Scheduler scheduler = new Scheduler(new Transaction[] { t1, t2, t3 });
      scheduler.Run();
    }

    static void Test2()
    {
      Operation[] op1 = new Operation[]
      {
        new Read("A", 1000),
        new Write("B", 5000),
        new Write("A", 7000),
        new Commit(8000)
      };

      Operation[] op2 = new Operation[]
      {
        new Write("A", 2000),
        new Commit(9000)
      };

      Operation[] op3 = new Operation[]
      {
        new Read("A", 3000),
        new Read("B", 4000),
        new Commit(6000)
      };

      Transaction t1 = new Transaction(op1);
      Transaction t2 = new Transaction(op2);
      Transaction t3 = new Transaction(op3);
      Scheduler scheduler = new Scheduler(new Transaction[] { t1, t2, t3 });
      scheduler.Run();
    }

    static void Test1()
    {
      Operation[] op1 = new Operation[]
      {
        new Read("A", 2000),
        new Read("B", 3000),
        new Commit(6000)
      };

      Operation[] op2 = new Operation[]
      {
        new Read("A", 1000),
        new Read("B", 4000),
        new Write("A", 5000),
        new Commit(7000)
      };

      Transaction t1 = new Transaction(op1);
      Transaction t2 = new Transaction(op2);
      Scheduler scheduler = new Scheduler(new Transaction[] { t1, t2 });
      scheduler.Run();
    }

    static void Main(string[] args)
    {
      Test2();
    }
  }
}
