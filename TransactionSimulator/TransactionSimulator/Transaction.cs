using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionSimulator
{
  //this interface defines a polymorphic data type to represent operations performed by
  //concurrent transactions
  public interface Operation
  {
    long Time { get; }
    void Visit(IOperationVisitor visitor);
  }

  public class Write : Operation
  {
    public string ObjectName { get; private set; }
    public long Time { get; private set; }

    public Write(string objectName, long time)
    {
      Time = time;
      ObjectName = objectName;
    }

    public void Visit(IOperationVisitor visitor)
    {
      visitor.OnWrite(ObjectName, Time);
    }


    public override string ToString()
    {
      return "W(" + ObjectName + ")";
    }
  }

  public class Read : Operation
  {
    public long Time { get; private set; }
    public string ObjectName { get; private set; }

    public Read(string objectName, long time)
    {
      Time = time;
      ObjectName = objectName;
    }

    public void Visit(IOperationVisitor visitor)
    {
      visitor.OnRead(ObjectName, Time);
    }

    public override string ToString()
    {
      return "R(" + ObjectName + ")";
    }
  }

  public class Commit : Operation
  {
    public long Time { get; private set; }
    public Commit(long time)
    {
      Time = time;
    }

    public void Visit(IOperationVisitor visitor)
    {
      visitor.OnCommit(Time);
    }

    public override string ToString()
    {
      return "Commit";
    }
  }

  //Visitor pattern for Operation
  public interface IOperationVisitor
  {
    void OnWrite(string obj, long time);
    void OnRead(string obj, long time);
    void OnCommit(long time);
  }

  public class Transaction
  {
    //the Id is static because we must keep the value for all the class instances
    private static int generatedId = 0;
    public int Id { get; private set; }
    public List<Operation> Execution { get; private set; }
    internal int ProgramCounter { get; set; }
    internal bool Paused { get; set; } //used to pause a transaction while it is waiting for a lock
    internal bool Committed { get; set; } //used to mark a transaction as committed

    private void Init()
    {
      Execution = new List<Operation>();
      Id = generatedId++;
      ProgramCounter = 0;
      Committed = false;
    }

    public Transaction()
    {
      Init();
    }

    public Transaction(IEnumerable<Operation> operations)
    {
      Init();
      foreach(Operation op in operations)
      {
        Execution.Add(op);
      }
      Execution.OrderBy(op => op.Time);
    }
  }
}
