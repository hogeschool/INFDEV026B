using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace TransactionSimulator
{
  //enumeration for the lock type
  public enum LockType { SHARED, EXCLUSIVE, UNLOCKED }

  //used to compose the message to write on the console and in the log file
  internal static class Logger
  {
    internal static string Log = "";
  }

  internal class Lock
  {
    internal string ObjectName { get; private set; } //name of the object associated to the lock
    internal List<Transaction> OwnerIds { get; private set; } //list of transactions owning the lock
    internal Queue<Tuple<Transaction, LockType>> TransactionQueue { get; set; } //list of transactions waiting to acquire the lock and the requested lock types
    internal LockType Kind { get; set; } //kind of lock

    public Lock(Transaction transaction, string objectName, LockType kind)
    {
      ObjectName = objectName;
      OwnerIds = new List<Transaction>();
      OwnerIds.Add(transaction);
      TransactionQueue = new Queue<Tuple<Transaction, LockType>>();
      Kind = kind;
    }

    //add a transaction to queue and pause it
    private void AddToQueue(Transaction transaction, LockType lockType)
    {
      TransactionQueue.Enqueue(new Tuple<Transaction, LockType>(transaction, lockType));
      transaction.Paused = true;
    }

    //When a transaction gets a shared lock, it is added among the owners and the lock type is set to SHARED
    private void GetShared(Transaction transaction)
    {
      string tname = "T" + transaction.Id;
      string message = tname + " <-- S(" + ObjectName + ")\n";
      Logger.Log += message;
      Console.Write(message);
      Kind = LockType.SHARED;
      OwnerIds.Add(transaction);
    }

    //When a transaction gets an exclusive lock, it is added among the onwers if it is not already there and the lock typ is set to EXCLUSIVE.
    //A transaction might already be in the owner list when requesting an EXCLUSIVE lock when it is upgrading its SHARED lock, i.e.
    //it is the only transaction owning the lock and that lock is SHARED.
    private void GetExclusive(Transaction transaction)
    {
      string tname = "T" + transaction.Id;
      string message = tname + " <-- X(" + ObjectName + ")\n";
      Logger.Log += message;
      Console.Write(message);
      Kind = LockType.EXCLUSIVE;
      if (!OwnerIds.Contains(transaction))
        OwnerIds.Add(transaction);
    }

    internal void Acquire(Transaction transaction, LockType requestedType)
    {
      string tname = "T" + transaction.Id;
      string message = "";
      if (requestedType == Kind && OwnerIds.Contains(transaction))
        return;
      switch (Kind)
      {
        //When a lock is UNLOCKED the transaction always gets the requested lock
        case LockType.UNLOCKED:
          switch (requestedType)
          {
            case LockType.SHARED:
              GetShared(transaction);
              break;
            case LockType.EXCLUSIVE:
              GetExclusive(transaction);
              break;
            default:
              break;
          }
          break;
        case LockType.SHARED:
          switch (requestedType)
          {
            //A SHARED lock can always be acquired, if the current state of the lock is SHARED
            case LockType.SHARED:
              GetShared(transaction);
              break;

            case LockType.EXCLUSIVE:
              //There is more than one transaction using the SHARED lock. In this case the EXCLUSIVE lock is denied and the
              //requesting transaction is placed in queue.
              if (OwnerIds.Exists(t => t.Id != transaction.Id))
              {
                message = tname + " <-- " + "Wait(" + ObjectName + ")\n";
                Logger.Log += message;
                Console.Write(message);
                AddToQueue(transaction, requestedType);
              }
              //If there exists no other transaction owning the SHARED lock, then the transaction can upgrade its SHARED lock
              //to an EXCLUSIVE lock.
              else
              {
                GetExclusive(transaction);
              }
              break;
          }
          break;
        case LockType.EXCLUSIVE:
          //if the lock is EXCLUSIVE, then any request to acquire it is denied an the requesting transaction is placed in queue
          switch (requestedType)
          {
            case LockType.SHARED:
              message = tname + " <-- " + "Wait(" + ObjectName + ")\n";
              Logger.Log += message;
              Console.Write(message);
              AddToQueue(transaction, requestedType);
              break;
            case LockType.EXCLUSIVE:
              message = tname + " <-- " + "Wait(" + ObjectName + ")\n";
              Logger.Log += message;
              Console.Write(message);
              AddToQueue(transaction, requestedType);
              break;
          }
          break;
      }
    }
    
    //When the lock is released, the first transaction in queue will try to acquire the lock again
    internal void RefreshQueue()
    {
      if (TransactionQueue.Count == 0)
        return;
      Tuple<Transaction, LockType> first = TransactionQueue.Dequeue();
      first.Item1.Paused = false;
      Acquire(first.Item1, first.Item2);
    }

    //When releasing the lock, the current transaction is removed from the owners. This function reports
    //whether the queue is empty or not.
    internal bool Release(Transaction transaction)
    {
      OwnerIds.Remove(transaction);
      return OwnerIds.Count == 0;
    }     
  }

  public class Scheduler
  {
    private List<Transaction> transactions;
    private List<Lock> locks;
    private double timeScale;

    private class TransactionExecution : IOperationVisitor
    {
      Scheduler scheduler;
      Transaction transaction;

      public TransactionExecution(Scheduler scheduler, Transaction transaction)
      {
        this.scheduler = scheduler;
        this.transaction = transaction;
      }

      public void OnCommit(long time)
      {
        //Find all the locks owned by the committing transaction
        List<Lock> transactionLocks = scheduler.locks.FindAll(l => l.OwnerIds.Contains(transaction));
        //build up a list of locks that can be released
        for (int i = 0; i < transactionLocks.Count; i++)
        {
          bool released = transactionLocks[i].Release(transaction);
          //unlock the lock and add it to the locks to be released
          if (released)
          {
            transactionLocks[i].Kind = LockType.UNLOCKED;
            string message = "Unlock(" + transactionLocks[i].ObjectName + ")\n";
            Logger.Log += message;
            Console.Write(message);
          }
        }
        //Refresh all the queues of the locks owned by the transaction
        for (int i = 0; i < transactionLocks.Count; i++)
        {
          transactionLocks[i].RefreshQueue();
        }

        //Mark the transaction as committed
        transaction.Committed = true;
      }

      //Read and write attempt to acquire respectively a SHARED and EXCLUSIVE lock.
      public void OnRead(string obj, long time)
      {
        scheduler.AcquireLock(transaction, obj, LockType.SHARED);
      }

      public void OnWrite(string obj, long time)
      {
        scheduler.AcquireLock(transaction, obj, LockType.EXCLUSIVE);
      }
    }

    public Scheduler(IEnumerable<Transaction> transactions, double timeScale = 1)
    {
      locks = new List<Lock>(); //list of managed locks
      this.transactions = transactions.ToList(); //list of transactions in the execution
      this.timeScale = timeScale;
    }

    internal void AcquireLock(Transaction transaction, string objectName, LockType kind)
    {
      //If the lock does not exist yet, we create it and add it to the list of locks
      if(!locks.Exists(l => l.ObjectName == objectName))
      {
        string message = "T" + transaction.Id + " <-- " + (kind == LockType.SHARED ? "S" : "X") + "(" + objectName + ")\n";
        Logger.Log += message;
        Console.Write(message);
        locks.Add(new Lock(transaction, objectName, kind));
      }
      //otherwise we attempt to acquire it
      else
      {
        Lock l = locks.Find(l1 => l1.ObjectName == objectName);
        l.Acquire(transaction, kind);
      }
    }

    public void Run()
    {
      Stopwatch watch = new Stopwatch(); //Initialize the timer
      watch.Start(); //Start the timer
      while (transactions.Count > 0) //The scheduler stops when there are no more transactions
      {
        string header = "";
        for (int i = 0; i < transactions.Count; i++)
        {
          long currentTime = watch.ElapsedMilliseconds; //Take the elapsed time since the beginning of the scheduling

          //A new operation can be executed if the timer of the scheduler is past the operation time and the transaction
          //is not paused, i.e. waiting to acquire a lock.
          if (currentTime >= transactions[i].Execution[transactions[i].ProgramCounter].Time * timeScale && !transactions[i].Paused)
          {
            const string line = "=============";
            string message = header + line + "TIME: " + watch.ElapsedMilliseconds + line + "\n\n";
            Console.Write(message);
            Logger.Log += message;
            Operation op = transactions[i].Execution[transactions[i].ProgramCounter];
            op.Visit(new TransactionExecution(this, transactions[i]));

            //The program counter of the transaction is increased only if the transaction has not been stopped.
            //This check is performed because executing an operation can result into the transaction having to
            //wait for a lock and being stopped
            if (!transactions[i].Paused)
            {
              message = "T" + transactions[i].Id + " <-- " + op.ToString() + "\n";
              Console.Write(message);
              Logger.Log += message;
              transactions[i].ProgramCounter++;
            }
          }
          header = "\n\n";
        }
        //Cleanup the list of transactions by removing those that completed all the instructions
        transactions.RemoveAll(t => t.Committed);
      }
      File.WriteAllText("log.txt", Logger.Log);
    }
  }
}
