using System;
using System.Collections.Generic;
using AirportDB;
using MapReduce;

namespace MapReduceExercises
{
  class Program
  {
    static void Main(string[] args)
    {
      AirportDatabase db = AirportDatabase.Test();
      var q1 = db.Query1();
      var q2 = db.Query2();
      var q3 = db.Query3();
      var q4 = db.Query4();
      var q5 = db.Query5();
      var q6 = db.Query6();
    }
  }
}
