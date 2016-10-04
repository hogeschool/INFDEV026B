using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;

namespace Data
{
  public abstract class Value
  {
    protected ValueVisitor visitor;

    public abstract bool IsEmpty();
  }

  public class ValueVisitor
  {
    public bool IsEmpty(DBReal x) { return false; }
    public bool IsEmpty(DBInteger x) { return false; }
    public bool IsEmpty(DBString x) { return false; }
    public bool IsEmpty(DBNull x) { return false; }
    public bool IsEmpty(EmptyResult x) { return true; }
  }

  public class DBReal : Value
  {
    public double Value { get; set; }

    public override string ToString()
    {
      return Value.ToString();
    }

    public static DBReal operator +(DBReal x, DBReal y) { return new DBReal(x.Value + y.Value); }

    public DBReal(double d) { Value = d; visitor = new ValueVisitor(); }

    public override bool IsEmpty()
    {
      return visitor.IsEmpty(this);
    }
  }

  public class DBInteger : Value
  {
    public int Value { get; set; }

    public override string ToString()
    {
      return Value.ToString();
    }

    public DBInteger(int i) { Value = i; visitor = new ValueVisitor(); }

    public override bool IsEmpty()
    {
      return visitor.IsEmpty(this);
    }

    public static DBInteger operator+(DBInteger x, DBInteger y) { return new DBInteger(x.Value + y.Value); }
  }

  public class DBString : Value
  {
    public string Value { get; set; }

    public override string ToString()
    {
      return Value.ToString();
    }

    public DBString(string s) { Value = s; visitor = new ValueVisitor(); }

    public override bool IsEmpty()
    {
      return visitor.IsEmpty(this);
    }

    public static DBString operator+(DBString x, DBString y) { return new DBString(x.Value + y.Value); }
  }

  public class DBNull : Value
  {
    public override string ToString()
    {
      return "NULL";
    }

    public override bool IsEmpty()
    {
      return visitor.IsEmpty(this);
    }

    public DBNull() { visitor = new ValueVisitor(); }
  }

  public class EmptyResult: Value
  {
    public override string ToString()
    {
      return "";
    }

    public override bool IsEmpty()
    {
      return visitor.IsEmpty(this);
    }

    public EmptyResult() { visitor = new ValueVisitor(); }
  }

  public class Table
  {
    public List<Value[]> Rows { get; set; }


    public override string ToString()
    {
      string s = "";
      for (int i = 0; i < Rows.Count; i++)
      {
        for (int j = 0; j < Rows[i].Length; j++)
        {
          if (!Rows[i].Any(x => x.IsEmpty()))
            s += Rows[i][j].ToString() + (j == Rows[i].Length - 1 ? "\n" : "  ");
        }
      }
      return s;
    }

    public void AddRow(Value[] row)
    {
      if (Rows.Count > 0 && Rows[0].Length > row.Length)
        throw new ArgumentException("Rows have different number of arguments");
      Rows.Add(row);
    }

    public Table(List<Value[]> rows)
    {
      int elements = rows[0].Length;
      for (int i = 0; i < rows.Count; i++)
      {
        if (elements != rows[i].Length)
          throw new ArgumentException("Rows have different number of arguments");
        else
          elements = rows[i].Length;
      }
      Rows = rows;
    }

    public Table(string fileName)
    {
      string[] lines = File.ReadAllLines(fileName);
      List<Value[]> table = new List<Value[]>(lines.Length);
      for (int i = 0; i < lines.Length; i++)
      {
        string[] columns = lines[i].Split(',');
        Value[] values = new Value[columns.Length];
        for (int j = 0; j < columns.Length; j++)
        {
          string cell = columns[j].Where(x => x != ' ' && x != '\t').Aggregate("", (x, y) => x + y);
          int intValue;
          double realValue;

          if (Int32.TryParse(cell, out intValue))
            values[j] = new DBInteger(intValue);
          else if (Double.TryParse(cell, out realValue))
            values[j] = new DBReal(realValue);
          else if (cell == "NULL")
            values[j] = new DBNull();
          else
            values[j] = new DBString(cell);
        }
        table.Add(values);
      }
      Rows = table;
    }

    public Table Map(Func<Value[],Value[]> mapFunction)
    {
      List<Value[]> rows = new List<Value[]>();
      for (int i = 0; i < Rows.Count; i++)
      {
        rows.Add(mapFunction(Rows[i]));
      }
      rows = rows.Where(r => !r.Any(v => v.IsEmpty())).ToList();
      return new Table(rows);
    }

    public Value[] Reduce(Func<Value[], Value[], Value[]> reduction, Value[] reducedValue)
    {
      for (int i = 0; i < Rows.Count; i++)
      {
        reducedValue = reduction(reducedValue, Rows[i]);
      }
      return reducedValue;
    }
  }
}
