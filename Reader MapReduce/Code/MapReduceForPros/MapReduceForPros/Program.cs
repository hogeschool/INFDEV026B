using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MapReduceForPros
{
  class EmployeeTuple
  {
    //id, name, surname, sex, salary
    public Tuple<int, string, string, char, double> Tuple { get; set; }

    public EmployeeTuple(int id, string name, string surname, char sex, double salary)
    {
      Tuple = new Tuple<int, string, string, char, double>(id, name, surname, sex, salary);
    }
  }

  class Employee
  {
    public int Id { get; set; }
    public string Name { get; set; }
    public string Surname { get; set; }
    public char Sex { get; set; }
    public double Salary { get; set; }

    public Employee(int id, string name, string surname, char sex, double salary)
    {
      Id = id;
      Name = name;
      Surname = surname;
      Sex = sex;
      Salary = salary;
    }
  }

  class CompanyCar
  {
    public string Plate { get; set; }
    public string Model { get; set; }
    public int EmployeeId { get; set; }
    public CompanyCar(string plate, string model, int employee)
    {
      Plate = plate;
      Model = model;
      EmployeeId = employee;
    }
  }

  static class MapReduce
  {
    public static IEnumerable<T2> Map<T1, T2>(IEnumerable<T1> collection, Func<T1, T2> transformation)
    {
      T2[] result = new T2[collection.Count()];
      for (int i = 0; i < collection.Count(); i++)
      {
        result[i] = transformation(collection.ElementAt(i));
      }
      return result;
    }

    public static T2 Reduce<T1, T2>(IEnumerable<T1> collection, T2 init, Func<T2, T1, T2> operation)
    {
      T2 result = init;
      for (int i = 0; i < collection.Count(); i++)
      {
        result = operation(result, collection.ElementAt(i));
      }
      return result;
    }

    public static IEnumerable<T> Where<T>(IEnumerable<T> collection, Func<T, bool> condition)
    {
      List<T> result = new List<T>();
      for (int i = 0; i < collection.Count(); i++)
      {
        if (condition(collection.ElementAt(i)))
          result.Add(collection.ElementAt(i));
      }
      return result;
    }

    public static IEnumerable<Tuple<T1, T2>> Join<T1, T2>(IEnumerable<T1> table1, IEnumerable<T2> table2, Func<Tuple<T1, T2>, bool> condition)
    {
      return
        Reduce(table1, new List<Tuple<T1, T2>>(),
          (queryResult, x) =>
          {
            List<Tuple<T1, T2>> combination =
              Reduce(table2, new List<Tuple<T1, T2>>(),
                      (c, y) =>
                      {
                        Tuple<T1, T2> row = new Tuple<T1, T2>(x, y);
                        if (condition(row))
                          c.Add(row);
                        return c;
                      });
            queryResult.AddRange(combination);
            return queryResult;
          });
    }
  }

  class Program
  {
    static void Main(string[] args)
    {
      int[] numbers = { 3, -1, 4, -20, 6 };
      List<EmployeeTuple> employeeTupleTable =
        new List<EmployeeTuple>(new EmployeeTuple[]
        {
          new EmployeeTuple(3952, "Frank", "Moses", 'M', 2500.50),
          new EmployeeTuple(1403, "John", "Ford", 'M', 1200.50),
          new EmployeeTuple(3433, "Michelle", "Brown", 'F', 3250.25),
          new EmployeeTuple(3540, "Daniel", "Smith", 'M', 2500.50)
        });
      List<Employee> employeeTable =
        new List<Employee>(new Employee[]
        {
          new Employee(3952, "Frank", "Moses", 'M', 2500.50),
          new Employee(1403, "John", "Ford", 'M', 1200.50),
          new Employee(3433, "Michelle", "Brown", 'F', 3250.25),
          new Employee(3540, "Daniel", "Smith", 'M', 2500.50)
        });
      List<CompanyCar> carTable =
        new List<CompanyCar>(new CompanyCar[]
        {
          new CompanyCar("092592", "Mercedes SLK", 3952),
          new CompanyCar("168368", "Ford Focus", 3540)
        });
      IEnumerable<Employee> raised = MapReduce.Map(employeeTable,
                                        employee =>
                                        {
                                          return 
                                          new Employee(
                                            employee.Id,
                                            employee.Name,
                                            employee.Surname,
                                            employee.Sex,
                                            employee.Salary + employee.Salary * 0.1);
                                        });
      var data = MapReduce.Map(employeeTable,
        employee =>
          {
            return new
            {
              Name = employee.Name,
              Surname = employee.Surname
            };
          });
      IEnumerable<Employee> filtered = MapReduce.Where(employeeTable, e => e.Salary > 1500);
      data = MapReduce.Map(filtered,
        employee =>
        {
          return new
          {
            Name = employee.Name,
            Surname = employee.Surname
          };
        });
      IEnumerable<string> converted = MapReduce.Map(numbers, x => x.ToString());
      string concatenation = MapReduce.Reduce(numbers, "", (accumulator, x) => accumulator + x.ToString());
      double salarySum = MapReduce.Reduce(employeeTable, 0.0, (accumulator,e) => accumulator + e.Salary);
      double filteredSum = MapReduce.Reduce(filtered, 0.0, (accumulator, e) => accumulator + e.Salary);
      IEnumerable<Employee> filteredWithReduce =
        MapReduce.Reduce(
          employeeTable,
          new List<Employee>(),
          (queryResult, e) =>
          {
            if (e.Salary > 1500)
              queryResult.Add(e);
            return queryResult;
          });
      data =
        MapReduce.Map(filteredWithReduce,
        employee =>
        {
          return new
          {
            Name = employee.Name,
            Surname = employee.Surname
          };
        });
      var dataWithReduce =
        MapReduce.Reduce(employeeTable,
                         new List<dynamic>(),
                         (queryResult, e) =>
                         {
                           queryResult.Add(
                               new
                               {
                                 Name = e.Name,
                                 Surname = e.Surname
                               });
                           return queryResult;
                         });
      var filterAndProjectionWithReduce =
        MapReduce.Reduce(employeeTable,
                         new List<dynamic>(),
                         (queryResult, e) =>
                         {
                           if (e.Salary > 1500)
                             queryResult.Add(
                               new
                               {
                                 Name = e.Name,
                                 Surname = e.Surname
                               });
                           return queryResult;
                         });
      IEnumerable<Tuple<Employee, CompanyCar>> employeecars =
        MapReduce.Join(employeeTable, carTable, t => t.Item1.Id == t.Item2.EmployeeId);
    }

    static List<EmployeeTuple> RaiseSalary(List<EmployeeTuple> employees)
    {
      List<EmployeeTuple> result = new List<EmployeeTuple>();
      for (int i = 0; i < employees.Count; i++)
      {
        result.Add(new EmployeeTuple
          (
            employees[i].Tuple.Item1,
            employees[i].Tuple.Item2,
            employees[i].Tuple.Item3,
            employees[i].Tuple.Item4,
            employees[i].Tuple.Item5 + employees[i].Tuple.Item5 * 0.1));
      }
      return result;
    }

    static List<Employee> RaiseSalary(List<Employee> employees)
    {
      List<Employee> result = new List<Employee>();
      for (int i = 0; i < employees.Count; i++)
      {
        result.Add(new Employee
          (
            employees[i].Id,
            employees[i].Name,
            employees[i].Surname,
            employees[i].Sex,
            employees[i].Salary + employees[i].Salary * 0.1));
      }
      return result;
    }

    static string[] Foo<T>(T[] numbers)
    {
      string[] result = new string[numbers.Length];
      for (int i = 0; i < numbers.Length; i++)
      {
        result[i] = numbers.ToString();
      }
      return result;
    }

    static int SumList(int[] l)
    {
      int sum = 0;
      for (int i = 0; i < l.Length; i++)
      {
        sum = sum + l[i];
      }
      return sum;
    }

    static string Concat<T>(T[] l)
    {
      string c = "";
      for (int i = 0; i < l.Length; i++)
      {
        c = c + l[i].ToString();
      }
      return c;
    }
  }
}
