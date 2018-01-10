using System;
using System.Collections.Generic;
using MapReduce;


namespace AirportDB
{
  public enum AirportSize { SMALL, MEDIUM, LARGE }

  public class Company
  {
    public string Name { get; set; }
    public int FleetSize { get; set; }
    public double Assets { get; set; }

    public Company(string name, int size, double assets)
    {
      Name = name;
      FleetSize = size;
      Assets = assets;
    }
  }

  public class Flight
  {
    public string Code { get; set; }
    public string PlaneName { get; set; }
    public string CompanyName { get; set; }
    public string DestinationCity { get; set; }
    public string DestinationAirport { get; set; }
    public double Price { get; set; }

    public Flight(string code, string name, string companyName, string destinationCity, string destinationAirport, double price)    
    {
      Code = code;
      PlaneName = name;
      CompanyName = companyName;
      DestinationCity = destinationCity;
      DestinationAirport = destinationAirport;
      Price = price;
    }
  }

  public class Airport
  {
    public string Name { get; set; }
    public string City { get; set; }
    public int Capacity { get; set; }
    public AirportSize Size { get; set; }

    public Airport(string name, string city, int capacity, AirportSize size)
    {
      Name = name;
      City = city;
      Capacity = capacity;
      Size = size;
    }
  }

  public class AirportDatabase
  {
    public IEnumerable<Company> CompanyTable { get; set; }
    public IEnumerable<Flight> FlightTable { get; set; }
    public IEnumerable<Airport> AirportTable { get; set; }

    public AirportDatabase(IEnumerable<Company> companies, IEnumerable<Flight> flights, IEnumerable<Airport> airports)
    {
      CompanyTable = companies;
      FlightTable = flights;
      AirportTable = airports;
    }

    public static AirportDatabase Test()
    {
      Company[] companies = new Company[]
      {
        new Company("RyanAir", 50, 3000000),
        new Company("Lufthansa", 200, 50000000),
        new Company("Transavia", 75, 6000000),
        new Company("KLM", 200, 3500000)
      };
      Airport[] airports = new Airport[]
      {
        new Airport("Fiumicino", "Rome", 300, AirportSize.LARGE),
        new Airport("Rotteram-DenHaag", "Rotterdam", 40, AirportSize.SMALL),
        new Airport("Heathrow", "London", 100, AirportSize.MEDIUM),
        new Airport("Stansted", "London", 75, AirportSize.SMALL),
        new Airport("Gatwick", "London", 400, AirportSize.LARGE)
      };
      Flight[] flights = new Flight[]
      {
        new Flight("RA9395", "ASX", "RyanAir", "Rome", "Fiumicino", 60),
        new Flight("RA9334", "TRE", "RyanAir", "London", "Heathrow", 39.95),
        new Flight("LF9212", "FAF", "Lufthansa", "London", "Stansted", 250),
        new Flight("LF9210", "FAX", "Lufthansa", "Rome", "Fiumicino", 300),
        new Flight("KL9053", "WRE", "KLM", "London", "Gatwick", 200),
        new Flight("TR8268", "ARE", "Transavia", "Rotterdam", "Rotterdam-DenHaag", 225.55)
      };
      return new AirportDatabase(companies, flights, airports);
    }

    /*
     * select *
     * from flight
    */
    public IEnumerable<Flight> Query1()
    {
        return FlightTable.Map(x => x);
    }

    /*
     * select code, company, destinationAirport
     * from flight
     */

    public IEnumerable<Tuple<string, string, string>> Query2()
    {
      return FlightTable.Map(flight => new Tuple<string, string, string>(flight.Code, flight.CompanyName, flight.DestinationAirport));
    }

    /*
     * select code, destinationAirport
     * from flight
     * where company = 'Lufthansa'
     */

    public IEnumerable<Tuple<string, string>> Query3()
    {
      return 
        FlightTable.Reduce
        (new List<Flight>(), (l, flight) =>
          {
            if (flight.CompanyName == "Lufthansa")
              l.Add(flight);
            return l;
          }).Map
        (flight => new Tuple<string, string>(flight.Code, flight.DestinationAirport));
    }

    /*
     * select *
     * from flights f, airport a
     * where f.destinationAirport = a.name && f.destinationCity = a.city
     * */

    public IEnumerable<Tuple<Flight, Airport>> Query4()
    {
      return
        FlightTable.Join
        (AirportTable, t => t.Item1.DestinationAirport == t.Item2.Name &&
                            t.Item1.DestinationCity == t.Item2.City);
    }

    /*
     * select f.code, f.company, a.size
     * from flights f, airport a
     * where f.destinationAirport = a.name && f.destinationCity = a.city && f.company = Lufthansa
     * */

    public IEnumerable<Tuple<string, string, AirportSize>> Query5()
    {
      return
        FlightTable.Join(
        AirportTable, t => t.Item1.DestinationAirport == t.Item2.Name &&
                            t.Item1.DestinationCity == t.Item2.City &&
                            t.Item1.CompanyName == "Lufthansa").Map(
        t => new Tuple<string, string, AirportSize>(t.Item1.Code, t.Item1.CompanyName, t.Item2.Size));
    }

    /* 
     * select sum(f.price)
     * from flights f, company c
     * where f.companyName = c.name and c.name = "Lufthansa"
     */

    public double Query6()
    {
      return
        FlightTable.Join(
        CompanyTable, t => t.Item1.CompanyName == t.Item2.Name && t.Item2.Name == "Lufthansa").Reduce(
        0.0, (s, t) => s += t.Item1.Price);
    }
  }
}
