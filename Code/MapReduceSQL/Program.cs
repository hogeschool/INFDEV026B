using System;
using System.Collections.Generic;
using System.Collections;

namespace MapReduceSQL
{
	class MainClass
	{
		public static void Main (string [] args)
		{
			//create a list
			List<int> ls = new List<int> (new int [] { 2016, 2017, 2009, 2010 });

			//SELECT years FROM ls 
			//process a list
			List<int> mList = ProccesList<int>.MapL (ls, (int x) => x);

			//print the output
			Console.WriteLine ("\n SELECT years FROM ls");
			foreach (int element in mList) {
				Console.WriteLine (element);
			}

			//SELECT years + 2 FROM ls 
			//process a list
			List<int> mList2 = ProccesList<int>.MapL (ls, (int x) => x + 2 );


			//print the output
			Console.WriteLine ("\n SELECT years + 2 FROM ls ");
			foreach (int element in mList2) {
				Console.WriteLine (element);
			}

			//SELECT year FROM ls WHERE year > 2016
			//process a list
			List<int> fList = ProccesList<int>.FilterL (ls, (int x) => x > 2016 );

			//print the output
			Console.WriteLine ("\n SELECT year FROM ls WHERE year > 2016");
			foreach (int element in fList) {
				Console.WriteLine (element);
			}


			//Test MapReduce with MongoDB 
			MongoSample.testMongo ();

		}

	}


	static class ProccesList<T>
	{


		//map will apply the function f to each element in the list
		public static List<U> MapL<U> (List<T> l, Func<T, U> f)
		{

			List<U> tempL = new List<U> ();
			foreach (T element in l) {
				U el = f (element);
				tempL.Add (el);
			}
			return tempL;
		}



		public static List<T> FilterL (List<T> l, Func<T, bool> p)
		{
			List<T> tempL = new List<T> ();
			foreach (T element in l) {
				if (p(element)){
					tempL.Add (element);
				}
			}
			return tempL;


		}


	}
}
