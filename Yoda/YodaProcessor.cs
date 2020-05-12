using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Net;
using System.Web;


namespace DataHarvester.Yoda
{
	public class YodaProcessor
	{
		HelperFunctions hp;


		public YodaProcessor()
		{
			hp = new HelperFunctions();
		}


		public Study ProcessData(YodaDataLayer repo, Yoda_Record st)
		{
			Study s = new Study();

			return s;

		}


		public void StoreData(YodaCopyHelpers yoda_mappings, YodaDataLayer repo, Study s)
		{
			//
		}



		public SuppDoc FindSuppDoc(List<SuppDoc> supp_docs, string name)
		{
			SuppDoc sd = null;
			foreach (SuppDoc s in supp_docs)
			{
				if (s.doc_name == name)
				{
					sd = s;
					break;
				}
			}
			return sd;
		}

	}
}
