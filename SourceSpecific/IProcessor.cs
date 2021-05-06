using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;

namespace DataHarvester
{
    interface IProcessor
    {
        public Study ProcessData(XmlDocument d, DateTime? download_datetime);

        public void StoreData(Study s, string db_conn);
    }
}
