using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orient.Client;

namespace onlabtest
{
    class Database
    {
        public ODatabase openDatabase(string _host, int _port,
            string _dbName, string _user, string _passwd)
        {

            // CONSOLE LOG
            Console.WriteLine("Opening Database: {0}", _dbName);

            // OPEN DATABASE
            ODatabase database = new ODatabase(_host, _port, _dbName,
                ODatabaseType.Graph, _user, _passwd);

            // RETURN ODATABASE INSTANCE
            return database;
        }
    }
}