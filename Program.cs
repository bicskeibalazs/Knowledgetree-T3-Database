using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orient.Client;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema;
using DBUpload;
using System.Threading;

namespace DBUpload
{
    class Program


    {
        //Egyszerű sql utasításokkal tölt fel, a for ciklusban beállított dokumentumokra.
        //Aminerekhez,
        public static void AThreadProc1()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            AminerVertex a = new AminerVertex { };
            url = @"F:\graph\aminer_papers_";
            for (int i = 0; i < 1/*55*/; i++)
            {
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    while ((json = sr.ReadLine()) != null)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);

                            if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                                {
                                    v.authors[currentAuthor].@class = "Author";
                                    v.authors[currentAuthor].@type = "d";
                                }
                            a.aminer_id = v.id;
                            a.title = v.title;
                            a.authors = v.authors;
                            a.year = v.year;
                            a.venue = v.venue;
                            a.keywords = v.keywords;
                            a.fos = v.fos;
                            a.n_citation = v.n_citation;
                            a.references = v.references;
                            a.page_stat = v.page_stat;
                            a.page_end = v.page_end;
                            a.doc_type = v.doc_type;
                            a.lang = v.lang;
                            a.publisher = v.publisher;
                            a.volume = v.volume;
                            a.issue = v.issue;
                            a.issn = v.issn;
                            a.isbn = v.isbn;
                            a.doi = v.doi;
                            a.pdf = v.pdf;
                            a.url = v.url;
                            a.Abstract = v.Abstract;
                            json = JsonConvert.SerializeObject(a).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                            database.Command("INSERT INTO Document CONTENT " + json);
                    }
                }
                Console.WriteLine("a" + i);
            }
        }
        public static void AThreadProc2()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            AminerVertex a = new AminerVertex { };
            url = @"F:\graph\aminer_papers_";
            for (int i = 1/*55*/; i < 2/*105*/; i++)
            {
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    while ((json = sr.ReadLine()) != null)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        a.aminer_id = v.id;
                        a.title = v.title;
                        a.authors = v.authors;
                        a.year = v.year;
                        a.venue = v.venue;
                        a.keywords = v.keywords;
                        a.fos = v.fos;
                        a.n_citation = v.n_citation;
                        a.references = v.references;
                        a.page_stat = v.page_stat;
                        a.page_end = v.page_end;
                        a.doc_type = v.doc_type;
                        a.lang = v.lang;
                        a.publisher = v.publisher;
                        a.volume = v.volume;
                        a.issue = v.issue;
                        a.issn = v.issn;
                        a.isbn = v.isbn;
                        a.doi = v.doi;
                        a.pdf = v.pdf;
                        a.url = v.url;
                        a.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(a).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        database.Command("INSERT INTO Document CONTENT " + json);
                    }
                }
                Console.WriteLine("a" + i);
            }
        }
        public static void AThreadProc3()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            AminerVertex a = new AminerVertex { };
            url = @"F:\graph\aminer_papers_";
            for (int i = 2/*105*/; i < 3/*155*/; i++)
            {
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    while ((json = sr.ReadLine()) != null)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        a.aminer_id = v.id;
                        a.title = v.title;
                        a.authors = v.authors;
                        a.year = v.year;
                        a.venue = v.venue;
                        a.keywords = v.keywords;
                        a.fos = v.fos;
                        a.n_citation = v.n_citation;
                        a.references = v.references;
                        a.page_stat = v.page_stat;
                        a.page_end = v.page_end;
                        a.doc_type = v.doc_type;
                        a.lang = v.lang;
                        a.publisher = v.publisher;
                        a.volume = v.volume;
                        a.issue = v.issue;
                        a.issn = v.issn;
                        a.isbn = v.isbn;
                        a.doi = v.doi;
                        a.pdf = v.pdf;
                        a.url = v.url;
                        a.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(a).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        database.Command("INSERT INTO Document CONTENT " + json);
                    }
                }
                Console.WriteLine("a" + i);
            }
        }
        //MAG-okhoz.
        public static void MThreadProc4()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            MagVertex m = new MagVertex { };
            url = @"F:\graph\mag_papers_";
            for (int i = 0; i < 1/*67*/; i++)
            {
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    while ((json = sr.ReadLine()) != null)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                            if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                                {
                                    v.authors[currentAuthor].@class = "Author";
                                    v.authors[currentAuthor].@type = "d";
                                }
                            m.mag_id = v.id;
                            m.title = v.title;
                            m.authors = v.authors;
                            m.year = v.year;
                            m.venue = v.venue;
                            m.keywords = v.keywords;
                            m.fos = v.fos;
                            m.n_citation = v.n_citation;
                            m.references = v.references;
                            m.page_stat = v.page_stat;
                            m.page_end = v.page_end;
                            m.doc_type = v.doc_type;
                            m.lang = v.lang;
                            m.publisher = v.publisher;
                            m.volume = v.volume;
                            m.issue = v.issue;
                            m.issn = v.issn;
                            m.isbn = v.isbn;
                            m.doi = v.doi;
                            m.pdf = v.pdf;
                            m.url = v.url;
                            m.Abstract = v.Abstract;
                            json = JsonConvert.SerializeObject(m).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                            database.Command("INSERT INTO Document CONTENT " + json);
                    }
                }
                Console.WriteLine("m" + i);
            }
        }
        public static void MThreadProc5()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            MagVertex m = new MagVertex { };
            url = @"F:\graph\mag_papers_";
            for (int i = 67; i < 68/*122*/; i++)
            {
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    while ((json = sr.ReadLine()) != null)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        m.mag_id = v.id;
                        m.title = v.title;
                        m.authors = v.authors;
                        m.year = v.year;
                        m.venue = v.venue;
                        m.keywords = v.keywords;
                        m.fos = v.fos;
                        m.n_citation = v.n_citation;
                        m.references = v.references;
                        m.page_stat = v.page_stat;
                        m.page_end = v.page_end;
                        m.doc_type = v.doc_type;
                        m.lang = v.lang;
                        m.publisher = v.publisher;
                        m.volume = v.volume;
                        m.issue = v.issue;
                        m.issn = v.issn;
                        m.isbn = v.isbn;
                        m.doi = v.doi;
                        m.pdf = v.pdf;
                        m.url = v.url;
                        m.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(m).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        database.Command("INSERT INTO Document CONTENT " + json);
                    }
                }
                Console.WriteLine("m" + i);
            }
        }
        public static void MThreadProc6()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");
            
            string url, json;
            Vertex v;

            MagVertex m = new MagVertex { };
            url = @"F:\graph\mag_papers_";
            for (int i = 122; i < 123/*167*/; i++)
            {
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    while ((json = sr.ReadLine()) != null)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        m.mag_id = v.id;
                        m.title = v.title;
                        m.authors = v.authors;
                        m.year = v.year;
                        m.venue = v.venue;
                        m.keywords = v.keywords;
                        m.fos = v.fos;
                        m.n_citation = v.n_citation;
                        m.references = v.references;
                        m.page_stat = v.page_stat;
                        m.page_end = v.page_end;
                        m.doc_type = v.doc_type;
                        m.lang = v.lang;
                        m.publisher = v.publisher;
                        m.volume = v.volume;
                        m.issue = v.issue;
                        m.issn = v.issn;
                        m.isbn = v.isbn;
                        m.doi = v.doi;
                        m.pdf = v.pdf;
                        m.url = v.url;
                        m.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(m).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        database.Command("INSERT INTO Document CONTENT " + json);
                    }
                }
                Console.WriteLine("m" + i);
            }
        }
        //Mint az előzőek, csak batchelve, az ideális batch méret tervezés alatt.
        public static void AmiTransThr1()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            AminerVertex a = new AminerVertex { };
            url = @"F:\graph\aminer_papers_";
            for (int i = 0; i < 52/*155*/; i++)
            {
                string batch;
                StringBuilder builder = new StringBuilder("BEGIN;\n", 250000000);
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    for (int rcount = 1; (json = sr.ReadLine()) != null; rcount++)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        a.aminer_id = v.id;
                        a.title = v.title;
                        a.authors = v.authors;
                        a.year = v.year;
                        a.venue = v.venue;
                        a.keywords = v.keywords;
                        a.fos = v.fos;
                        a.n_citation = v.n_citation;
                        a.references = v.references;
                        a.page_stat = v.page_stat;
                        a.page_end = v.page_end;
                        a.doc_type = v.doc_type;
                        a.lang = v.lang;
                        a.publisher = v.publisher;
                        a.volume = v.volume;
                        a.issue = v.issue;
                        a.issn = v.issn;
                        a.isbn = v.isbn;
                        a.doi = v.doi;
                        a.pdf = v.pdf;
                        a.url = v.url;
                        a.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(a).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        builder.Append("INSERT INTO Document CONTENT ");
                        builder.Append(json);
                        builder.Append(";\n");
                        if (rcount % 1000 == 0)
                        {
                            builder.Append("COMMIT;");
                            batch = builder.ToString();
                            Console.WriteLine(String.Concat(DateTime.Now, " a", i, " ", rcount / 1000));
                            database.SqlBatch(batch).Run();
                            builder.Clear();
                            builder.Append("BEGIN;\n");
                        }
                    }
                }
                builder.Append("COMMIT;");
                batch = builder.ToString();
                database.SqlBatch(batch).Run();
                Console.WriteLine(String.Concat(DateTime.Now, " a", i, " OK"));
            }
        }
        public static void AmiTransThr2()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            AminerVertex a = new AminerVertex { };
            url = @"F:\graph\aminer_papers_";
            for (int i = 52; i < 104/*155*/; i++)
            {
                string batch;
                StringBuilder builder = new StringBuilder("BEGIN;\n", 250000000);
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    for (int rcount = 1; (json = sr.ReadLine()) != null; rcount++)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        a.aminer_id = v.id;
                        a.title = v.title;
                        a.authors = v.authors;
                        a.year = v.year;
                        a.venue = v.venue;
                        a.keywords = v.keywords;
                        a.fos = v.fos;
                        a.n_citation = v.n_citation;
                        a.references = v.references;
                        a.page_stat = v.page_stat;
                        a.page_end = v.page_end;
                        a.doc_type = v.doc_type;
                        a.lang = v.lang;
                        a.publisher = v.publisher;
                        a.volume = v.volume;
                        a.issue = v.issue;
                        a.issn = v.issn;
                        a.isbn = v.isbn;
                        a.doi = v.doi;
                        a.pdf = v.pdf;
                        a.url = v.url;
                        a.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(a).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        builder.Append("INSERT INTO Document CONTENT ");
                        builder.Append(json);
                        builder.Append(";\n");
                        if (rcount % 1000 == 0)
                        {
                            builder.Append("COMMIT;");
                            batch = builder.ToString();
                            Console.WriteLine(String.Concat(DateTime.Now, " a", i, " ", rcount / 1000));
                            database.SqlBatch(batch).Run();
                            builder.Clear();
                            builder.Append("BEGIN;\n");
                        }
                    }
                }
                builder.Append("COMMIT;");
                batch = builder.ToString();
                database.SqlBatch(batch).Run();
                Console.WriteLine(String.Concat(DateTime.Now, " a", i, " OK"));
            }
        }
        public static void AmiTransThr3()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            AminerVertex a = new AminerVertex { };
            url = @"F:\graph\aminer_papers_";
            for (int i = 104; i < 155/*155*/; i++)
            {
                string batch;
                StringBuilder builder = new StringBuilder("BEGIN;\n", 250000000);
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    for (int rcount = 1; (json = sr.ReadLine()) != null; rcount++)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        a.aminer_id = v.id;
                        a.title = v.title;
                        a.authors = v.authors;
                        a.year = v.year;
                        a.venue = v.venue;
                        a.keywords = v.keywords;
                        a.fos = v.fos;
                        a.n_citation = v.n_citation;
                        a.references = v.references;
                        a.page_stat = v.page_stat;
                        a.page_end = v.page_end;
                        a.doc_type = v.doc_type;
                        a.lang = v.lang;
                        a.publisher = v.publisher;
                        a.volume = v.volume;
                        a.issue = v.issue;
                        a.issn = v.issn;
                        a.isbn = v.isbn;
                        a.doi = v.doi;
                        a.pdf = v.pdf;
                        a.url = v.url;
                        a.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(a).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        builder.Append("INSERT INTO Document CONTENT ");
                        builder.Append(json);
                        builder.Append(";\n");
                        if (rcount % 1000 == 0)
                        {
                            builder.Append("COMMIT;");
                            batch = builder.ToString();
                            Console.WriteLine(String.Concat(DateTime.Now, " a", i, " ", rcount / 1000));
                            database.SqlBatch(batch).Run();
                            builder.Clear();
                            builder.Append("BEGIN;\n");
                        }
                    }
                }
                builder.Append("COMMIT;");
                batch = builder.ToString();
                database.SqlBatch(batch).Run();
                Console.WriteLine(String.Concat(DateTime.Now, " a", i, " OK"));
            }
        }
        public static void MagTransThr4()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            MagVertex m = new MagVertex { };
            url = @"F:\graph\mag_papers_";
            for (int i = 0; i < 56/*167*/; i++)
            {
                string batch;
                StringBuilder builder = new StringBuilder("BEGIN;\n", 250000000);
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    for(int rcount = 1; (json = sr.ReadLine()) != null; rcount++)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        m.mag_id = v.id;
                        m.title = v.title;
                        m.authors = v.authors;
                        m.year = v.year;
                        m.venue = v.venue;
                        m.keywords = v.keywords;
                        m.fos = v.fos;
                        m.n_citation = v.n_citation;
                        m.references = v.references;
                        m.page_stat = v.page_stat;
                        m.page_end = v.page_end;
                        m.doc_type = v.doc_type;
                        m.lang = v.lang;
                        m.publisher = v.publisher;
                        m.volume = v.volume;
                        m.issue = v.issue;
                        m.issn = v.issn;
                        m.isbn = v.isbn;
                        m.doi = v.doi;
                        m.pdf = v.pdf;
                        m.url = v.url;
                        m.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(m).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        builder.Append("INSERT INTO Document CONTENT ");
                        builder.Append(json);
                        builder.Append(";\n");
                        if(rcount % 1000 == 0)
                        {
                            builder.Append("COMMIT;");
                            batch = builder.ToString();
                            Console.WriteLine(String.Concat(DateTime.Now, " m", i, " ", rcount / 1000));
                            database.SqlBatch(batch).Run();
                            builder.Clear();
                            builder.Append("BEGIN;\n");
                        }
                    }
                }
                builder.Append("COMMIT;");
                batch = builder.ToString();
                database.SqlBatch(batch).Run();
                Console.WriteLine(String.Concat(DateTime.Now, " m", i, " OK"));
            }
        }
        public static void MagTransThr5()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            MagVertex m = new MagVertex { };
            url = @"F:\graph\mag_papers_";
            for (int i = 56; i < 112/*167*/; i++)
            {
                string batch;
                StringBuilder builder = new StringBuilder("BEGIN;\n", 250000000);
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    for (int rcount = 1; (json = sr.ReadLine()) != null; rcount++)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        m.mag_id = v.id;
                        m.title = v.title;
                        m.authors = v.authors;
                        m.year = v.year;
                        m.venue = v.venue;
                        m.keywords = v.keywords;
                        m.fos = v.fos;
                        m.n_citation = v.n_citation;
                        m.references = v.references;
                        m.page_stat = v.page_stat;
                        m.page_end = v.page_end;
                        m.doc_type = v.doc_type;
                        m.lang = v.lang;
                        m.publisher = v.publisher;
                        m.volume = v.volume;
                        m.issue = v.issue;
                        m.issn = v.issn;
                        m.isbn = v.isbn;
                        m.doi = v.doi;
                        m.pdf = v.pdf;
                        m.url = v.url;
                        m.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(m).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        builder.Append("INSERT INTO Document CONTENT ");
                        builder.Append(json);
                        builder.Append(";\n");
                        if (rcount % 1000 == 0)
                        {
                            builder.Append("COMMIT;");
                            batch = builder.ToString();
                            Console.WriteLine(String.Concat(DateTime.Now, " m", i, " ", rcount / 1000));
                            database.SqlBatch(batch).Run();
                            builder.Clear();
                            builder.Append("BEGIN;\n");
                        }
                    }
                }
                builder.Append("COMMIT;");
                batch = builder.ToString();
                database.SqlBatch(batch).Run();
                Console.WriteLine(String.Concat(DateTime.Now, " m", i, " OK"));
            }
        }
        public static void MagTransThr6()
        {
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");

            string url, json;
            Vertex v;

            MagVertex m = new MagVertex { };
            url = @"F:\graph\mag_papers_";
            for (int i = 112; i < 167/*167*/; i++)
            {
                string batch;
                StringBuilder builder = new StringBuilder("BEGIN;\n", 250000000);
                using (StreamReader sr = File.OpenText(url + i + ".txt"))
                {
                    for (int rcount = 1; (json = sr.ReadLine()) != null; rcount++)
                    {
                        v = JsonConvert.DeserializeObject<Vertex>(json);
                        if (v.authors != null) for (int currentAuthor = 0; currentAuthor < v.authors.Length; currentAuthor++)
                            {
                                v.authors[currentAuthor].@class = "Author";
                                v.authors[currentAuthor].@type = "d";
                            }
                        m.mag_id = v.id;
                        m.title = v.title;
                        m.authors = v.authors;
                        m.year = v.year;
                        m.venue = v.venue;
                        m.keywords = v.keywords;
                        m.fos = v.fos;
                        m.n_citation = v.n_citation;
                        m.references = v.references;
                        m.page_stat = v.page_stat;
                        m.page_end = v.page_end;
                        m.doc_type = v.doc_type;
                        m.lang = v.lang;
                        m.publisher = v.publisher;
                        m.volume = v.volume;
                        m.issue = v.issue;
                        m.issn = v.issn;
                        m.isbn = v.isbn;
                        m.doi = v.doi;
                        m.pdf = v.pdf;
                        m.url = v.url;
                        m.Abstract = v.Abstract;
                        json = JsonConvert.SerializeObject(m).Replace("Abstract", "abstract").Replace("type", "@type").Replace("class", "@class").Replace("doc_@type", "doc_type");
                        builder.Append("INSERT INTO Document CONTENT ");
                        builder.Append(json);
                        builder.Append(";\n");
                        if (rcount % 1000 == 0)
                        {
                            builder.Append("COMMIT;");
                            batch = builder.ToString();
                            Console.WriteLine(String.Concat(DateTime.Now, " m", i, " ", rcount / 1000));
                            database.SqlBatch(batch).Run();
                            builder.Clear();
                            builder.Append("BEGIN;\n");
                        }
                    }
                }
                builder.Append("COMMIT;");
                batch = builder.ToString();
                database.SqlBatch(batch).Run();
                Console.WriteLine(String.Concat(DateTime.Now, " m", i, " OK"));
            }
        }
        //Éleket tölt fel, jelenleg nem működik megfelelően.
        public static void Edge1()
        {
            try
            {
                OServer server = Server.Connect();
                ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");
                string url, json;
                Vertex v;

                AminerVertex a = new AminerVertex { };
                url = @"F:\graph\aminer_papers_";
                for (int i = 0; i < 1; i++)
                {
                    using (StreamReader sr = File.OpenText(url + i + ".txt"))
                    {
                        while ((json = sr.ReadLine()) != null)
                        {
                            v = JsonConvert.DeserializeObject<Vertex>(json);
                            a.aminer_id = v.id;
                            a.references = v.references;
                            if (a.references != null) if (a.references.Length != 0) foreach (string refr in a.references)
                                    //if (a.references.Length != 0) foreach (string refr in a.references)
                                    //{
                                    //    database.Command("create edge from (select from document where aminer_id = '" + a.aminer_id + "') to (select from document where aminer_id = '" + refr + "')");
                                    //}
                                    for (int refn = 0; refn < a.references.Length; refn++)
                                    {
                                        database.Command("create edge from (select from document where aminer_id = '" + a.aminer_id + "') to (select from document where aminer_id = '" + a.references[refn] + "')");
                                    }
                        }
                    }
                    Console.WriteLine("a" + i);
                }
            }
            catch (OException e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public static void Edge2()
        {
            try
            {
                OServer server = Server.Connect();
                ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");
                string url, json;
                Vertex v;

                AminerVertex a = new AminerVertex { };
                url = @"F:\graph\aminer_papers_";
                for (int i = 1; i < 2; i++)
                {
                    using (StreamReader sr = File.OpenText(url + i + ".txt"))
                    {
                        while ((json = sr.ReadLine()) != null)
                        {
                            v = JsonConvert.DeserializeObject<Vertex>(json);
                            a.aminer_id = v.id;
                            a.references = v.references;
                            //if (a.references.Length != 0) foreach (string refr in a.references)
                            //{
                            //    database.Command("create edge from (select from document where aminer_id = '" + a.aminer_id + "') to (select from document where aminer_id = '" + refr + "')");
                            //}
                            if (a.references != null) for (int refn = 0; refn < a.references.Length; refn++)
                            {
                                database.Command("create edge from (select from document where aminer_id = '" + a.aminer_id + "') to (select from document where aminer_id = '" + a.references[refn] + "')");
                            }
                        }
                    }
                    Console.WriteLine("a" + i);
                }
            }
            catch (OException e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public static void Edge3()
        {
            try
            {
                OServer server = Server.Connect();
                ODatabase database = new ODatabase("localhost", 2424, "TreeOfScience", ODatabaseType.Graph, "admin", "admin");
                string url, json;
                Vertex v;

                AminerVertex a = new AminerVertex { };
                url = @"F:\graph\aminer_papers_";
                for (int i = 2; i < 3; i++)
                {
                    using (StreamReader sr = File.OpenText(url + i + ".txt"))
                    {
                        while ((json = sr.ReadLine()) != null)
                        {
                            v = JsonConvert.DeserializeObject<Vertex>(json);
                            a.aminer_id = v.id;
                            a.references = v.references;
                            //if (a.references.Length != 0) foreach (string refr in a.references)
                            //{
                            //    database.Command("create edge from (select from document where aminer_id = '" + a.aminer_id + "') to (select from document where aminer_id = '" + refr + "')");
                            //}
                            if (a.references != null) for(int refn = 0; refn < a.references.Length; refn++)
                            {
                                database.Command("create edge from (select from document where aminer_id = '" + a.aminer_id + "') to (select from document where aminer_id = '" + a.references[refn] + "')");
                            }
                        }
                    }
                    Console.WriteLine("a" + i);
                }
            }
            catch (OException e)
            {
                Console.WriteLine(e.Message);
            }
        }

        static void Main(string[] args)
        {
            /*
            Thread thr1 = new Thread(new ThreadStart(AmiTransThr1)),
            thr2 = new Thread(new ThreadStart(AmiTransThr2)),
            thr3 = new Thread(new ThreadStart(AmiTransThr3)),
            thr4 = new Thread(new ThreadStart(MagTransThr4)),
            thr5 = new Thread(new ThreadStart(MagTransThr5)),
            thr6 = new Thread(new ThreadStart(MagTransThr6));
            thr1.Start();
            thr2.Start();
            thr3.Start();
            thr4.Start();
            thr5.Start();
            thr6.Start();
            */
            // Thread thr1 = new Thread(new ThreadStart(MagTransThr));
            // thr1.Start();
            /*
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, "TOSDB", ODatabaseType.Graph, "admin", "admin");
            bool succes = true;
            try
            {
                database.Command("create edge from (select from document where aminer_id = '1a53e9ab9eb7602d970354a97e') to (select from document where aminer_id = '2a53e9ab9eb7602d970354a97easd')");
                server.
            }
            catch(OException e)
            {
                succes = false;
            }
            if(succes) Console.WriteLine("ok");
            */
            //Szálakat kell indítani, és magic.
            Thread thr1 = new Thread(new ThreadStart(Edge1)),
            thr2 = new Thread(new ThreadStart(Edge2)),
            thr3 = new Thread(new ThreadStart(Edge3));
            thr1.Start();
            thr2.Start();
            thr3.Start();
            
            Console.ReadKey();
        }
    }
}