using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using CommonCode;
using System.Xml;
using MySql.Data.MySqlClient;

namespace NetflixImporter
{
    public partial class Form1 : Form
    {
        private MySqlConnection _sql = null;

        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            /*
            WebClient webClient = new WebClient();
            webClient.DownloadFile("http://api.netflix.com/catalog/titles/full?v=2.0?oauth_consumer_key=vwhxtj7gjyze6mvddb28znjd&oauth_nonce=FlucT&oauth_signature=lujB0gjGYHdm2DCDQQ%2FYxCzjvR8%3D&oauth_signature_method=HMAC-SHA1&oauth_timestamp=1328651121&oauth_version=1.0&output=xml", @"c:\netflix2.xml");
             */

            var conn = new MySqlConnectionInfo("localhost", "root", "Password01", "Netflix");

           // int countdown = 250;

            using (_sql = new MySqlConnection(conn.ConnectionString))
            {
                _sql.Open();

                MySqlTransaction transaction = _sql.BeginTransaction();

                _sql.NewCommandEx("DELETE FROM Title"       ).ExecuteNonQuery();
                _sql.NewCommandEx("DELETE FROM Category"    ).ExecuteNonQuery();
                _sql.NewCommandEx("DELETE FROM Link"        ).ExecuteNonQuery();
                _sql.NewCommandEx("DELETE FROM External"    ).ExecuteNonQuery();
                _sql.NewCommandEx("DELETE FROM Availability").ExecuteNonQuery();

                var textReader = new XmlTextReader("C:\\netflix.xml");

                // Read until end of file
                while (textReader.Read())
                {
                    XmlNodeType nType = textReader.NodeType;

                    // if node type is an element
                    if (nType == XmlNodeType.Element)
                    {
                        if (textReader.Name == "title_index_item")
                        {
                            ParseTitleIndexItem(textReader);
                            //countdown--;

                            //if (countdown == 0)
                             //   break;
                        }
                    }
                }

                transaction.Commit();
            }

            return;
         
        }

        class Link
        {
            public string rel;
            public string href;
            public string title;
        }

        class ExternalID
        {
            public string rel;
            public long Value;
        }

        class Category
        {
            public string scheme;
            public string label;
            public string term;
            public string status;
        }

        class Availability
        {
            public DateTime? DateFrom;
            public DateTime? DateTo;
            public string Label;
            public string Status;
            public string Scheme;
            public string Term;
        }

        private void ParseTitleIndexItem(XmlTextReader textReader)
        {
            string title       = string.Empty;
            string id          = string.Empty;
            string releaseYear = string.Empty;
            DateTime? updated = null;
            string studio = null;
            int? sequence_nr_in_series = null;

            var links            = new List<Link>();
            var externalIDs      = new List<ExternalID>();
            var categories       = new List<Category>();
            var availabilityList = new List<Availability>();


            while (textReader.Read())
            {
                XmlNodeType nType = textReader.NodeType;

                // if node type is an element
                if (nType == XmlNodeType.Element)
                {
                    switch (textReader.Name)
                    {
                        case "title":
                            textReader.Read();
                            title = textReader.Value;
                            break;

                        case "id":
                            textReader.Read();
                            id = textReader.Value;
                            break;

                        case "release_year":
                            textReader.Read();
                            releaseYear = textReader.Value;
                            break;

                        case "external_ids":
                            externalIDs.AddRange(ParseExternalIDs(textReader));
                            break;

                        case "delivery_formats":
                            availabilityList.AddRange(ParseDeliveryFormats(textReader));
                            break;

                        case "updated":
                            textReader.Read();
                            updated = FromUnixTimestamp(textReader.Value);
                            break;

                        case "studio":
                            textReader.Read();
                            studio = textReader.Value;
                            break;

                        case "category":
                            categories.Add(new Category
                            {
                                scheme = textReader["scheme"],
                                label  = textReader["label"],
                                term   = textReader["term"],
                                status = textReader["status"]
                            });
                            break;

                        case "link":
                            var link = new Link
                            {
                                href = textReader["href"],
                                title = textReader["title"],
                            };

                            switch (textReader["rel"])
                            {
                                case "alternate":
                                    link.rel = "alternate";
                                    break;

                                case "http://schemas.netflix.com/catalog/disc":
                                    link.rel = "disc";
                                    break;

                                case "http://schemas.netflix.com/catalog/person.actor":
                                    link.rel = "actor";
                                    break;

                                case "http://schemas.netflix.com/catalog/person.director":
                                    link.rel = "director";
                                    break;

                                case "http://schemas.netflix.com/catalog/program":
                                    link.rel = "program";
                                    break;

                                case "http://schemas.netflix.com/catalog/season":
                                    link.rel = "season";
                                    break;

                                case "http://schemas.netflix.com/catalog/series":
                                    link.rel = "series";
                                    break;

                                case "instant":
                                    link.rel = "instant";
                                    break;

                                default:
                                    break;
                            }

                            links.Add(link);
                            
                            break;

                        case "sequence_nr_in_series":
                            textReader.Read();
                            sequence_nr_in_series = int.Parse(textReader.Value);
                            break;

                        default:
                            break;
                    }
                }
                
                // if node type is an entity\
                if (nType == XmlNodeType.EndElement)
                {
                    if (textReader.Name == "title_index_item")
                    {
                        long titleID;

                        using (MySqlCommand command = _sql.NewCommandEx("INSERT INTO Title (TitleNetflixID, TitleReleaseYear, TitleTitle, TitleUpdated, TitleStudio, TitleSequenceNRInSeries) VALUES (?TitleNetflixID, ?TitleReleaseYear, ?TitleTitle, ?TitleUpdated, ?TitleStudio, ?TitleSequenceNRInSeries)"))
                        {
                            command.AddString("TitleNetflixID", id);
                            command.AddString("TitleTitle", title);
                            command.AddInt32("TitleReleaseYear", int.Parse(releaseYear));
                            command.AddInt32("TitleSequenceNRInSeries", sequence_nr_in_series);
                            command.AddDate("TitleUpdated", updated);
                            command.AddString("TitleStudio", studio);
                            command.ExecuteNonQuery();

                            titleID = command.LastInsertedId;
                        }

                        title       = string.Empty;
                        id          = string.Empty;
                        releaseYear = string.Empty;
                        updated     = null;
                        studio      = null;
                        sequence_nr_in_series = null;
                        
                        foreach (ExternalID externalID in externalIDs)
                        {
                            using (MySqlCommand command = _sql.NewCommandEx("INSERT INTO External (ExternalRel, ExternalValue, ExternalTitleID) VALUES (?ExternalRel, ?ExternalValue, ?ExternalTitleID)"))
                            {
                                //command.AddString("ExternalRel", externalID.rel);
                                command.AddString("ExternalValue", externalID.Value);
                                command.AddInt32("ExternalTitleID", titleID);

                                command.ExecuteNonQuery();
                            }
                        }
                        
                        foreach (Link link in links)
                        {
                            using (MySqlCommand command = _sql.NewCommandEx("INSERT INTO Link (LinkHref, LinkRel, LinkTitle, LinkTitleID) VALUES (?LinkHref, ?LinkRel, ?LinkTitle, ?LinkTitleID)"))
                            {
                                command.AddString("LinkHref", link.href);
                                command.AddString("LinkRel", link.rel);
                                command.AddString("LinkTitle", link.title);
                                command.AddInt32("LinkTitleID", titleID);

                                command.ExecuteNonQuery();
                            }
                        }
                        
                        foreach (Category category in categories)
                        {
                            using (MySqlCommand command = _sql.NewCommandEx("INSERT INTO Category (CategoryScheme, CategoryLabel, CategoryTerm, CategoryStatus, CategoryTitleID) VALUES (?CategoryScheme, ?CategoryLabel, ?CategoryTerm, ?CategoryStatus, ?CategoryTitleID)"))
                            {
                                command.AddString("CategoryScheme", category.scheme);
                                command.AddString("CategoryLabel", category.label);
                                command.AddString("CategoryTerm", category.term);
                                command.AddString("CategoryStatus", category.status);
                                command.AddInt32("CategoryTitleID", titleID);

                                command.ExecuteNonQuery();
                            }
                        }

                        foreach (Availability availability in availabilityList)
                        {
                            using (MySqlCommand command = _sql.NewCommandEx("INSERT INTO Availability (AvailabilityDateFrom, AvailabilityDateTo, AvailabilityLabel, AvailabilityTerm, AvailabilityStatus, AvailabilityTitleID, AvailabilityScheme) VALUES (?AvailabilityDateFrom, ?AvailabilityDateTo, ?AvailabilityLabel, ?AvailabilityTerm, ?AvailabilityStatus, ?AvailabilityTitleID, ?AvailabilityScheme)"))
                            {
                                command.AddDate("AvailabilityDateFrom", availability.DateFrom);
                                command.AddDate("AvailabilityDateTo", availability.DateTo);
                                command.AddString("AvailabilityLabel", availability.Label);
                                command.AddString("AvailabilityTerm", availability.Term);
                                command.AddString("AvailabilityStatus", availability.Status);
                                command.AddString("AvailabilityScheme", availability.Scheme);                                
                                command.AddInt32("AvailabilityTitleID", titleID);

                                command.ExecuteNonQuery();
                            }
                        }
                        
                        links.Clear();
                        externalIDs.Clear();
                        categories.Clear();
                        availabilityList.Clear();

                        return;
                    }
                }
            }

            return;
        }


        private DateTime FromUnixTimestamp(double timestamp)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return origin.AddSeconds(timestamp);
        }

        private DateTime FromUnixTimestamp(string timestamp)
        {
            return FromUnixTimestamp(double.Parse(timestamp));
        }

        private List<Availability> ParseDeliveryFormats(XmlTextReader textReader)
        {
            var availabilityList = new List<Availability>();

            DateTime? dateFrom = null;
            DateTime? dateTo = null;

            while (textReader.Read())
            {
                XmlNodeType nType = textReader.NodeType;

                // if node type is an element
                if (nType == XmlNodeType.Element)
                {
                    switch (textReader.Name)
                    {
                        case "availability":
                            dateFrom = null;
                            dateTo   = null;

                            if(textReader["available_from"] != null)
                                dateFrom = FromUnixTimestamp(textReader["available_from"]);

                            if (textReader["available_until"] != null)
                                dateTo = FromUnixTimestamp(textReader["available_until"]);
                            break;

                        case "category":
                            availabilityList.Add(new Availability
                            {
                                DateFrom = dateFrom,
                                DateTo   = dateTo,
                                Scheme   = textReader["scheme"],
                                Label    = textReader["label"],
                                Term     = textReader["term"],
                                Status   = textReader["status"]
                            });
                            break;
                    }
                }

                // if node type is an entity\
                if (nType == XmlNodeType.EndElement)
                {
                    if (textReader.Name == "delivery_formats")
                        return availabilityList;
                }
            }

            return availabilityList;
        }

        private List<ExternalID> ParseExternalIDs(XmlTextReader textReader)
        {
            var externalIDs = new List<ExternalID>();

            while (textReader.Read())
            {
                XmlNodeType nType = textReader.NodeType;

                // if node type is an element
                if (nType == XmlNodeType.Element)
                {
                    switch (textReader.Name)
                    {
                        case "id":
                            var externalID = new ExternalID();

                            externalID.rel = textReader["rel"];

                            textReader.Read();
                            externalID.Value = long.Parse(textReader.Value);

                            externalIDs.Add(externalID);
                            break;
                    }
                }

                // if node type is an entity\
                if (nType == XmlNodeType.EndElement)
                {
                    if (textReader.Name == "external_ids")
                        return externalIDs;
                }
            }

            return externalIDs;
        }
    }
}
