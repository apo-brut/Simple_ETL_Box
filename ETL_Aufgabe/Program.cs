using ETL_Aufgabe.Models;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using System.Globalization;

namespace ETL_Aufgabe
{
    class Program
    {
        static void Main(string[] args)
        {
          //  Bereitstellen für Datenbank Verbindung

           const string _connectionString = "Server=localhost,3306; Database=club_db; User Id=root; Password=;convert zero datetime=True";
            MariaDbConnectionManager mysql = new MariaDbConnectionManager(new MariaDbConnectionString(_connectionString));
            ControlFlow.DefaultDbConnection = mysql;
            // Tabelle erstellen nach ETL Prozess

            CreateTableTask.CreateIfNotExists("TVerein", new List<TableColumn>
            {
                new TableColumn("Id", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
                new TableColumn("Nachname", "VARCHAR(50)", allowNulls: true),
                new TableColumn("Vorname", "VARCHAR(50)", allowNulls: true),
                new TableColumn("Geburtsdatum", "VARCHAR(50)", allowNulls: true),
                new TableColumn("Geschlecht", "VARCHAR(20)", allowNulls: true),
                new TableColumn("Adresse", "VARCHAR(100)", allowNulls: true),
                new TableColumn("PLZ", "VARCHAR(20)", allowNulls: true),
                new TableColumn("Beitrittsdatum", "VARCHAR(50)", allowNulls: true),
                new TableColumn("Status", "VARCHAR(25)", allowNulls: true),
                new TableColumn("Bankverbindung", "VARCHAR(50)", allowNulls: true),
                new TableColumn("Telefon", "VARCHAR(50)", allowNulls: true)
            });

            // Verbindung zum Sourcen zur VEREIN1 und VEREIN2

            // Extrahieren wir hier, wir nehmen die csv. dateis und erstellen in stringarray

            CsvSource<string[]> verein1 = new CsvSource<string[]>(@"C:\ETLBox_Data\ETL_VEREIN1.csv");
            CsvSource<string[]> verein2 = new CsvSource<string[]>(@"C:\ETLBox_Data\ETL_VEREIN2.csv");

            // Spalten TRANSformieren Stringrrays transformiert nach TVerein objects
            RowTransformation<string[], TVerein> v1transform = new RowTransformation<string[], TVerein>(
                row => new TVerein
                {
                    Nachname = row[1],
                    Vorname = row[2],
                    Geburtsdatum = DateTime.ParseExact(row[3],"dd.MM.yyyy", CultureInfo.InvariantCulture).ToString(),
                    Geschlecht = row[4],
                    Adresse = row[5],
                    PLZ = row[6],
                    Telefon = row[7],
                    Beitrittsdatum = DateTime.ParseExact(row[8], "dd.MM.yyyy", CultureInfo.InvariantCulture).ToString(),
                    Status = row[9],
                }
                );

            RowTransformation<string[], TVerein> v2transform = new RowTransformation<string[], TVerein>(
             row => new TVerein
             {
                 Nachname = row[1].Split(",")[1],
                 Vorname = row[1].Split(",")[0],
                 Geburtsdatum = DateTime.ParseExact(row[2], "M/d/yyyy", CultureInfo.InvariantCulture).ToString(),
                 Geschlecht = Sexconvert(row[3]),
                 Adresse = row[4] + " " + row[5],
                 PLZ = row[6],
                 Bankverbindung = row[7],
                 Beitrittsdatum = DateTime.ParseExact(row[8], "M/d/yyyy", CultureInfo.InvariantCulture).ToString(),
                 Status = Statusconvert(row[9]),
             }
             );

            // Ziel der Extraction Lade ist definiert
            DbDestination<TVerein> dbDestination = new DbDestination<TVerein>(mysql, "TVerein");


            // Ziel der Log Datei
            var textDest = new TextDestination<TVerein>(@"C:\ETLBox_Data\tverein.log");
            textDest.WriteLineFunc = row => {
                Console.WriteLine($"{row.Nachname}\t{row.Vorname}\t{row.Status}");
                return $"{row.Nachname}\t{row.Vorname}\t{row.Status}\t{row.Geburtsdatum}\t{row.Adresse}\t{row.Geschlecht}";
            };

    
            using var loggerFactory = LoggerFactory.Create(builder => {
                builder
                    .AddFilter("Microsoft", Microsoft.Extensions.Logging.LogLevel.Warning)
                    .AddFilter("System", Microsoft.Extensions.Logging.LogLevel.Warning)
                    .SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace)
                    .AddNLog("nlog.config");
            });
            ETLBox.Logging.Logging.LogInstance = loggerFactory.CreateLogger("Default");

         
            // die eingelesenen Dateis senden nach Transformierungsprozess

            verein1.LinkTo(v1transform);
            verein2.LinkTo(v2transform);
            // die Objects sind zur DB und Log dateis geladen
            v1transform.LinkTo(textDest);
            v1transform.LinkTo(dbDestination);
            v2transform.LinkTo(textDest);
            v2transform.LinkTo(dbDestination);
            verein1.Execute();
            verein2.Execute();
            //Network.Execute(verein1, verein2);
        }
        public static string Sexconvert(string sex)
        {
            if (sex == "Maennlich")
            {
                return "M";
            }
            if (sex == "Weiblich")
            {
                return "W";

            }
            return "D";
        }
        public static string Statusconvert (string status)
        {
            if(status == "B")
            {
                return "Begruender";

            }
            if(status == "C")
            {
                return "Chef";
            }
            if( status == "V")
            {
                return "Verwalter";
            }
            return "Mitglied";
        }
    }
    
}

