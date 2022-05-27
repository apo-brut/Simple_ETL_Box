using ETLBox.DataFlow;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL_Aufgabe.Models
{
    public class TVerein
    {
       //[ColumnMap("Id")]
        // public int Id { get; set; }

        public string Nachname { get; set; }
        public string Vorname { get; set; }
        public string Geburtsdatum { get; set; }
        public string Geschlecht { get; set; }
        public string Adresse { get; set; }
        public string PLZ { get; set; }
        public string Bankverbindung { get; set; }
        public string Beitrittsdatum { get; set; }
        public string Status { get; set; }
        public string Telefon { get; set; }
    }
}
