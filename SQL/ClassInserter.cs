using System;
using System.Collections;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Reflection;


namespace Toolbox.SQL
{
    public class ClassInserter
    {
        public static void Insert(string connString, object classToInsert, Table tabeSpec = null)
        {
            string tableName;
            string schemaName = "dbo";
            if (!classToInsert.GetType().GetInterfaces().Contains(typeof(IEnumerable)))
            {


                var classType = classToInsert.GetType();
                if (classType.CustomAttributes.Any(a => a.AttributeType == typeof(Table)))
                {
                    var schemaAttribute = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table))
                        .NamedArguments.First(n => n.MemberName == "Schema").TypedValue.Value as string;
                    tableName = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table)).NamedArguments
                        .First(n => n.MemberName == "TableName").TypedValue.Value as string;
                    if (!string.IsNullOrEmpty(schemaAttribute)) schemaName = schemaAttribute;
                }
                else if (tabeSpec != null)
                {
                    var schemaAttribute = tabeSpec.Schema;
                    tableName = tabeSpec.TableName;
                    if (!string.IsNullOrEmpty(schemaAttribute)) schemaName = schemaAttribute;
                }
                else
                    tableName = classType.Name;

                string insertInto = "INSERT INTO " + schemaName + "." + tableName + "(\n";
                insertInto = insertInto.Replace("\"", "");
                string values = "VALUES\n(";
                bool first = true;
                foreach (var info in classType.GetProperties())
                {
                    if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInInserter))) continue;
                    insertInto += first ? info.Name : "\n," + info.Name;
                    values += first
                        ? setQuotes(info, classToInsert)
                        : "\n," + setQuotes(info, classToInsert).ToString();
                    first = false;
                }

                var insertString = insertInto + ")\n" + values + ")";


                using (var conn = new SqlConnection(connString))
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = insertString;
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
            }
            else
            {
                TooBigTest((IEnumerable) classToInsert);
                
                var classType = classToInsert.GetType().GetGenericArguments()[0];
                if (classType.CustomAttributes.Any(a => a.AttributeType == typeof(Table)))
                {
                    var schemaAttribute = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table))
                        .NamedArguments.First(n => n.MemberName == "Schema").TypedValue.Value as string;
                    tableName = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table)).NamedArguments
                        .First(n => n.MemberName == "TableName").TypedValue.Value as string;
                    if (!string.IsNullOrEmpty(schemaAttribute)) schemaName = schemaAttribute;
                }
                else if (tabeSpec != null)
                {
                    var schemaAttribute = tabeSpec.Schema;
                    tableName = tabeSpec.TableName;
                    if (!string.IsNullOrEmpty(schemaAttribute)) schemaName = schemaAttribute;
                }
                else
                    tableName = classType.Name;

                string insertInto = "INSERT INTO " + schemaName + "." + tableName + "\n";
                insertInto = insertInto.Replace("\"", "");
                string values = "VALUES\n";
                bool first = true;
                foreach (var info in classType.GetProperties())
                {
                    if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInInserter))) continue;
                    insertInto += first ? "(" + info.Name : "\n," + info.Name;
                    first = false;

                }

                var enumer = ((IEnumerable) classToInsert).GetEnumerator();
                while (enumer.MoveNext())
                {
                    
                    first = true;
                    foreach (var info in classType.GetProperties())
                    {
                        if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInInserter))) continue;
                        values += first? "("+setQuotes(info, enumer.Current): "\n," + setQuotes(info, enumer.Current);
                        first = false;
                    }
                    values += "),\n";
                }
                values = values.TrimEnd(new[] {',', '\n'});

                var insertString = insertInto + ")\n" + values;


                using (var conn = new SqlConnection(connString))
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = insertString;
                    conn.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void TooBigTest(IEnumerable classToInsert)
        {
            var cnt = 0;
            var c = (ICollection) classToInsert;
            if (c != null)
            {
                cnt = c.Count;
            }
            else
            {
                var enumer = ((IEnumerable)classToInsert).GetEnumerator();
                while (enumer.MoveNext())
                {
                    cnt++;
                }
            }

            if (cnt>1000)
            {
                throw new ArgumentOutOfRangeException($"Tried to insert {cnt} values.\nSQL prohibits inserts of more than a 1000 values. ");
            }
        }

        private static string setQuotes(PropertyInfo info, object obj)
        {
            dynamic val = info.GetValue(obj);
            var pType = info.PropertyType;
            if (pType == typeof(string) || pType == typeof(char))
                val = "'" + val + "'";
            if (pType == typeof(DateTimeOffset) || pType == typeof(DateTime))
            {
                val = "'" + val.ToString("s") + "'";
            }
            if (pType == typeof(bool))
            {
                val = (bool)val ? "1" : "0";
            }

            return val.ToString(CultureInfo.InvariantCulture);
        }
    }

    public class Table : System.Attribute
    {
        public string TableName { get; set; }
        public string Schema { get; set; }
        //private string _database;

        public Table(string tableName = "", string schema = "")
        {
            TableName = tableName;
            Schema = schema;
            //this._database = database;
        }
    }
    public class SkipInInserter : System.Attribute
    {
    }

}
