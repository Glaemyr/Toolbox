using System;
using System.Collections;
using System.Data;
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

            Insert(connString, classToInsert, null, tabeSpec);
        }

        private static void Insert(string connString, object classToInsert, SqlConnection conn, Table tabeSpec = null)
        {
            if (tabeSpec == null) tabeSpec = GetTableSpec(classToInsert);
            if (!classToInsert.GetType().GetInterfaces().Contains(typeof(IEnumerable)))
            {


                var classType = classToInsert.GetType();


                string insertInto = "INSERT INTO " + tabeSpec.Schema + "." + tabeSpec.TableName + "(\n";
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

                if (conn == null)
                {
                    using (conn = new SqlConnection(connString))
                    {
                        var cmd = conn.CreateCommand();
                        cmd.CommandText = insertString;
                        conn.Open();
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = insertString;
                    if (conn.State == ConnectionState.Closed) conn.Open();
                    cmd.ExecuteNonQuery();
                }
            }
            else
            {
                if (IsTooBig((IEnumerable)classToInsert)) throw new ArgumentOutOfRangeException("More than a 1000 rows were tried to be inserted. Toolbox currently does not support that");


                var classType = classToInsert.GetType().GetGenericArguments()[0];

                string insertInto = "INSERT INTO " + tabeSpec.Schema + "." + tabeSpec.TableName + "\n";
                insertInto = insertInto.Replace("\"", "");
                string values = "VALUES\n";
                bool first = true;
                foreach (var info in classType.GetProperties())
                {
                    if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInInserter))) continue;
                    insertInto += first ? "(" + info.Name : "\n," + info.Name;
                    first = false;

                }

                var enumer = ((IEnumerable)classToInsert).GetEnumerator();
                while (enumer.MoveNext())
                {

                    first = true;
                    foreach (var info in classType.GetProperties())
                    {
                        if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInInserter))) continue;
                        values += first ? "(" + setQuotes(info, enumer.Current) : "\n," + setQuotes(info, enumer.Current);
                        first = false;
                    }
                    values += "),\n";
                }
                values = values.TrimEnd(new[] { ',', '\n' });

                var insertString = insertInto + ")\n" + values;

                if (conn == null)
                {


                    using (conn = new SqlConnection(connString))
                    {
                        var cmd = conn.CreateCommand();
                        cmd.CommandText = insertString;
                        conn.Open();
                        cmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = insertString;
                    if (conn.State == ConnectionState.Closed) conn.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }
        public static void Merge(string connString, object classToInsert, string[] on, string whenMatched = null, string whenNotMatched = null, string[] update = null, Table tabeSpec = null)
        {
            if (tabeSpec == null) tabeSpec = GetTableSpec(classToInsert);
            Type classType;
            if (classToInsert.GetType().GetInterfaces().Contains(typeof(IEnumerable)))
                classType = classToInsert.GetType().GetGenericArguments()[0];
            else classType = classToInsert.GetType();

            using (var conn = new SqlConnection(connString))
            {
                var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT TOP(0) * INTO #tmp_{tabeSpec.TableName} FROM {tabeSpec.Schema}.{tabeSpec.TableName}";
                conn.Open();
                cmd.ExecuteNonQuery();
                Insert(connString, classToInsert, conn, new Table(tableName: $"#tmp_{tabeSpec.TableName}", schema: tabeSpec.Schema));

                string onStr = "";
                var first = true;
                foreach (var s in on)
                {
                    onStr += first ? $"TARGET.{s} = SOURCE.{s}\n" : $"AND TARGET.{s} = SOURCE.{s}\n";
                    first = false;
                }

                string values = "VALUES (";
                string insert = "INSERT (";
                // TODO: __UPDATE
                first = true;
                foreach (var info in classType.GetProperties())
                {
                    if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInInserter))) continue;
                    insert += first ? info.Name : "\n," + info.Name;
                    values += first ? $"SOURCE.{info.Name}" : "\n," + $"SOURCE.{info.Name}";

                    first = false;
                }

                insert += ") ";
                values += ")";
                insert += values;
                string mergeSql = $"MERGE INTO {tabeSpec.Schema}.{tabeSpec.TableName} AS TARGET\n" +
                                  $"USING #tmp_{tabeSpec.TableName} AS SOURCE\n" +
                                  "ON\n" +
                                  onStr +
                                  (whenMatched != null ? $"WHEN MATCHED THEN\n{whenMatched}" : "") +
                                  (whenNotMatched != null ? $"WHEN NOT MATCHED THEN\n {whenNotMatched}" : "");

                mergeSql = mergeSql.Replace("{insert}", insert);
                mergeSql += ";";
                cmd.CommandText = mergeSql;
                cmd.ExecuteNonQuery();
            }
        }

        private static Table GetTableSpec(object classToInsert)
        {
            string tableName;
            string schemaName = "dbo";
            Type classType;
            if (classToInsert.GetType().GetInterfaces().Contains(typeof(IEnumerable)))
            {
                classType = classToInsert.GetType().GetGenericArguments()[0];
                if (classType.CustomAttributes.Any(a => a.AttributeType == typeof(Table)))
                {
                    var schemaAttribute = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table))
                        .NamedArguments.First(n => n.MemberName == "Schema").TypedValue.Value as string;
                    tableName = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table)).NamedArguments
                        .First(n => n.MemberName == "TableName").TypedValue.Value as string;
                    if (!string.IsNullOrEmpty(schemaAttribute)) schemaName = schemaAttribute;
                }
                else
                    tableName = classType.Name;

                return new Table(tableName: tableName, schema: schemaName);
            }


            classType = classToInsert.GetType();
            if (classType.CustomAttributes.Any(a => a.AttributeType == typeof(Table)))
            {
                var schemaAttribute = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table))
                    .NamedArguments.First(n => n.MemberName == "Schema").TypedValue.Value as string;
                tableName = classType.CustomAttributes.First(a => a.AttributeType == typeof(Table)).NamedArguments
                    .First(n => n.MemberName == "TableName").TypedValue.Value as string;
                if (!string.IsNullOrEmpty(schemaAttribute)) schemaName = schemaAttribute;
            }

            else
                tableName = classType.Name;

            return new Table(tableName: tableName, schema: schemaName);
        }
        private static bool IsTooBig(IEnumerable classToInsert)
        {
            var cnt = 0;
            var c = (ICollection)classToInsert;
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

            return cnt > 1000;
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
