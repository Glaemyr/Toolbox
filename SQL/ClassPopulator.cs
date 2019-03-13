using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace Toolbox.SQL
{
    public class ClassPopulator
    {
        public static T MapToClass<T>(SqlDataReader reader) where T : class
        {
            T returnedObject = Activator.CreateInstance<T>();
            List<PropertyInfo> modelProperties = returnedObject.GetType().GetProperties().OrderBy(p => p.MetadataToken).ToList();
            for (int i = 0; i < modelProperties.Count; i++)
            {
                if (!reader.Read()) break;
                var pType = modelProperties[i].PropertyType;
                if (Nullable.GetUnderlyingType(pType) != null)
                {
                    pType = Nullable.GetUnderlyingType(pType);
                    if (reader[i] == null || reader[i] == DBNull.Value)
                    {
                        modelProperties[i].SetValue(returnedObject, null, null);
                        continue;
                    }
                }

                var value = reader.GetValue(i);
                modelProperties[i].SetValue(returnedObject, Convert.ChangeType(value, pType), null);

            }

            return returnedObject;
        }

        public static T MapToClassByPropertyName<T>(SqlDataReader reader) where T : class
        {
            T returnedObject = Activator.CreateInstance<T>();
            List<PropertyInfo> modelProperties = returnedObject.GetType().GetProperties().ToList();

            var type = typeof(T);
            var methods = type.GetMethods();
            if (!methods.Any((info => info.Name == "Add")))
            {
                //If if the type is not an IEnumerable, read 1 row and return that
                if (!reader.Read()) return null;
                foreach (PropertyInfo info in modelProperties)
                {
                    if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInPopulator))) continue;

                    var conversionType = Nullable.GetUnderlyingType(info.PropertyType) ?? info.PropertyType;
                    var value = reader[info.Name];
                    if (value is DBNull) continue;
                    info.SetValue(returnedObject, Convert.ChangeType(value, conversionType)); //TODO: reader[info.Name] can be changed to prioritize a custom tag
                }
            }
            else
            { // Else: get the first generic argument, make a list of that, add each row as an element, and return it as a T
                Type elemType = returnedObject.GetType().GetGenericArguments()[0];
                dynamic items = Activator.CreateInstance(typeof(T));
                var method = items.GetType().GetMethod("Add");
                while (reader.Read())
                {
                    var item = Activator.CreateInstance(elemType);
                    modelProperties = item.GetType().GetProperties().ToList();
                    foreach (PropertyInfo info in modelProperties)
                    {
                        var conversionType = Nullable.GetUnderlyingType(info.PropertyType) ?? info.PropertyType;
                        if (info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInPopulator))) continue;
                        var value = reader[info.Name];
                        info.SetValue(item, Convert.ChangeType(value, conversionType)); //TODO: reader[info.Name] can be changed to prioritize a custom tag
                    }

                    items.Add((dynamic)item);
                }

                return (T)items;
            }


            return returnedObject;
        }

        public static List<Dictionary<string, object>> DictionaryPopulator(SqlDataReader reader)
        {
            var rtn = new List<Dictionary<string, object>>();
            while (reader.Read())
            {
                var dict = new Dictionary<string, object>();
                var columns = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
                foreach (var col in columns)
                {
                    dict.Add(col, reader[col]);
                }
                rtn.Add(dict);
            }


            return rtn;
        }

        public static T MapToClass<T>(string connectionString) where T : class
        {
            var columnList = "";
            var first = true;
            foreach (var property in typeof(T).GetProperties())
            {
                if (property.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInPopulator))) continue;
                columnList += first ? $"[{property.Name}]\n" : $",[{property.Name}]\n";
                first = false;

            }

            var table = ClassInserter.GetTableSpec(Activator.CreateInstance<T>());

            using (var conn = new SqlConnection(connectionString))
            using (var cmd = new SqlCommand($"SELECT {columnList} FROM {table.Schema}.{table.TableName}", conn))
            {
                conn.Open();
                var sqlDataReader = cmd.ExecuteReader();
                return MapToClass<T>(sqlDataReader);
            }
        }

        public static T MapToClassByPropertyName<T>(string connectionString) where T : class
        {
            var columnList = "";
            var first = true;
            var type = typeof(T);
            var methods = type.GetMethods();
            if (methods.Any((info => info.Name == "Add"))) type = type.GetGenericArguments()[0];
            foreach (var property in type.GetProperties())
            {
                if (property.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInPopulator))) continue;
                columnList += first ? $"[{property.Name}]\n" : $",[{property.Name}]\n";
                first = false;

            }

            var table = ClassInserter.GetTableSpec(Activator.CreateInstance<T>());

            using (var conn = new SqlConnection(connectionString))
            using (var cmd = new SqlCommand($"SELECT {columnList} FROM {table.Schema}.{table.TableName}", conn))
            {
                conn.Open();
                var sqlDataReader = cmd.ExecuteReader();
                return MapToClassByPropertyName<T>(sqlDataReader);
            }
        }
    }
    public class SkipInPopulator : System.Attribute
    {
    }
}
