using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
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
                if (!reader.Read()) continue;
                modelProperties[i].SetValue(returnedObject, Convert.ChangeType(reader.GetValue(i), modelProperties[i].PropertyType), null);
            }

            return returnedObject;
        }

        public static T MapToClassByPropertyName<T>(SqlDataReader reader) where T : class
        {
            T returnedObject = Activator.CreateInstance<T>();
            List<PropertyInfo> modelProperties = returnedObject.GetType().GetProperties().ToList();

            if (!typeof(T).GetInterfaces().Contains(typeof(IEnumerable)))
            {
                //If if the type is not an IEnumerable, read 1 row and return that
                if (!reader.Read()) return null;
                foreach (PropertyInfo info in modelProperties)
                {
                    var conversionType = Nullable.GetUnderlyingType(info.PropertyType) ?? info.PropertyType;
                    var value = reader[info.Name];
                    if(value is DBNull) continue;
                    info.SetValue(returnedObject, Convert.ChangeType(value, conversionType)); //TODO: reader[info.Name] can be changed to prioritize a custom tag
                }
            }
            else
            { // Else: get the first generic argument, make a list of that, add each row as an element, and return it as a T
                Type elemType = returnedObject.GetType().GetGenericArguments()[0];
                IList items = (IList) Activator.CreateInstance(typeof(List<>).MakeGenericType(elemType));
                while (reader.Read())
                {
                    var item = Activator.CreateInstance(elemType);
                    modelProperties = item.GetType().GetProperties().ToList();
                    foreach (PropertyInfo info in modelProperties)
                    {
                        var conversionType = Nullable.GetUnderlyingType(info.PropertyType)?? info.PropertyType;
                        if(info.CustomAttributes.Any(a => a.AttributeType == typeof(SkipInPopulator)))continue;
                        var value = reader[info.Name];
                        info.SetValue(item, Convert.ChangeType(value, conversionType)); //TODO: reader[info.Name] can be changed to prioritize a custom tag
                    }

                    items.Add(item);
                }

                return (T) items;
            }


            return returnedObject;
        }
    }
    public class SkipInPopulator : System.Attribute
    {
    }
}
