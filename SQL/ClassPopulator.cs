using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Toolbox.SQL
{
    public class ClassPopulator
    {
        public static T MapToClass<T>(SqlDataReader reader) where T : class
        {
            T returnedObject = Activator.CreateInstance<T>();
            List<PropertyInfo> modelProperties = returnedObject.GetType().GetProperties().OrderBy(p => p.MetadataToken).ToList();
            for (int i = 0; i < modelProperties.Count; i++)
                modelProperties[i].SetValue(returnedObject, Convert.ChangeType(reader.GetValue(i), modelProperties[i].PropertyType), null);
            return returnedObject;
        }

        public static T MapToClassByPropertyName<T>(SqlDataReader reader) where T : class
        {
            T returnedObject = Activator.CreateInstance<T>();
            List<PropertyInfo> modelProperties = returnedObject.GetType().GetProperties().OrderBy(p => p.MetadataToken).ToList();

            foreach (PropertyInfo info in modelProperties)
            {
                info.SetValue(returnedObject, Convert.ChangeType(reader[info.Name], info.PropertyType)); //TODO: reader[info.Name] can be changed to prioritize a custom tag
            }
            return returnedObject;
        }
    }
}
