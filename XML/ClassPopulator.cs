using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Xml.Linq;

namespace Toolbox.XML
{
    public class ClassPopulator
    {
        public static object MapToClassByPropertyName(XElement xml,Type rtnType)
        {
            var returnedObject = Activator.CreateInstance(rtnType);
            List<PropertyInfo> modelProperties = returnedObject.GetType().GetProperties().ToList();

            foreach (PropertyInfo info in modelProperties)
            {
                var conversionType = Nullable.GetUnderlyingType(info.PropertyType) ?? info.PropertyType;
                var name = xml.Name.LocalName;
                if (conversionType.IsClass)
                {
                    info.SetValue(returnedObject, Convert.ChangeType(MapToClassByPropertyName(,conversionType), conversionType));
                }
                else
                {
                    info.SetValue(returnedObject, Convert.ChangeType(name, conversionType));
                }
            }



            return returnedObject;
        }
    }
}
