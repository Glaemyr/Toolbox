using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace Toolbox.XML
{
    public class ClassPopulator
    {
        public static object MapToClassByPropertyName(XElement xml, Type rtnType, object args = null)
        {

            object returnedObject;
            //if (args == typeof(Array))
            //{

            //}
            //else
            returnedObject = Activator.CreateInstance(rtnType);


            List<PropertyInfo> modelProperties = returnedObject.GetType().GetProperties().ToList();

            foreach (PropertyInfo info in modelProperties)
            {
                if(info.GetCustomAttributes<XmlIgnoreAttribute>().Any())continue;
                var conversionType = Nullable.GetUnderlyingType(info.PropertyType) ?? info.PropertyType;

                var name = info.Name;

                if (conversionType.IsArray)
                {
                    name = conversionType.GetElementType().Name;
                    var elements = xml.Elements();
                    var enumerator = elements.GetEnumerator();
                    bool elementFound = false;
                    List<XElement> currentArrayElements = new List<XElement>();
                    XElement element;
                    while (enumerator.MoveNext())
                    {
                        element = enumerator.Current;
                        if (element.Attributes().Any(a => a.Name.LocalName == "type"))
                        {
                            var typeName = element.Attributes().First(a => a.Name.LocalName == "type").Value.Split(':')[1]; // Split out namespace
                            if (typeName != name) continue;
                            currentArrayElements.Add(element);
                        }

                        if (element.Name.LocalName != name) continue;
                        currentArrayElements.Add(element);

                    }
                    var listType = typeof(List<>);
                    var constructedListType = listType.MakeGenericType(conversionType.GetElementType());
                    dynamic eleList = Activator.CreateInstance(constructedListType);
                    foreach (var xElement in currentArrayElements)
                    {
                        var byPropertyName = MapToClassByPropertyName(xElement, conversionType.GetElementType());
                        eleList.Add(Convert.ChangeType(byPropertyName, conversionType));
                    }

                    // Make list of element type, loop trhough array - insert each element - MapToClassByPropertyName(element, conversionType.GetElementType(), conversionType.BaseType)
                    info.SetValue(returnedObject, Convert.ChangeType(eleList.ToArray(), conversionType));
                }
                else
                {
                    var xElements = xml.Elements();
                    var newXEle = xElements.First(e => e.Name.LocalName == name);
                    if(isNil(newXEle))continue;
                    if (!IsSimple(conversionType))
                    {
                        
                        info.SetValue(returnedObject,
                            Convert.ChangeType(MapToClassByPropertyName(newXEle, conversionType), conversionType));
                    }
                    else
                    {
                        info.SetValue(returnedObject, Convert.ChangeType(newXEle.Value, conversionType));
                    }
                }
            }



            return returnedObject;
        }

        private static bool isNil(XElement xElement)
        {
            if (xElement.Attributes().Any(a => a.Name.LocalName == "nil"))
            {
                var value = xElement.Attributes().First(a => a.Name.LocalName == "nil").Value; // Split out namespace
                return value == "true";
            }

            return false;

        }

        static bool IsSimple(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                // nullable type, check if the nested type is simple.
                return IsSimple(type.GetGenericArguments()[0]);
            }
            return type.IsPrimitive
                   || type.IsEnum
                   || type == typeof(string)
                   || type == typeof(decimal)
                   || type == typeof(DateTime);
        }
    }
}


/*

    {]}


    */
