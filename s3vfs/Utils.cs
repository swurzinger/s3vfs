using System;
using System.Collections.Generic;
using System.Linq;

namespace s3vfs
{
    public static class Utils
    {
        // see https://stackoverflow.com/a/419058
        public static IEnumerable<IEnumerable<T>> Chunked<T>(this IEnumerable<T> enumerable, int groupSize)
        {
            // The list to return.
            List<T> list = new List<T>(groupSize);

            // Cycle through all of the items.
            foreach (T item in enumerable)
            {
                // Add the item.
                list.Add(item);

                // If the list has the number of elements, return that.
                if (list.Count == groupSize)
                {
                    // Return the list.
                    yield return list;

                    // Set the list to a new list.
                    list = new List<T>(groupSize);
                }
            }

            // Return the remainder if there is any,
            if (list.Count != 0)
            {
                // Return the list.
                yield return list;
            }
        }

        public static string JoinToString<T>(this IEnumerable<T> enumerable, string delimiter = ", ")
        {
            return string.Join(delimiter, enumerable);
        }


        public static void RemoveAll<K, V>(this Dictionary<K, V> dictionary, Func<KeyValuePair<K, V>, bool> filter)
        {
            var toRemove = dictionary.Where(filter)
                .Select(pair => pair.Key)
                .ToList();

            foreach (var key in toRemove)
            {
                dictionary.Remove(key);
            }
        }
    }
}