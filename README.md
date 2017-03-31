# SqlCeBulkCopy
.NET Library for loading data fast (doing bulk inserts) into a SQL Server Compact database file. Attempts to mimic the SqlClient SqlBulkCopy API.

**How fast is it?**

Some  timings from testing - load 2 column table with no constraints/indexes:

1.000.000 rows: 6 seconds = 166.666 rows/second
5.000.000 rows: 28 seconds = 178.000 rows/second

**How to get it**

For use with SQL Server Compact 4.0, simply install the [NuGet package](http://nuget.org/packages/ErikEJ.SqlCeBulkCopy)

The library also works with SQL Server Compact 3.5 and .NET Compact Framework [see Releases](https://github.com/ErikEJ/SqlCeBulkCopy/releases)

**How to use it**

Sample usage of the API - the WriteToServer method also accepts a DataTable, an IEnumerable or an IEnumerable<T>

        using ErikEJ.SqlCe;

        private static void DoBulkCopy(bool keepNulls, IDataReader reader)
        {
            SqlCeBulkCopyOptions options = new SqlCeBulkCopyOptions();
            if (keepNulls)
            {
                options = options |= SqlCeBulkCopyOptions.KeepNulls;
            }
            using (SqlCeBulkCopy bc = new SqlCeBulkCopy(connectionString, options))
            {
                bc.DestinationTableName = "tblDoctor";
                bc.WriteToServer(reader);
            }
        }

[Online documentation](https://erikej.github.io/SqlCeBulkCopy/)

[Offline documentation](https://github.com/ErikEJ/SqlCeBulkCopy/blob/master/doc/SqlCeBulkCopyDoc.1.1.zip)

