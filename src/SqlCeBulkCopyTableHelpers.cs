using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlServerCe;
using System.Globalization;

namespace ErikEJ.SqlCe
{
    /// <summary>
    /// Helpers for queries against sql ce for things like schema and auto id columns
    /// </summary>
    internal static class SqlCeBulkCopyTableHelpers
    {
        internal static int IdentityOrdinal(SqlCeConnection conn, SqlCeTransaction transaction, string tableName)
        {
            int ordinal = -1;
            using (var ordCmd = AdoNetUtils.CreateCommand(conn, transaction, string.Format(CultureInfo.InvariantCulture, 
				"SELECT ORDINAL_POSITION FROM information_schema.columns WHERE TABLE_NAME = N'{0}' AND AUTOINC_SEED IS NOT NULL", tableName)))
            {
                object val = ordCmd.ExecuteScalar();
                if (val != null)
                    ordinal = (int)val - 1;
            }
            return ordinal;
        }

        internal static long GetAutoIncNext(SqlCeConnection conn, string destinationTableName)
        {
            using (var ainCmd = AdoNetUtils.CreateCommand(conn, null,
                string.Format(CultureInfo.InvariantCulture,
                "SELECT AUTOINC_NEXT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' AND AUTOINC_INCREMENT IS NOT NULL", destinationTableName)))
            {
                object res = ainCmd.ExecuteScalar();
                if (res != null)
                {
                    return (long)res;
                }
                return 0;
            }
        }

        internal static bool IsCopyOption(SqlCeBulkCopyOptions options, SqlCeBulkCopyOptions copyOption)
        {
            return ((options & copyOption) == options);
        }
    }
}
