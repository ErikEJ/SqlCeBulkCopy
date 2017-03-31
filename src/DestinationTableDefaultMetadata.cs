using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlServerCe;
using System.Globalization;

namespace ErikEJ.SqlCe
{
	internal sealed class DestinationTableDefaultMetadata
	{

		public string ColumnName
		{
			get;
			set;
		}

		public bool IsNullable
		{
			get;
			set;
		}

		public bool HasDefault
		{
			get;
			set;
		}

		public DestinationTableDefaultMetadata(IDataReader reader)
		{
			ColumnName = (reader.GetString(0) ?? string.Empty).ToUpper(CultureInfo.InvariantCulture);
			IsNullable = reader.GetString(1).Equals("YES", StringComparison.OrdinalIgnoreCase);
			HasDefault = reader.GetBoolean(2);
		}

		public static List<DestinationTableDefaultMetadata> GetDataForTable(SqlCeConnection conn, SqlCeTransaction transaction, string tableName)
		{
			var retVal = new List<DestinationTableDefaultMetadata>();

			using (var ordCmd = AdoNetUtils.CreateCommand(conn, transaction, string.Format(CultureInfo.InvariantCulture,
					"SELECT Column_Name, Is_Nullable, Column_HasDefault FROM information_schema.columns WHERE TABLE_NAME = N'{0}' ORDER BY Ordinal_Position;", tableName)))
			{
				using (var val = ordCmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))
				{
					while (val.Read())
					{
						retVal.Add(new DestinationTableDefaultMetadata(val));
					}
				}
			}

			return retVal;
		}
	}
}
