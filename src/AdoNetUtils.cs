using System.Collections.Generic;
using System.Data;

namespace ErikEJ.SqlCe
{
	internal static class AdoNetUtils
	{
		internal static IDbCommand CreateCommand(IDbConnection cn, IDbTransaction transaction, string sql, params KeyValuePair<string, object>[] parameters)
		{
			var cmd = cn.CreateCommand();
			cmd.CommandText = sql;
			cmd.CommandType = CommandType.Text;

			AddParameters(cmd, parameters);

			if (transaction != null)
				cmd.Transaction = transaction;

			return cmd;
		}

		private static void AddParameters(IDbCommand cmd, params KeyValuePair<string, object>[] parameters)
		{
			foreach (var dacParam in parameters)
			{
				var parameter = cmd.CreateParameter();
				parameter.ParameterName = dacParam.Key;
				parameter.Value = dacParam.Value;

				cmd.Parameters.Add(parameter);
			}
		}
	}
}