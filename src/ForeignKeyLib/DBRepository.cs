using System.Collections.Generic;
using System.Data.SqlServerCe;

namespace ErikEJ.SqlCe.ForeignKeyLib
{
    internal class DbRepository : IRepository
    {
        private SqlCeConnection _cn;
        private delegate void AddToListDelegate<T>(ref List<T> list, SqlCeDataReader dr);

        /// <summary>
        /// Initializes a new instance of the <see cref="DbRepository"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public DbRepository(string connectionString)
        {
            _cn = new SqlCeConnection(connectionString);
            _cn.Open();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            if (_cn != null)
            {
                _cn.Close();
                _cn = null;
            }
        }

        private static void AddToListString(ref List<string> list, SqlCeDataReader dr)
        {
            list.Add(dr.GetString(0));
        }

        private static void AddToListConstraints(ref List<Constraint> list, SqlCeDataReader dr)
        {
            list.Add(new Constraint
            {
                ConstraintTableName = dr.GetString(0)
                , ConstraintName = dr.GetString(1)
                , ColumnName = dr.GetString(2)
                , UniqueConstraintTableName = dr.GetString(3)
                , UniqueConstraintName = dr.GetString(4)
                , UniqueColumnName = dr.GetString(5)
                , UpdateRule = dr.GetString(6)
                , DeleteRule  = dr.GetString(7)
                , Columns = new ColumnList()
                , UniqueColumns = new ColumnList()
            });
        }

        private List<T> ExecuteReader<T>(string commandText, AddToListDelegate<T> AddToListMethod)
        {
            List<T> list = new List<T>();
            using (var cmd = new SqlCeCommand(commandText, _cn))
            {
                using (var dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                        AddToListMethod(ref list, dr);
                }
            }
            return list;
        }

        #region IRepository Members

        /// <summary>
        /// Gets all table names.
        /// </summary>
        /// <returns></returns>
        public List<string> GetAllTableNames()
        {
            return ExecuteReader(
                "SELECT table_name FROM information_schema.tables WHERE TABLE_TYPE <> N'SYSTEM TABLE' "
                , new AddToListDelegate<string>(AddToListString));
        }

        /// <summary>
        /// Gets all foreign keys.
        /// </summary>
        /// <returns></returns>
        public List<Constraint> GetAllForeignKeys()
        {
            var list = ExecuteReader(
                "SELECT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
                "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
                "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
                "AND KCU1.TABLE_NAME = RC.CONSTRAINT_TABLE_NAME " +
                "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
                "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
                , new AddToListDelegate<Constraint>(AddToListConstraints));

            return RepositoryHelper.GetGroupForeingKeys(list, GetAllTableNames());
        }

        ///// <summary>
        ///// Gets all foreign keys.
        ///// </summary>
        ///// <param name="tableName">Name of the table.</param>
        ///// <returns></returns>
        //public List<Constraint> GetAllForeignKeys(string tableName)
        //{
        //    var list = ExecuteReader(
        //        "SELECT KCU1.TABLE_NAME AS FK_TABLE_NAME,  KCU1.CONSTRAINT_NAME AS FK_CONSTRAINT_NAME, KCU1.COLUMN_NAME AS FK_COLUMN_NAME, " +
        //        "KCU2.TABLE_NAME AS UQ_TABLE_NAME, KCU2.CONSTRAINT_NAME AS UQ_CONSTRAINT_NAME, KCU2.COLUMN_NAME AS UQ_COLUMN_NAME, RC.UPDATE_RULE, RC.DELETE_RULE, KCU2.ORDINAL_POSITION AS UQ_ORDINAL_POSITION, KCU1.ORDINAL_POSITION AS FK_ORDINAL_POSITION " +
        //        "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC " +
        //        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1 ON KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " +
        //        "AND KCU1.TABLE_NAME = RC.CONSTRAINT_TABLE_NAME " +
        //        "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2 ON  KCU2.CONSTRAINT_NAME =  RC.UNIQUE_CONSTRAINT_NAME AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION AND KCU2.TABLE_NAME = RC.UNIQUE_CONSTRAINT_TABLE_NAME " +
        //        "WHERE KCU1.TABLE_NAME = '" + tableName + "' " +
        //        "ORDER BY FK_TABLE_NAME, FK_CONSTRAINT_NAME, FK_ORDINAL_POSITION"
        //        , new AddToListDelegate<Constraint>(AddToListConstraints));
        //    return Helper.GetGroupForeingKeys(list, GetAllTableNames());
        //}

        #endregion

		public void RunCommand(string sql)
		{
			using (SqlCeCommand cmd = new SqlCeCommand())
			{
				cmd.Connection = _cn;
				cmd.CommandText = sql;
				cmd.ExecuteNonQuery();
			}
		}
    }
}
