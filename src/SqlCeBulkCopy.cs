using System;
using System.Data;
using System.Data.SqlServerCe;
using System.Globalization;
using System.Collections.Generic;
using System.Collections;
using ErikEJ.SqlCe.ForeignKeyLib;
using Constraint = ErikEJ.SqlCe.ForeignKeyLib.Constraint;
#if PocketPC
#else
using Salient.Data;
#endif
namespace ErikEJ.SqlCe
{
    /// <summary>
    /// Lets you efficiently bulk load a SQL Server Compact table with data from another source.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Ce")]
    public class SqlCeBulkCopy : IDisposable
    {
        private static readonly Type DbNullType = typeof(DBNull);
        private SqlCeBulkCopyColumnMappingCollection mappings = new SqlCeBulkCopyColumnMappingCollection();
        private int _notifyAfter;
        private long _autoIncNext;
        private readonly SqlCeConnection _conn;
        private readonly SqlCeTransaction _trans;
        private readonly bool _ownsConnection;
        private bool _ownsTransaction;
        private bool _keepNulls;
        private bool _keepIdentity;
        private bool _ignoreDuplicateErrors;
        private string _destination;
#if PocketPC
#else
        private List<Constraint> _savedConstraints;
        private bool _disableConstraints;
#endif
        /// <summary>
        /// Initializes a new instance of the SqlCeBulkCopy class using the specified open instance of SqlCeConnection. 
        /// </summary>
        /// <param name="connection"></param>
        public SqlCeBulkCopy(SqlCeConnection connection)
        {
            _conn = connection;
        }
        /// <summary>
        /// Initializes a new instance of the SqlCeBulkCopy class using the specified open instance of SqlCeConnection and the specified active SqlCeTransaction.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        public SqlCeBulkCopy(SqlCeConnection connection, SqlCeTransaction transaction)
        {
            _conn = connection;
            _trans = transaction;
        }

        /// <summary>
        /// Creates a new instance of the SqlCeBulkCopy class, using the specified connection and options
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="copyOptions"></param>
        public SqlCeBulkCopy(SqlCeConnection connection, SqlCeBulkCopyOptions copyOptions)
        {
            _conn = connection;
            SetOptions(copyOptions);
        }

        /// <summary>
        /// Initializes a new instance of the SqlCeBulkCopy class, using the specified connection, transaction and options.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <param name="copyOptions"></param>
        public SqlCeBulkCopy(SqlCeConnection connection, SqlCeBulkCopyOptions copyOptions, SqlCeTransaction transaction)
        {
            _conn = connection;
            _trans = transaction;
            SetOptions(copyOptions);
        }

        /// <summary>
        /// Initializes a new instance of the SqlCeBulkCopy class using the specified connection string.
        /// </summary>
        /// <param name="connectionString"></param>
        public SqlCeBulkCopy(string connectionString)
        {
            _conn = new SqlCeConnection(connectionString);
            _ownsConnection = true;
        }
        /// <summary>
        /// Initializes a new instance of the SqlCeBulkCopy class, using the specified connection string and options
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="copyOptions"></param>
        public SqlCeBulkCopy(string connectionString, SqlCeBulkCopyOptions copyOptions)
        {
            _conn = new SqlCeConnection(connectionString);
            _ownsConnection = true;
            SetOptions(copyOptions);
        }

        /// <summary>
        /// Name of the destination table in the SQL Server Compact database.
        /// </summary>
        public string DestinationTableName
        {
            get
            {
                return _destination;
            }
            set
            {
                _destination = value;
            }
        }
        /// <summary>
        /// Returns a collection of SqlCeBulkCopyColumnMapping items. Column mappings define the relationships between columns in the data source and columns in the destination.
        /// </summary>
        public SqlCeBulkCopyColumnMappingCollection ColumnMappings
        {
            get
            {
                return mappings;
            }
        }
        /// <summary>
        /// The integer value of the BatchSize property, or zero if no value has been set.
        /// In this implementation not used.
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// The integer value of the BulkCopyTimeout property.
        /// In this implementation not used.
        /// </summary>
        public int BulkCopyTimeout { get; set; }

        /// <summary>
        /// Defines the number of rows to be processed before generating a notification event. 
        /// </summary>
        public int NotifyAfter
        {
            get
            {
                return _notifyAfter;
            }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException("value", "Must be > 0");
                }
                _notifyAfter = value;
            }
        }

        /// <summary>
        /// Occurs every time that the number of rows specified by the NotifyAfter property have been processed. 
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Ce")]
        public event EventHandler<SqlCeRowsCopiedEventArgs> RowsCopied;

        /// <summary>
        /// Closes the SqlCeBulkCopy instance. 
        /// </summary>
        public void Close()
        {
            if (_ownsConnection && _conn != null)
            {
                _conn.Dispose();
            }
        }

        /// <summary>
        /// Copies all rows in the supplied DataTable to a destination table specified by the DestinationTableName property of the SqlCeBulkCopy object. 
        /// </summary>
        /// <param name="table"></param>
        public void WriteToServer(DataTable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            WriteToServer(table, 0);
        }
        /// <summary>
        /// Copies only rows that match the supplied row state in the supplied DataTable to a destination table specified by the DestinationTableName property of the SqlCeBulkCopy object. 
        /// </summary>
        /// <param name="table"></param>
        /// <param name="rowState"></param>
        public void WriteToServer(DataTable table, DataRowState rowState)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            WriteToServer(new SqlCeBulkCopyDataTableAdapter(table, rowState));
        }

        /// <summary>
        /// Copies all rows in the supplied IDataReader to a destination table specified by the DestinationTableName property of the SqlBulkCopy object. 
        /// </summary>
        /// <param name="reader"></param>
        public void WriteToServer(IDataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }
            WriteToServer(new SqlCeBulkCopyDataReaderAdapter(reader));				
        }

#if PocketPC
#else
        /// <summary>
        /// Copies all rows in the supplied IEnumerable&lt;> to a destination table specified by the DestinationTableName property of the SqlBulkCopy object.
        /// </summary>
        /// <param name="collection">IEnumerable&lt;>. For IEnumerable use other constructor and specify type.</param>
        public void WriteToServer<T>(IEnumerable<T> collection)
        {
            using (var reader = new EnumerableDataReader(collection))
            {
                WriteToServer(new SqlCeBulkCopyDataReaderAdapter(reader));
            }
        }

        /// <summary>
        /// Copies all rows in the supplied IEnumerable to a destination table specified by the DestinationTableName property of the SqlBulkCopy object.
        /// Use other constructor for IEnumerable&lt;>
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="elementType"></param>
        public void WriteToServer(IEnumerable collection, Type elementType)
        {
            if (collection == null)
            {
                throw new ArgumentNullException("collection");
            }
            using (var reader = new EnumerableDataReader(collection, elementType))
            {
                WriteToServer(new SqlCeBulkCopyDataReaderAdapter(reader));
            }
        }
#endif

        private void SetOptions(SqlCeBulkCopyOptions options)
        {
            _keepNulls = IsCopyOption(SqlCeBulkCopyOptions.KeepNulls, options);
            _keepIdentity = IsCopyOption(SqlCeBulkCopyOptions.KeepIdentity, options);
            _ignoreDuplicateErrors = IsCopyOption(SqlCeBulkCopyOptions.IgnoreDuplicateErrors, options);
#if PocketPC
#else
            _disableConstraints = IsCopyOption(SqlCeBulkCopyOptions.DisableConstraints, options);
#endif
        }

        private void WriteToServer(ISqlCeBulkCopyInsertAdapter adapter)
        {
            CheckDestination();

            if (_conn.State != ConnectionState.Open)
            {
                _conn.Open();
            }

            GetAndDropConstraints();

            List<KeyValuePair<int, int>> map;
            int totalRows = 0;
			SqlCeTransaction localTrans = _trans ?? _conn.BeginTransaction();

            if (ColumnMappings.Count > 0)
            {
                //mapping are set, and should be validated
                map = ColumnMappings.ValidateCollection(_conn, localTrans, adapter, _keepNulls, _destination);
            }
            else
            {
                //create default column mappings
                map = SqlCeBulkCopyColumnMappingCollection.Create(_conn, localTrans, adapter, _keepNulls, _destination);
            }

            using (var cmd = new SqlCeCommand(_destination, _conn, localTrans))
            {
                cmd.CommandType = CommandType.TableDirect;
                using (var rs = cmd.ExecuteResultSet(ResultSetOptions.Updatable))
                {
                    var idOrdinal = SqlCeBulkCopyTableHelpers.IdentityOrdinal(_conn, localTrans, _destination);
                    var rec = rs.CreateRecord();

                    var rowCounter = 0;
                    IdInsertOn(localTrans, idOrdinal);

                    //Converting to an array removed the perf issue of a list and foreach statement.
                    var cm = map.ToArray();

                    while (adapter.Read())
                    {
                        if (adapter.SkipRow())
                            continue;

                        for (var i = 0; i < cm.Length; i++)
                        {
                            //caching the values this way do not cause a perf issue.
                            var sourceIndex = cm[i].Key;
                            var destIndex = cm[i].Value;

                            // Let the destination assign identity values
                            if (!_keepIdentity && destIndex == idOrdinal)
                                continue;

                            //determine if we should ever allow this in the map.
                            if (sourceIndex < 0)
                                continue;

                            var value = sourceIndex > -1 ? adapter.Get(sourceIndex) : null;

                            if (value != null && value.GetType() != DbNullType)
                            {
                                rec.SetValue(destIndex, value);
                            }
                            else
                            {
                                //we can't write to an auto number column so continue
                                if (_keepNulls && destIndex == idOrdinal)
                                    continue;

                                if (_keepNulls)
                                {
                                    rec.SetValue(destIndex, DBNull.Value);
                                }
                                else
                                {
                                    rec.SetDefault(destIndex);
                                }
                            }
                            
                        }
                        rowCounter++;
                        totalRows++;
                        try
                        {
                            rs.Insert(rec);
                        }
                        catch (SqlCeException ex)
                        {
                            if (ex.NativeError == 25016 && _ignoreDuplicateErrors) //A duplicate value cannot be inserted into a unique index.
                            {
                                System.Diagnostics.Trace.TraceWarning("SqlCeBulkCopy: Duplicate value error was ignored");
                                continue;
                            }
                            else
                            {
                                throw;
                            }
                        }
                        // Fire event if needed
                        if (RowsCopied != null && _notifyAfter > 0 && rowCounter == _notifyAfter)
                        {
                            FireRowsCopiedEvent(totalRows);
                            rowCounter = 0;
                        }
                    }
                    IdInsertOff(localTrans, idOrdinal);
                    if (RowsCopied != null)
                    {
                        FireRowsCopiedEvent(totalRows);
                    }
                }
            }

            //if we have our own transaction, we will commit it
            if (_trans == null)
            {
                localTrans.Commit(CommitMode.Immediate);
				localTrans.Dispose();
            }
            ResetSeed(totalRows);
			RestoreConstraints();
        }

        private void CheckDestination()
        {
            if (string.IsNullOrEmpty(_destination))
            {
                throw new ArgumentException("DestinationTable not specified");
            }
            _ownsTransaction = (_trans == null);
        }

        private void GetAndDropConstraints()
        {
#if PocketPC
#else            
            if (_disableConstraints)
            {
                var fkRepo = new ForeignKeyRepository(_conn.ConnectionString, _destination);
                _savedConstraints = fkRepo.GetConstraints();
                fkRepo.DropConstraints();
            }
#endif
        }

        private void RestoreConstraints()
        {
#if PocketPC
#else
            var fkRepo = new ForeignKeyRepository(_conn.ConnectionString, _destination);
            try
            {
                if (_disableConstraints)
                {
                    fkRepo.AddConstraints(_savedConstraints);
                }
            }
            catch (SqlCeException ex)
            { 
                throw new Exception(ex.Message + Environment.NewLine + fkRepo.GetAddConstraintStatements(_savedConstraints), ex);
            }
#endif
        }
        
        private void IdInsertOn(SqlCeTransaction localTrans, int idOrdinal)
        {
            if (_keepIdentity && idOrdinal >= 0)
            {
				using (var idCmd = AdoNetUtils.CreateCommand(_conn, localTrans, string.Format(CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] ON", DestinationTableName)))
                {
                    idCmd.ExecuteNonQuery();
                }
                if (_trans == null)
                {
                    _autoIncNext = SqlCeBulkCopyTableHelpers.GetAutoIncNext(_conn, DestinationTableName);
                }
            }
        }
        

        private void IdInsertOff(SqlCeTransaction localTrans, int idOrdinal)
        {
            if (_keepIdentity && idOrdinal >= 0)
            {
				using (var idCmd = AdoNetUtils.CreateCommand(_conn, localTrans, string.Format(CultureInfo.InvariantCulture, "SET IDENTITY_INSERT [{0}] OFF", DestinationTableName)))
                {
                    idCmd.ExecuteNonQuery();
                }
            }
        }

        private void ResetSeed(int totalRows)
        {
            if (totalRows == 0)
                return;

            if (!_keepIdentity)
                return;

            //Cannot run re-seed when using user supplied transaction, so fail silently
            if (_keepIdentity && _trans != null)
            {
                return;
            }

            var newAutoIncNext = SqlCeBulkCopyTableHelpers.GetAutoIncNext(_conn, DestinationTableName);

            if (_autoIncNext != newAutoIncNext)
                return;
            
            using (var transact = _conn.BeginTransaction())
            {
                // Get Identity column
                string idCol = null;
                using (var ainCmd = AdoNetUtils.CreateCommand(_conn, transact, string.Format(CultureInfo.InvariantCulture,
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}' AND AUTOINC_INCREMENT IS NOT NULL", DestinationTableName)))
                {
                    object res = ainCmd.ExecuteScalar();
                    if (res != null)
                    {
                        idCol = (string)res;
                    }
                }
                if (string.IsNullOrEmpty(idCol))
                    return;

                // Get Max value if the column
                long? maxVal = null;
                using (var ainCmd = AdoNetUtils.CreateCommand(_conn, transact, string.Format(CultureInfo.InvariantCulture,
                    "SELECT CAST(MAX([{0}]) AS bigint) FROM [{1}]", idCol, DestinationTableName)))
                {
                    object res = ainCmd.ExecuteScalar();
                    if (res != null)
                    {
                        maxVal = (long)res;
                    }
                }
                if (!maxVal.HasValue)
                    return;

                //Reseed                    
                using (var ainCmd = AdoNetUtils.CreateCommand(_conn, transact, string.Format(CultureInfo.InvariantCulture,
                    "ALTER TABLE [{0}] ALTER COLUMN [{1}] IDENTITY ({2},1);", DestinationTableName, idCol, maxVal + 1)))
                {
                    ainCmd.ExecuteNonQuery();
                }
                transact.Commit();
            }
        }

        private void OnRowsCopied(SqlCeRowsCopiedEventArgs e)
        {
            if (RowsCopied != null)
            {
                RowsCopied(this, e);
            }
        }

        private void FireRowsCopiedEvent(long rowsCopied)
        {
            SqlCeRowsCopiedEventArgs args = new SqlCeRowsCopiedEventArgs(rowsCopied);
            OnRowsCopied(args);
        }

        private bool IsCopyOption(SqlCeBulkCopyOptions copyOption, SqlCeBulkCopyOptions options)
        {
            return ((options & copyOption) == copyOption);
        }

        #region IDisposable Members

        /// <summary>
        /// Release resources owned by this instance
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_ownsConnection && _conn != null)
                {
                    _conn.Dispose();
                }
                if (_ownsTransaction && _trans != null)
                {
                    _trans.Dispose();
                }
            }
        }
        /// <summary>
        /// Release resources owned by this instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Release resources owned by this instance
        /// </summary>
        ~SqlCeBulkCopy()
        {
            Dispose(false);
        }

        #endregion

    }
}
