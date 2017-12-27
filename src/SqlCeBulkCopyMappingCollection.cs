using System;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.Data.SqlServerCe;
using System.Globalization;

namespace ErikEJ.SqlCe
{
    /// <summary>
    /// Collection of SqlCeBulkCopyColumnMapping objects 
    /// </summary>
    public sealed class SqlCeBulkCopyColumnMappingCollection : Collection<SqlCeBulkCopyColumnMapping>
    {
        #region Fields

        #endregion

        /// <summary>
        /// Add a new ColumnMapping
        /// </summary>
        /// <param name="sourceColumn"></param>
        /// <param name="destinationColumn"></param>
        public void Add(string sourceColumn, string destinationColumn)
        {
            base.Add(new SqlCeBulkCopyColumnMapping(sourceColumn, destinationColumn));
        }

        /// <summary>
        /// Add a new ColumnMapping
        /// </summary>
        /// <param name="sourceColumnIndex"></param>
        /// <param name="destinationColumnIndex"></param>
        public void Add(int sourceColumnIndex, int destinationColumnIndex)
        {
            base.Add(new SqlCeBulkCopyColumnMapping(sourceColumnIndex, destinationColumnIndex));
        }

        /// <summary>
        /// Add a new ColumnMapping
        /// </summary>
        /// <param name="sourceColumnIndex"></param>
        /// <param name="destinationColumn"></param>
        public void Add(int sourceColumnIndex, string destinationColumn)
        { 
            base.Add(new SqlCeBulkCopyColumnMapping(sourceColumnIndex, destinationColumn));
        }

        /// <summary>
        /// Add a new ColumnMapping
        /// </summary>
        /// <param name="sourceColumn"></param>
        /// <param name="destinationColumnIndex"></param>
        public void Add(string sourceColumn, int destinationColumnIndex)
        {
            base.Add(new SqlCeBulkCopyColumnMapping(sourceColumn, destinationColumnIndex));
        }


        #region Properties

        internal int IdentityOrdinal
        {
            get;
            private set;
        }

        #endregion Properties

        #region constructors

        internal SqlCeBulkCopyColumnMappingCollection()
        {

        }

        #endregion

        #region other methods

        internal static List<KeyValuePair<int, int>> Create(SqlCeConnection conn, SqlCeTransaction transaction, ISqlCeBulkCopyInsertAdapter adapter, bool keepNulls, string tableName)
        {
            var retVal = new List<KeyValuePair<int, int>>();
            //we use this to determine if we throw an error while building maps.
            int idOrdinal = SqlCeBulkCopyTableHelpers.IdentityOrdinal(conn, transaction, tableName);

            var destColumnData = DestinationTableDefaultMetadata.GetDataForTable(conn, transaction, tableName);

            //we are going to validate all of the columns but if we don't map then we will not set the HasMappings
            //A map is defined as
            //1. any column that the column order is changed
            //2. field exists in the dest but not the source and the dest has a default.
            //3. Identity column that is not 0 ?? we may be able to remove this one.

            //we only really care about the destination columns being mapped. If too many columns exist do we really care????

            var sourceColumns = GetSourceColumns(adapter);

            for (int destIndex = 0; destIndex < destColumnData.Count; destIndex++)
            {
                var destColumn = destColumnData[destIndex];
                var sourceIndex = sourceColumns.IndexOf(destColumn.ColumnName);
                //see if the source is the same as the destination ordinal
                //if (destIndex != sourceIndex) //we have a map if we care later

                //If the source index is -1 and the dest does not allow nulls or has a default, it is an error

                if (sourceIndex < 0)
                {
                    //either we allow nulls or the ordinal is the index and therefore it is valid
                    if (!destColumnData[destIndex].HasDefault && ((!destColumnData[destIndex].IsNullable && !keepNulls) && idOrdinal != destIndex))
                    {
                        //var error = destColumnData[destIndex].HasDefault + " " + destColumnData[destIndex].IsNullable + " " + keepNulls + " " + idOrdinal + " " + destIndex;
                        throw new InvalidOperationException(string.Format("Source column '{0}' does not exist and destination does not allow nulls.", destColumn.ColumnName));
                    }
                }   

                retVal.Add(new KeyValuePair<int, int>(sourceIndex, destIndex));
            }

            return retVal;
        }

        internal List<KeyValuePair<int, int>> ValidateCollection(SqlCeConnection conn, SqlCeTransaction transaction, ISqlCeBulkCopyInsertAdapter adapter, bool keepNulls, string tableName)
        {
            if (Count > 0)
            {
                var retVal = new List<KeyValuePair<int, int>>();
                var sourceColumns = GetSourceColumns(adapter);
                var destColumns = ToColumnNames(DestinationTableDefaultMetadata.GetDataForTable(conn, transaction, tableName));

                foreach (SqlCeBulkCopyColumnMapping mapping in Items)
                {
                    var sourceColumnName = (mapping.SourceColumn ?? string.Empty).ToUpper(CultureInfo.InvariantCulture);
                    var destColumnName = (mapping.DestinationColumn ?? string.Empty).ToUpper(CultureInfo.InvariantCulture);
                    int sourceIndex;
                    int destIndex;

                    //verify if we have a source column name that it exists
                    if (!string.IsNullOrEmpty(sourceColumnName))
                    {
                        if (!sourceColumns.Contains(sourceColumnName))
                        {
                            throw new ApplicationException("No column exists with the name of " + mapping.SourceColumn + " in source."); //use collection name for error since it has original casing.
                        }
                        sourceIndex = sourceColumns.IndexOf(sourceColumnName);
                    }
                    else
                    {
                        if (mapping.SourceOrdinal < 0 || mapping.SourceOrdinal >= destColumns.Count)
                            throw new ApplicationException("No column exists at index " + mapping.SourceOrdinal + " in source."); //use collection name for error since it has original casing.

                        sourceIndex = mapping.SourceOrdinal;
                    }

                    if (!string.IsNullOrEmpty(destColumnName))
                    {
                        if (destColumnName.StartsWith("[") && destColumnName.EndsWith("]"))
                        {
                            destColumnName = destColumnName.Substring(1, destColumnName.Length - 2);
                        }

                        if (!destColumns.Contains(destColumnName))
                        {
                            string bestFit = null;

                            foreach (var existingColumn in destColumns)
                            {
                                if (String.Equals(existingColumn, destColumnName, StringComparison.OrdinalIgnoreCase))
                                    bestFit = existingColumn;
                            }

                            if (bestFit == null)
                                throw new ApplicationException("Destination column " + mapping.DestinationColumn + " does not exist in destination table " +
                                                                tableName + " in database " + conn.Database + "."); //use collection name for error since it has original casing.
                            else
                                throw new ApplicationException(
                                    "Destination column " + mapping.DestinationColumn + " does not exist in destination table " + tableName
                                        + " in database " + conn.Database + "." +
                                    " Best found match is " + bestFit + "."); //use collection name for error since it has original casing.
                        }
                        else
                        {
                            destIndex = destColumns.IndexOf(destColumnName);
                        }
                    }
                    else
                    {
                        if (mapping.DestinationOrdinal < 0 || mapping.DestinationOrdinal >= destColumns.Count)
                            throw new ApplicationException(
                                "No column exists at index " + mapping.DestinationOrdinal + " in destination table " + tableName +
                                                            "in database " + conn.Database + "."); //use collection name for error since it has original casing.
                        destIndex = mapping.DestinationOrdinal;
                    }
                    retVal.Add(new KeyValuePair<int, int>(sourceIndex, destIndex));
                }

                retVal.Sort((a, b) =>
                {
                    return a.Key.CompareTo(b.Key);
                });

                return retVal;
            }
            else
            {
                return Create(conn, transaction, adapter, keepNulls, tableName);
            }
        }

        private static List<string> GetSourceColumns(ISqlCeBulkCopyInsertAdapter adapter)
        {
            var sourceColumns = new List<string>();

            for (int fieldCount = 0; fieldCount < adapter.FieldCount; fieldCount++)
            {
                sourceColumns.Add(adapter.FieldName(fieldCount).ToUpper(CultureInfo.InvariantCulture));
            }
            return sourceColumns;
        }

        private static List<string> ToColumnNames(List<DestinationTableDefaultMetadata> meta)
        {
            var retVal = new List<string>();
            foreach (var item in meta)
            {
                retVal.Add(item.ColumnName);
            }
            return retVal;
        }

        #endregion
    }
}


