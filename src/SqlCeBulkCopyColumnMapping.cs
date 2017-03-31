using System;
using System.Collections.Generic;
using System.Text;

namespace ErikEJ.SqlCe
{
    /// <summary>
    /// Defines the mapping between a column in a SqlCeBulkCopy instance's data source and a column in the instance's destination table.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Ce")]
    public sealed class SqlCeBulkCopyColumnMapping
    {
        // Fields
        internal string _destinationColumnName;
        internal int _destinationColumnOrdinal;
        internal string _sourceColumnName;
        internal int _sourceColumnOrdinal;

        // Methods
        /// <summary>
        /// Default constructor that initializes a new SqlCeBulkCopyColumnMapping object. 
        /// </summary>
        public SqlCeBulkCopyColumnMapping()
        {
        }
        /// <summary>
        /// Creates a new column mapping, using column ordinals to refer to source and destination columns. 
        /// </summary>
        /// <param name="sourceColumnOrdinal"></param>
        /// <param name="destinationOrdinal"></param>
        public SqlCeBulkCopyColumnMapping(int sourceColumnIndex, int destinationColumnIndex)
        {
            SourceOrdinal = sourceColumnIndex;
            DestinationOrdinal = destinationColumnIndex;
        }

        /// <summary>
        /// Creates a new column mapping, using a column ordinal to refer to the source column and a column name for the target column. 
        /// </summary>
        /// <param name="sourceColumnOrdinal"></param>
        /// <param name="destinationColumn"></param>
        public SqlCeBulkCopyColumnMapping(int sourceColumnIndex, string destinationColumn)
        {
            SourceOrdinal = sourceColumnIndex;
            DestinationColumn = destinationColumn;
        }

        /// <summary>
        /// Creates a new column mapping, using a column name to refer to the source column and a column ordinal for the target column. 
        /// </summary>
        /// <param name="sourceColumn"></param>
        /// <param name="destinationOrdinal"></param>
        public SqlCeBulkCopyColumnMapping(string sourceColumn, int destinationColumnIndex)
        {
            SourceColumn = sourceColumn;
            DestinationOrdinal = destinationColumnIndex;
        }

        /// <summary>
        /// Creates a new column mapping, using column names to refer to source and destination columns. 
        /// </summary>
        /// <param name="sourceColumn"></param>
        /// <param name="destinationColumn"></param>
        public SqlCeBulkCopyColumnMapping(string sourceColumn, string destinationColumn)
        {
            SourceColumn = sourceColumn;
            DestinationColumn = destinationColumn;
        }

        // Properties
        /// <summary>
        /// Name of the column being mapped in the destination database table. 
        /// </summary>
        public string DestinationColumn
        {
            get
            {
                if (_destinationColumnName != null)
                {
                    return _destinationColumnName;
                }
                return string.Empty;
            }
            set
            {
                _destinationColumnOrdinal = -1;
                _destinationColumnName = value;
            }
        }

        /// <summary>
        /// Ordinal value of the destination column within the destination table. 
        /// </summary>
        public int DestinationOrdinal
        {
            get
            {
                return _destinationColumnOrdinal;
            }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException("value", "Must be > 0");
                }
                _destinationColumnName = null;
                _destinationColumnOrdinal = value;
            }
        }

        /// <summary>
        /// Name of the column being mapped in the data source.
        /// </summary>
        public string SourceColumn
        {
            get
            {
                if (_sourceColumnName != null)
                {
                    return _sourceColumnName;
                }
                return string.Empty;
            }
            set
            {
                _sourceColumnOrdinal = -1;
                _sourceColumnName = value;
            }
        }
        /// <summary>
        /// The ordinal position of the source column within the data source. 
        /// </summary>
        public int SourceOrdinal
        {
            get
            {
                return _sourceColumnOrdinal;
            }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException("value", "Must be > 0");
                }
                _sourceColumnName = null;
                _sourceColumnOrdinal = value;
            }
        }
    }

}
