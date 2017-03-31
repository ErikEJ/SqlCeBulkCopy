using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace ErikEJ.SqlCe
{
    internal sealed class SqlCeBulkCopyDataReaderAdapter : ISqlCeBulkCopyInsertAdapter
    {
        private readonly IDataReader _reader = null;

        public int FieldCount
        {
            get 
            { 
                return _reader.FieldCount; 
            }
        }

        public string FieldName(int column)
        {
            return _reader.GetName(column);
        }

        public SqlCeBulkCopyDataReaderAdapter(IDataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }
            _reader = reader;
        }

        public object Get(int column)
        {
            return _reader[column];
        }

        public bool Read()
        {
            return _reader.Read();
        }

        public bool SkipRow()
        {
            //we have no way to skip a row
            return false;
        }
    }
}
