using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Collections;

namespace ErikEJ.SqlCe
{
    internal class SqlCeBulkCopyDataTableAdapter : ISqlCeBulkCopyInsertAdapter
    {
        private readonly DataTable _table;
        private readonly DataRowState _rowState;
        private int _rowNumber = -1;
        private DataRow _currentRow;

        public int FieldCount
        {
            get
            {
                return _table.Columns.Count;
            }
        }

        public string FieldName(int column)
        {
            var retVal = _table.Columns[column].ColumnName;
            if (retVal.StartsWith("[") && retVal.EndsWith("]"))
            {
                retVal = retVal.Substring(1, retVal.Length - 2);
            }
            return retVal;
        }

        public SqlCeBulkCopyDataTableAdapter(DataTable table, DataRowState rowState)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }
            _table = table;
            _rowState = rowState;
        }

        public object Get(int column)
        {
            //should we throw an out or range exception? this would only happen if Read was not called.
            if (_currentRow == null) {
                return null;
            }
            return _currentRow[column];
        }

        public bool Read()
        {
            if (_rowNumber + 1 < _table.Rows.Count) {
                _currentRow = _table.Rows[++_rowNumber];
                return true;
            }
            return false;
        }

        public bool SkipRow()
        {
            if (_currentRow == null) {
                return false;
            }
            // Never process deleted rows
            if (_currentRow.RowState == DataRowState.Deleted)
                return true;

            // if a specific rowstate is requested
            if (_rowState != 0)
            {
                if (_currentRow.RowState != _rowState)
                    return true;
            }

            return false;
        }

    }
}
