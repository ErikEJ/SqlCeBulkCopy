using System;

namespace ErikEJ.SqlCe
{
    /// <summary>
    /// Represents the set of arguments passed to the SqlCeRowsCopiedEventHandler.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Ce")]
    public class SqlCeRowsCopiedEventArgs : EventArgs
    {
        private long _rowsCopied;
        private bool _abort;

        /// <summary>
        /// Represents the set of arguments passed to the SqlCeRowsCopiedEventHandler.
        /// </summary>
        /// <param name="rowsCopied"></param>
        public SqlCeRowsCopiedEventArgs(long rowsCopied)
        {
            _rowsCopied = rowsCopied;
        }
        /// <summary>
        /// Gets a value that returns the number of rows copied during the current bulk copy operation. 
        /// </summary>
        public long RowsCopied
        {
            get
            {
                return _rowsCopied;
            }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the bulk copy operation should be aborted. 
        /// </summary>
        public bool Abort
        {
            get
            {
                return _abort;
            }
            set
            {
                _abort = value;
            }
        }
    }
}
