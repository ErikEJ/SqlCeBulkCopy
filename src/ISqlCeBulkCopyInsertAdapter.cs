namespace ErikEJ.SqlCe
{
    /// <summary>
    /// Interface used by the adapters so we are able to remove the duplicate code.
    /// </summary>
    internal interface ISqlCeBulkCopyInsertAdapter
    {

        int FieldCount { get; }

        /// <summary>
        /// The name of the field at the ordinal
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        string FieldName(int column);

        /// <summary>
        /// Get by the column Id (replacement for this[int i]
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        object Get(int column);

        /// <summary>
        /// Move to the next row
        /// </summary>
        /// <returns></returns>
        bool Read();

        /// <summary>
        /// Skip the current row
        /// </summary>
        /// <returns></returns>
        bool SkipRow();

    }
}
