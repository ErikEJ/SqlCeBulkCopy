using System;

namespace ErikEJ.SqlCe
{
    /// <summary>
    /// Bitwise flag that specifies one or more options to use with an instance of SqlCeBulkCopy.
    /// This enumeration has a FlagsAttribute attribute that allows a bitwise combination of its member values.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Ce"), Flags]
    public enum SqlCeBulkCopyOptions
    {
        /// <summary>
        /// No options enabled
        /// </summary>
        Default = 0x0,
        /// <summary>
        /// Preserve source identity values. When not specified, identity values are assigned by the destination.
        /// This is implemented by using 'SET IDENTITY_INSERT [table] ON' when enabled
        /// NOTICE: If you use a SqlCeTransaction in the class constructor, re-seeding of the target table will not take place. 
        /// You can re-seed manually by running Compact on the database, or executing:
        /// ALTER TABLE [MyTable] ALTER COLUMN [MyIdentityCol] IDENTITY (9999,1);
        /// (where 9999 is the value after the highest value in use)
        /// </summary>
        KeepIdentity = 0x1,
        /// <summary>
        /// Removes foreign key constraints while data is being inserted. Will reapply the constraints once insert process is completed.
        /// </summary>
        DisableConstraints = 0x2,
        /// <summary>
        /// Preserve null values in the destination table regardless of the settings for default values. When not specified, null values are replaced by default values where applicable.
        /// </summary>
        KeepNulls = 0x8,
        /// <summary>
        /// Ignores error 25016 - A duplicate value cannot be inserted into a unique index
        /// </summary>
        IgnoreDuplicateErrors = 0x16
    }
}
