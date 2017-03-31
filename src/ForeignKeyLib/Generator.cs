using System;
using System.Text;

namespace ErikEJ.SqlCe.ForeignKeyLib
{
    /// <summary>
    /// Class for generating scripts
    /// Use the GeneratedScript property to get the resulting script
    /// </summary>
    public class Generator : IGenerator
    {
        private readonly StringBuilder _sbScript;
        public Generator()
        {
			_sbScript = new StringBuilder(10485760);
		}

        public void GenerateForeignKey(Constraint constraint)
        {
            _sbScript.AppendFormat(System.Globalization.CultureInfo.InvariantCulture, "ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ({2}) REFERENCES [{3}]({4}) ON DELETE {5} ON UPDATE {6};{7}"
                , constraint.ConstraintTableName
                , constraint.ConstraintName
                , constraint.Columns.ToString()
                , constraint.UniqueConstraintTableName
                , constraint.UniqueColumns.ToString()
                , constraint.DeleteRule
                , constraint.UpdateRule
                , Environment.NewLine);
        }

        public void GenerateForeignKeyDrop(Constraint constraint)
        {
            _sbScript.Append(string.Format("ALTER TABLE [{0}] DROP CONSTRAINT [{1}];{2}", constraint.ConstraintTableName, constraint.ConstraintName, Environment.NewLine));
        }

		/// <summary>
		/// Gets the generated script.
		/// </summary>
		/// <value>The generated script.</value>
		public string GeneratedScript
		{
			get { return _sbScript.ToString(); }
		}
    }
}
