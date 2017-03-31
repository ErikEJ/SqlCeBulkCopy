using System.Collections.Generic;
using System.Linq;

namespace ErikEJ.SqlCeScripting
{
    public class RepositoryHelper
    {
        // Contrib from hugo on CodePlex - thanks!
        public static List<Constraint> GetGroupForeingKeys(List<Constraint> foreignKeys, List<string> allTables)
        {
            var groupedForeingKeys = new List<Constraint>();

            var uniqueTables = (from c in foreignKeys
                                select c.ConstraintTableName).Distinct().ToList();
            int i = 1;
            foreach (string tableName in uniqueTables)
            {
                {
                    var uniqueConstraints = (from c in foreignKeys
                                             where c.ConstraintTableName == tableName
                                             select c.ConstraintName).Distinct().ToList();
                    foreach (string item in uniqueConstraints)
                    {
                        string value = item;
                        var constraints = foreignKeys.Where(c => c.ConstraintName.Equals(value, System.StringComparison.Ordinal) && c.ConstraintTableName == tableName).ToList();

                        if (constraints.Count == 1)
                        {
                            Constraint constraint = constraints[0];
                            constraint.Columns.Add(constraint.ColumnName);
                            constraint.UniqueColumns.Add(constraint.UniqueColumnName);
                            var found = groupedForeingKeys.Any(fk => fk.ConstraintName == constraint.ConstraintName && fk.ConstraintTableName != constraint.ConstraintTableName);
                            if (found)
                            {
                                constraint.ConstraintName = constraint.ConstraintName + i.ToString();
                                i++;
                            }
                            else
                            {
                                var tfound = allTables.Any(ut => ut == constraint.ConstraintName);
                                if (tfound)
                                {
                                    constraint.ConstraintName = constraint.ConstraintName + i.ToString();
                                    i++;
                                }
                            }

                            groupedForeingKeys.Add(constraint);
                        }
                        else
                        {
                            var newConstraint = new Constraint { ConstraintTableName = constraints[0].ConstraintTableName, ConstraintName = constraints[0].ConstraintName, UniqueConstraintTableName = constraints[0].UniqueConstraintTableName, UniqueConstraintName = constraints[0].UniqueConstraintName, DeleteRule = constraints[0].DeleteRule, UpdateRule = constraints[0].UpdateRule, Columns = new ColumnList(), UniqueColumns = new ColumnList() };
                            foreach (Constraint c in constraints)
                            {
                                newConstraint.Columns.Add(c.ColumnName);
                                newConstraint.UniqueColumns.Add(c.UniqueColumnName);
                            }
                            var found = groupedForeingKeys.Any(fk => fk.ConstraintName == newConstraint.ConstraintName && fk.ConstraintTableName != newConstraint.ConstraintTableName);
                            if (found)
                            {
                                newConstraint.ConstraintName = newConstraint.ConstraintName + i.ToString();
                                i++;
                            }
                            groupedForeingKeys.Add(newConstraint);
                        }
                    }
                }
            }
            return groupedForeingKeys;
        }
    }
}
