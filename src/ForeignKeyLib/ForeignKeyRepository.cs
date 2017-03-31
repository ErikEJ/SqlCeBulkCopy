using System;
using System.Collections.Generic;
using System.Linq;

namespace ErikEJ.SqlCe.ForeignKeyLib
{
    public class ForeignKeyRepository
    {
        private string _connectionString;
        private string _tableName;

        public ForeignKeyRepository(string connectionString, string tableName)
        {
            _connectionString = connectionString;
            _tableName = tableName;
        }

        protected ForeignKeyRepository() { }

        public List<Constraint> GetConstraints()
        {
            using (var repo = new DbRepository(_connectionString))
            {
                return repo.GetAllForeignKeys().Where(c => c.ConstraintTableName == _tableName).ToList();
            }
        }

        public void DropConstraints()
        {
            using (var repo = new DbRepository(_connectionString))
            {
                var constraints = repo.GetAllForeignKeys().Where(c => c.ConstraintTableName == _tableName).ToArray();
                foreach (var constraint in constraints)
                {
                    var generator = new Generator();
                    generator.GenerateForeignKeyDrop(constraint);
                    repo.RunCommand(generator.GeneratedScript);
                }
            }
        }

        public string GetAddConstraintStatements(List<Constraint> constraints)
        {
            var script = string.Empty;
            foreach (var constraint in constraints)
            {
                var generator = new Generator();
                generator.GenerateForeignKey(constraint);
                script = script + generator.GeneratedScript + Environment.NewLine;
            }
            return script;
        }

        public void AddConstraints(List<Constraint> constraints)
        {
            using (var repo = new DbRepository(_connectionString))
            {
                foreach (var constraint in constraints)
                {
                    var generator = new Generator();
                    generator.GenerateForeignKey(constraint);
                    repo.RunCommand(generator.GeneratedScript);
                }
            }
        }
    }
}
