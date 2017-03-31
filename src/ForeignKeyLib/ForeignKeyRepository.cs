using System;
using System.Collections.Generic;
using System.Text;
using ErikEJ.SqlCeScripting;
using System.Linq;

namespace ErikEJ.SqlCeScripting
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
            using (var repo = new DBRepository(_connectionString))
            {
                return repo.GetAllForeignKeys().Where(c => c.ConstraintTableName == _tableName).ToList();
            }
        }

        public void DropConstraints()
        {
            using (var repo = new DBRepository(_connectionString))
            {
                var constraints = repo.GetAllForeignKeys().Where(c => c.ConstraintTableName == _tableName).ToArray();
                foreach (var constraint in constraints)
                {
                    var generator = new Generator(repo);
                    generator.GenerateForeignKeyDrop(constraint);
                    repo.RunCommand(generator.GeneratedScript);
                }
            }
        }

        public string GetAddConstraintStatements(List<Constraint> constraints)
        {
            var script = string.Empty;
            using (var repo = new DBRepository(_connectionString))
            {
                foreach (var constraint in constraints)
                {
                    var generator = new Generator(repo);
                    generator.GenerateForeignKey(constraint);
                    script = script + generator.GeneratedScript + Environment.NewLine;
                }
            }
            return script;
        }

        public void AddConstraints(List<Constraint> constraints)
        {
            using (var repo = new DBRepository(_connectionString))
            {
                foreach (var constraint in constraints)
                {
                    var generator = new Generator(repo);
                    generator.GenerateForeignKey(constraint);
                    repo.RunCommand(generator.GeneratedScript);
                }
            }
        }
    }
}
