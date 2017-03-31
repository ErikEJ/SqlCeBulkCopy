using System;
using System.Collections.Generic;

namespace ErikEJ.SqlCe.ForeignKeyLib
{
	public interface IRepository : IDisposable
	{
		List<string> GetAllTableNames();
		List<Constraint> GetAllForeignKeys();
	}
}
