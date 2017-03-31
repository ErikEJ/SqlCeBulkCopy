using System;
using System.Collections.Generic;

namespace ErikEJ.SqlCeScripting
{
	public interface IRepository : IDisposable
	{
		List<string> GetAllTableNames();
		List<Constraint> GetAllForeignKeys();
	}
}
