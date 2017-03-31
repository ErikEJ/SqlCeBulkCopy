namespace ErikEJ.SqlCe.ForeignKeyLib
{
    public interface IGenerator
    {
		string GeneratedScript { get; }
		void GenerateForeignKey(Constraint constraint);
        void GenerateForeignKeyDrop(Constraint constraint);
    }
}
