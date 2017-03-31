namespace ErikEJ.SqlCeScripting
{
    public interface IGenerator
    {
		string GeneratedScript { get; }
		void GenerateForeignKey(Constraint constraint);
        void GenerateForeignKeyDrop(Constraint constraint);
    }
}
