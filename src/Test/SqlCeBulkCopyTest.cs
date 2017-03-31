using System;
using System.Diagnostics;
using System.IO;
using NUnit.Framework;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Globalization;
using System.Collections.Generic;
using System.Linq;
using ErikEJ.SqlCe.ForeignKeyLib;

namespace ErikEJ.SqlCe
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1709:IdentifiersShouldBeCasedCorrectly", MessageId = "Ce"), TestFixture]
    public sealed partial class SqlCeBulkCopyFixture
    {
        private enum SchemaType
        {
            NoConstraints,
            FullConstraints,
            FullNoIdentity,
            DataReaderTest,
            DataReaderTestMapped,
            DataReaderTestMappedWithPrimaryKey,
            DataReaderTestMappedKeepOriginal,
            DataReaderTestMappedCollectionKeepOriginal
        }

        private const string connectionString = @"Data Source=.\testdata\bulktest.sdf;Max Database Size=512";

        private const string fileName = @".\testdata\bulktest.sdf";

        private static long rowCount = 0;

        [Test]
        public void ExerciseEngineWithTable()
        {
            //RunDataTableTest(100000, SchemaType.NoConstraints, false);
            //RunDataTableTest(100000, SchemaType.NoConstraints, true);

            //RunDataTableTest(100000, SchemaType.FullConstraints, true);
            //RunDataTableTest(100000, SchemaType.FullConstraints, false);

            //RunDataTableTest(1000000, SchemaType.NoConstraints, false);
            //RunDataTableTest(1000000, SchemaType.NoConstraints, true);

            //RunDataTableTest(1000000, SchemaType.FullConstraints, true);
            //RunDataTableTest(1000000, SchemaType.FullConstraints, false);

            //RunDataTableTest(5000000, SchemaType.NoConstraints, false);
            //RunDataTableTest(5000000, SchemaType.NoConstraints, true);

            //RunDataTableTest(5000000, SchemaType.FullConstraints, true);
            //RunDataTableTest(5000000, SchemaType.FullConstraints, false);

            RunDataTableTest(10000, SchemaType.FullConstraints, true);

            //RunDataTableTestNoKeepId(10000, SchemaType.FullNoIdentity, false);
        }

        [Test]
        public void ExerciseEngineWithSeedReset()
        {
            RunDataTableTest(10000, SchemaType.FullConstraints, true);
            long autoIncNext = 0;
            using (SqlCeConnection conn = new SqlCeConnection(connectionString))
            {
                conn.Open();
                using (SqlCeCommand cmd = new SqlCeCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText = "SELECT AUTOINC_NEXT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'Shippers' AND AUTOINC_INCREMENT IS NOT NULL;";
                    object res = cmd.ExecuteScalar();
                    autoIncNext = (long)res;
                }
            }
            Assert.IsTrue(autoIncNext == 10004);
        }

        [Test]
        public void ExerciseEngineWithTableAndCheckRowCount()
        {
            RunDataTableTestNoKeepId(10009, SchemaType.FullNoIdentity, false);
            Assert.AreEqual(10009, rowCount);
        }

        [Test]
        public void ExerciseEngineWithList()
        {
            RunListTest(100000, SchemaType.NoConstraints, false);
            RunListTest(100000, SchemaType.NoConstraints, true);

            RunListTest(100000, SchemaType.FullConstraints, true);
            RunListTest(100000, SchemaType.FullConstraints, false);

            RunListTest(1000000, SchemaType.NoConstraints, false);
            RunListTest(1000000, SchemaType.NoConstraints, true);

            RunListTest(1000000, SchemaType.FullConstraints, true);
        }

        private void RunDataTableTest(int recordCount, SchemaType schemaType, bool keepNulls)
        {
            CreateDatabase(schemaType);
            using (DataTable testTable = new DataTable("Shippers"))
            {
                testTable.Columns.Add(new DataColumn("ShipperID", typeof(Int32)));
                testTable.Columns.Add(new DataColumn("CompanyName", typeof(System.String)));

                // On better hardware: (x64, SSD, 8 GB RAM)
                // Test 100000 rows =  0.5 / 1 seconds
                // Test 1000000 rows = 7 / 13 seconds

                for (int i = 0; i < recordCount; i++)
                {
                    if (i % 10 == 0 && schemaType != SchemaType.NoConstraints)
                    {
                        testTable.Rows.Add(new object[] { 4 + i, DBNull.Value });
                    }
                    else
                    {
                        testTable.Rows.Add(new object[] { 4 + i, Guid.NewGuid().ToString() });
                    }
                }
                RunBulkCopy(schemaType, keepNulls, testTable);
            }
        }

        private void RunListTest(int recordCount, SchemaType schemaType, bool keepNulls)
        {
            CreateDatabase(schemaType);
            List<Shippers> list = new List<Shippers>();

            // Speed test results without / with constraints
            // Test 100000 rows = 1.5 / 2 seconds
            // Test 1000000 rows = 13 / 19 seconds
            // Test 5000000 rows =  54 / 90 seconds

            // After SqlCeTransacion enabled:
            // Test 100000 rows =  1.5 / 2 seconds
            // Test 1000000 rows = 12 / 16 seconds
            // Test 5000000 rows =  50 / 67 seconds

            // On better hardware: (x64, SSD, 8 GB RAM)
            // Test 100000 rows =  0.5 / 1 seconds
            // Test 1000000 rows = 6 / 11 seconds
            // Test 5000000 rows =  28 / 57  seconds

            for (int i = 0; i < recordCount; i++)
            {
                if (i % 10 == 0 && schemaType != SchemaType.NoConstraints)
                {
                    list.Add( new Shippers { ShipperID = 4 + i, CompanyName = null });
                }
                else
                {
                    list.Add(new Shippers { ShipperID = 4 + i, CompanyName = Guid.NewGuid().ToString() }); 
                }
            }
            RunBulkCopy(schemaType, keepNulls, list);
        }

        internal class Shippers
        {
            public int ShipperID { get; set; }
            public string CompanyName { get; set; }
        }

        private void RunDataTableTestNoKeepId(int recordCount, SchemaType schemaType, bool keepNulls)
        {
            CreateDatabase(schemaType);
            DataTable testTable = new DataTable("Shippers");
            testTable.Columns.Add(new DataColumn("CompanyName", typeof(System.String)));

            for (int i = 0; i < recordCount; i++)
            {
                if (i % 10 == 0)
                {
                    testTable.Rows.Add(new object[] { DBNull.Value });
                }
                else
                {
                    testTable.Rows.Add(new object[] { Guid.NewGuid().ToString() });
                }
            }
            RunBulkCopy(schemaType, keepNulls, testTable);
        }

        private static void RunBulkCopy(SchemaType schemaType, bool keepNulls, List<Shippers> testList)
        {
            SqlCeBulkCopyOptions options = new SqlCeBulkCopyOptions();
            switch (schemaType)
            {
                case SchemaType.FullNoIdentity:
                    break;
                case SchemaType.NoConstraints:
                    break;
                case SchemaType.FullConstraints:
                    options = SqlCeBulkCopyOptions.KeepIdentity;
                    break;
                default:
                    break;
            }
            if (keepNulls)
            {
                options = options |= SqlCeBulkCopyOptions.KeepNulls;
            }

            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();

            using (SqlCeBulkCopy bc = new SqlCeBulkCopy(connectionString, options))
            {
                bc.DestinationTableName = "Shippers";
                bc.WriteToServer(testList);
            }
            sw.Stop();
            System.Diagnostics.Debug.WriteLine(string.Format(CultureInfo.InvariantCulture, "{0} rows copied in {1} ms, Constrained: {2}, Keep Nulls: {3}", testList.Count, sw.ElapsedMilliseconds, schemaType, keepNulls));
        }

        static void bc_RowsCopied(object sender, SqlCeRowsCopiedEventArgs e)
        {
            rowCount = e.RowsCopied;
        }

        private static void RunBulkCopy(SchemaType schemaType, bool keepNulls, DataTable testTable)
        {
            SqlCeBulkCopyOptions options = new SqlCeBulkCopyOptions();
            switch (schemaType)
            {
                case SchemaType.FullNoIdentity:
                    break;
                case SchemaType.NoConstraints:
                    break;
                case SchemaType.FullConstraints:
                    options = SqlCeBulkCopyOptions.KeepIdentity;
                    break;
                default:
                    break;
            }
            if (keepNulls)
            {
                options = options |= SqlCeBulkCopyOptions.KeepNulls;
            }
            
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            
            using (SqlCeBulkCopy bc = new SqlCeBulkCopy(connectionString, options))
            {
                bc.NotifyAfter = 1000;
                bc.RowsCopied += new EventHandler<SqlCeRowsCopiedEventArgs>(bc_RowsCopied);

                bc.DestinationTableName = "Shippers";
                bc.WriteToServer(testTable);
            }
            sw.Stop();
            System.Diagnostics.Debug.WriteLine(string.Format(CultureInfo.InvariantCulture, "{0} rows copied in {1} ms, Constrained: {2}, Keep Nulls: {3}", testTable.Rows.Count, sw.ElapsedMilliseconds, schemaType, keepNulls));
        }

        private static void CreateDatabase(SchemaType schemaType)
        {
            SqlCeEngine engine = new SqlCeEngine(connectionString);
            if (!Directory.Exists("testdata"))
            {
                Directory.CreateDirectory("testdata");
            }
            if (System.IO.File.Exists(fileName))
            {
                System.IO.File.Delete(fileName);
            }
            engine.CreateDatabase();
            using (SqlCeConnection conn = new SqlCeConnection(connectionString))
            {
                conn.Open();
                using (SqlCeCommand cmd = new SqlCeCommand())
                {
                    cmd.Connection = conn;
                    switch (schemaType)
                    {
                        case SchemaType.NoConstraints:
                            cmd.CommandText = "CREATE TABLE [Shippers] ([ShipperID] int NOT NULL, [CompanyName] nvarchar(40) NULL);";
                            cmd.ExecuteNonQuery();

                            break;
                        case SchemaType.FullConstraints:
                            cmd.CommandText = "CREATE TABLE [Shippers] ([ShipperID] int NOT NULL  IDENTITY (1,1), [CompanyName] nvarchar(40) NULL DEFAULT N'ABC');";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "ALTER TABLE [Shippers] ADD PRIMARY KEY ([ShipperID]);";
                            cmd.ExecuteNonQuery();

                            break;
                        case SchemaType.FullNoIdentity:
                            cmd.CommandText = "CREATE TABLE [Shippers] ([ShipperID] int NOT NULL  IDENTITY (1,1), [CompanyName] nvarchar(40) NULL DEFAULT N'ABC');";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "ALTER TABLE [Shippers] ADD PRIMARY KEY ([ShipperID]);";
                            cmd.ExecuteNonQuery();
                            break;

                        case SchemaType.DataReaderTest:
                        case SchemaType.DataReaderTestMapped:
                            cmd.CommandText = "CREATE TABLE [tblDoctor]([DoctorId] [int] NOT NULL, [FirstName] [nvarchar](50) NOT NULL, [MiddleName] [nvarchar](50) NULL, [LastName] [nvarchar](50) NULL, [FullName] [nvarchar](150) NOT NULL, [SpecialityId_FK] [int] NOT NULL, [Active] [bit] NOT NULL, [LastUpdated] [datetime] NOT NULL );";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "ALTER TABLE [tblDoctor] ADD PRIMARY KEY ([DoctorId]);";
                            cmd.ExecuteNonQuery();
                            break;
                        case SchemaType.DataReaderTestMappedKeepOriginal:
                        case SchemaType.DataReaderTestMappedWithPrimaryKey:
                        case SchemaType.DataReaderTestMappedCollectionKeepOriginal:
                            cmd.CommandText = "CREATE TABLE [tblDoctor]([DoctorId] [int] NOT NULL IDENTITY (1,1), [FirstName] [nvarchar](50) NOT NULL, [MiddleName] [nvarchar](50) NULL, [LastName] [nvarchar](50) NULL, [FullName] [nvarchar](150) NOT NULL, [SpecialityId_FK] [int] NOT NULL, [Active] [bit] NOT NULL, [LastUpdated] [datetime] NOT NULL );";
                            cmd.ExecuteNonQuery();

                            cmd.CommandText = "ALTER TABLE [tblDoctor] ADD PRIMARY KEY ([DoctorId]);";
                            cmd.ExecuteNonQuery();
                            break;

                        default:
                            break;
                    }
                }
            }
        }

        [Test]
        public void ExerciseEngineWithDataReader()
        {
            CreateDatabase(SchemaType.DataReaderTest);
            using (SqlConnection conn = new SqlConnection("Data Source=(local);Database=PfTest;Integrated Security=True"))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand("SELECT [DoctorId] ,[FirstName] ,[MiddleName] ,[LastName] ,[FullName] ,[SpecialityId_FK] ,[Active] ,[LastUpdated]  FROM [PfTest].[dbo].[tblDoctor];", conn))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        RunBulkCopy(SchemaType.DataReaderTest, true, reader);
                        Assert.IsNotNull(reader);
                        Assert.IsFalse(reader.IsClosed);
                    }
                }
            }
        }

        [Test]
        public void ExerciseEngineWithDataReaderMocked()
        {
            CreateDatabase(SchemaType.DataReaderTest);

            //Before adapter addition.
            //run with 1,000,000 records.

            //24.8 seconds with constraint
            //16.3 seconds without

            //After
            //23.3 seconds with constraint
            //15.1 seconds without

            //With Mapping
            //23.3 seconds with constraint
            //15.1 seconds without

            RunBulkCopy(SchemaType.DataReaderTest, true, new DoctorDataReader(1000000));
        }

        [Test]
        public void ExerciseEngineWithDataReaderMappedMocked()
        {
            CreateDatabase(SchemaType.DataReaderTest);

            //After
            //23.3 seconds with constraint
            //15.1 seconds without

            RunBulkCopy(SchemaType.DataReaderTestMapped, true, new DoctorDataReaderRemapped(1000000, 0));
        }

        [Test]
        public void ExerciseEngineWithDataReaderMappedMockedWithPrimaryKey()
        {
            CreateDatabase(SchemaType.DataReaderTestMappedWithPrimaryKey);

            RunBulkCopy(SchemaType.DataReaderTestMappedWithPrimaryKey, true, new DoctorDataReaderRemapped(100, 0));
            Assert.IsTrue(RecordExistsInTable(connectionString, "tblDoctor", "DoctorId", 1));
        }

        [Test]
        public void ExerciseEngineWithDataReaderMappedWithCollectionMockedKeepOriginal()
        {
            CreateDatabase(SchemaType.DataReaderTestMappedCollectionKeepOriginal);

            var startNumber = 9430;

            RunBulkCopy(SchemaType.DataReaderTestMappedCollectionKeepOriginal, true, true, new DoctorDataReaderRemapped(100, startNumber));

            //now load up the database and verify 9431 exists
            Assert.IsTrue(RecordExistsInTable(connectionString, "tblDoctor", "DoctorId", startNumber + 1));

        }

        [Test]
        public void ExerciseEngineWithDataReaderMappedMockedKeepOriginal()
        {
            CreateDatabase(SchemaType.DataReaderTestMappedKeepOriginal);

            var startNumber = 9430;

            RunBulkCopy(SchemaType.DataReaderTestMappedKeepOriginal, true, true, new DoctorDataReaderRemapped(100, startNumber));

            //now load up the database and verify 9431 exists
            Assert.IsTrue(RecordExistsInTable(connectionString, "tblDoctor", "DoctorId", startNumber + 1));

        }

        private bool RecordExistsInTable(string connString, string tableName, string columnName, int id)
        {

            using (var conn = new SqlCeConnection(connString))
            {
                conn.Open();
                using (var cmd = new SqlCeCommand(string.Format("SELECT TOP(1) {1} FROM [{0}] Where {1} = {2};", tableName, columnName, id), conn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            return true;
                        }
                    }
                }
            }

            return false;

        }


        private static void RunBulkCopy(SchemaType schemaType, bool keepNulls, IDataReader reader)
        {
            RunBulkCopy(schemaType, keepNulls, false, reader);
        }

        private static void RunBulkCopy(SchemaType schemaType, bool keepNulls, bool keepKey, IDataReader reader)
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
            sw.Start();
            SqlCeBulkCopyOptions options = new SqlCeBulkCopyOptions();
            if (keepNulls)
            {
                options = options |= SqlCeBulkCopyOptions.KeepNulls;
            }
            if (keepKey)
            {
                options = options |= SqlCeBulkCopyOptions.KeepIdentity;
            }
            using (SqlCeBulkCopy bc = new SqlCeBulkCopy(connectionString, options))
            {
                bc.DestinationTableName = "tblDoctor";

                if (schemaType == SchemaType.DataReaderTestMappedCollectionKeepOriginal)
                {
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping("Active", "Active"));
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping("MiddleName", "MiddleName"));
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping("FirstName", "FirstName"));
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping(2, "DoctorId"));
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping(3, 3)); //LastName
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping(4, 4));
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping("SpecialityId_FK", "SpecialityId_FK"));
                    bc.ColumnMappings.Add(new SqlCeBulkCopyColumnMapping("LastUpdated", "LastUpdated"));
                }

                bc.WriteToServer(reader);
            }
            sw.Stop();
            System.Diagnostics.Debug.WriteLine(string.Format(CultureInfo.InvariantCulture, "{0} rows copied in {1} ms, Constrained: {2}, Keep Nulls: {3}", "??", sw.ElapsedMilliseconds, schemaType, keepNulls));
        }

		[Test]
		public void Test_DisableConstraints_Disabled_Timing()
		{
			var connString = GetChinookConnectionString();
			using (var bc = new SqlCeBulkCopy(connString))
			{
				bc.DestinationTableName = "InvoiceLine";
				var sw = new Stopwatch();
				sw.Start();
				bc.WriteToServer(GetTestInvoiceLineTable(connString));
				sw.Stop();
				Debug.WriteLine("With constraints: " + sw.ElapsedMilliseconds);
			}
		}

		[Test]
		public void Test_DisableConstraints_Enabled_Timing()
		{
			var connString = GetChinookConnectionString();
			using (var bc = new SqlCeBulkCopy(connString, SqlCeBulkCopyOptions.DisableConstraints))
			{
				bc.DestinationTableName = "InvoiceLine";
				var sw = new Stopwatch();
				sw.Start();
				bc.WriteToServer(GetTestInvoiceLineTable(connString));
				sw.Stop();
				Debug.WriteLine("Without constraints: " + sw.ElapsedMilliseconds);
				var fkRepo = new ForeignKeyRepository(connString, "InvoiceLine");
				var savedConstraints = fkRepo.GetConstraints();
				Assert.IsTrue(savedConstraints.Count == 2);
			}
		}

		[Test]
		[ExpectedException(typeof(Exception), ExpectedMessage = "A foreign key value cannot be inserted because a corresponding primary key value does not exist", MatchType = MessageMatch.Contains)]
		public void Test_DisableConstraints_Enabled_And_Invalid_Data()
		{
			var connString = GetChinookConnectionString();
			using (var bc = new SqlCeBulkCopy(connString, SqlCeBulkCopyOptions.DisableConstraints))
			{
				bc.DestinationTableName = "InvoiceLine";
				var dt = GetTestInvoiceLineTable(connString);
				dt.Rows[0][1] = Int32.MaxValue - 10000; 
				bc.WriteToServer(dt);
			}
		}

		private string GetChinookConnectionString()
		{
			const string target = @"C:\tmp\SqlCe\Chinook2.sdf";
			if (File.Exists(target))
			{
				File.Delete(target);
			}
			File.Copy(@"C:\tmp\SqlCe\Chinook.sdf", @"C:\tmp\SqlCe\Chinook2.sdf");
			return string.Format("Data Source={0}", target);
		}

		private DataTable GetTestInvoiceLineTable(string connectionString)
		{
			using (var conn = new SqlCeConnection(connectionString))
			{
				var cmd = new SqlCeCommand("SELECT * FROM InvoiceLine", conn);
				conn.Open();
				var dr = cmd.ExecuteReader();
				var dt = new DataTable();
				dt.Load(dr);
				return dt;
			}
		}

        [Test]
        public void TestBinary16()
        {
            var row = new Dictionary<string, object> {
                { "id", generateId() },
                { "name", "test2" },
                { "type", 1 },
                { "email", "qwe@test.com" },
                { "mobile", "5368798797" },
                { "phone", "9879654321" },
                { "city", "London2" },
                { "town", "Town2" },
                { "address", "test street" }
            };

            var testData = new DataTable();
            foreach (var h in row.Keys)
            {
                testData.Columns.Add(h);
            }

            testData.Rows.Add(row.Values.ToArray());

            using (var bc = new SqlCeBulkCopy(String.Format("Data Source = {0}; Flush Interval = 3", "testdb.sdf")))
            {
                bc.DestinationTableName = "customer";
                /*bc.ColumnMappings.Add("id", "id");
                bc.ColumnMappings.Add("name", "name");
                bc.ColumnMappings.Add("type", "type");
                bc.ColumnMappings.Add("email", "email");
                bc.ColumnMappings.Add("mobile", "mobile");
                bc.ColumnMappings.Add("phone", "phone");
                bc.ColumnMappings.Add("city", "city");
                bc.ColumnMappings.Add("address", "address");*/
                bc.WriteToServer(testData);
            }

        }

        private static byte[] generateId()
        {
            var s = Guid.NewGuid().ToString();
            s = s.Substring(14, 4) + s.Substring(9, 4) + s.Substring(0, 8) + s.Substring(19, 4) + s.Substring(24);

            return System.Runtime.Remoting.Metadata.W3cXsd2001.SoapHexBinary.Parse(s.ToString()).Value;
        }




    }

    internal class tblDoctor
    {
        public int DoctorId
        {
            get;
            set;
        }
        public string FirstName
        {
            get;
            set;
        }
        public string MiddleName
        {
            get;
            set;
        }
        public string LastName
        {
            get;
            set;
        }
        public string FullName
        {
            get;
            set;
        }
        public int SpecialityId_FK
        {
            get;
            set;
        }
        public bool Active
        {
            get;
            set;
        }
        public DateTime LastUpdated
        {
            get;
            set;
        }
    }

    #region DoctorDataReader

    internal class DoctorDataReaderRemapped : DoctorDataReader
    {

        int _startId = 0;

        public DoctorDataReaderRemapped(int totalRecords, int startId)
            : base(totalRecords)
        {
            _startId = startId;
        }

        public override string GetName(int i)
        {
            if (i == 0)
            {
                return "FirstName";
                //return "DoctorId";
            }
            else if (i == 1)
            {
                return "MiddleName";
                //return "FirstName";
            }
            else if (i == 2)
            {
                return "DoctorId";
                //return "MiddleName";
            }

            return base.GetName(i);
        }

        public override object this[int i]
        {
            get
            {
                if (i == 0)
                {
                    return Record.FirstName;
                    //return "DoctorId";
                }
                else if (i == 1)
                {
                    return Record.MiddleName;
                    //return "FirstName";
                }
                else if (i == 2)
                {
                    return Count + _startId; //record.DoctorId;
                    //return "MiddleName";
                }
                return base[i];
            }
        }
    }

    /// <summary>
    /// Test class used so that we only have to create one record
    /// </summary>
    internal class DoctorDataReader : IDataReader
    {

        protected int Count = 0;
        protected readonly int _totalRecords = 0;

        protected tblDoctor Record = new tblDoctor()
        {
            Active = true,
            DoctorId = 0,
            FirstName = "Bob",
            LastName = "Cat",
            FullName = "Bob E Cat",
            LastUpdated = DateTime.Now,
            MiddleName = "E",
            SpecialityId_FK = 12
        };

        public DoctorDataReader(int totalRecords)
        {
            _totalRecords = totalRecords;
        }

        #region IDataReader Members

        public void Close()
        {
            //throw new NotImplementedException();
        }

        public int Depth
        {
            get
            {
                return 1;
            }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get
            {
                return false;
            }
        }

        public bool NextResult()
        {
            return false;
            //throw new NotImplementedException();
        }

        public bool Read()
        {
            Count++;
            return Count <= _totalRecords;
        }

        public int RecordsAffected
        {
            get
            {
                return 0; //throw new NotImplementedException();
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            //throw new NotImplementedException();
        }

        #endregion

        #region IDataRecord Members

        public int FieldCount
        {
            get
            {
                return 8;
            }
        }

        public bool GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        public byte GetByte(int i)
        {
            throw new NotImplementedException();
        }

        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        public decimal GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        public double GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        public int GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        public long GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        public virtual string GetName(int i)
        {
            if (i == 0)
            {
                return "DoctorId";
            }
            else if (i == 1)
            {
                return "FirstName";
            }
            else if (i == 2)
            {
                return "MiddleName";
            }
            else if (i == 3)
            {
                return "LastName";
            }
            else if (i == 4)
            {
                return "FullName";
            }
            else if (i == 5)
            {
                return "SpecialityId_FK";
            }
            else if (i == 6)
            {
                return "Active";
            }
            else if (i == 7)
            {
                return "LastUpdated";
            }
            else
            {
                return string.Empty;
            }
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            throw new NotImplementedException();
        }

        public object GetValue(int i)
        {
            throw new NotImplementedException();
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public object this[string name]
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public virtual object this[int i]
        {
            get
            {
                if (i == 0)
                {
                    return Count; //record.DoctorId;
                }
                else if (i == 1)
                {
                    return Record.FirstName;
                }
                else if (i == 2)
                {
                    return Record.MiddleName;
                }
                else if (i == 3)
                {
                    return Record.LastName;
                }
                else if (i == 4)
                {
                    return Record.FullName;
                }
                else if (i == 5)
                {
                    return Record.SpecialityId_FK;
                }
                else if (i == 6)
                {
                    return Record.Active;
                }
                else if (i == 7)
                {
                    return Record.LastUpdated;
                }
                else
                {
                    return null;
                }
            }
        }

        #endregion
    }

    #endregion DoctorDataReader

}
