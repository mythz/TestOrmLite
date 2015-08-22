using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using ServiceStack;
using ServiceStack.DataAnnotations;
using ServiceStack.Logging;
using ServiceStack.OrmLite;
using ServiceStack.OrmLite.Oracle;
using ServiceStack.Text;

namespace TestOrmLite
{
    public enum Dialect
    {
        Sqlite,
        SqlServer,
        PostgreSql,
        MySql,
        SqlServerMdf,
        Oracle,
        Firebird,
        VistaDb,
    }

    public class IntegrationTest
    {
        public IntegrationTest() : this(Dialect.Sqlite)
        {
            LogManager.LogFactory = new ConsoleLogFactory(debugEnabled: true);
        }

        public IntegrationTest(Dialect dialect)
        {
            Dialect = dialect;
            Init();
        }

        public class Config
        {
            public static string SqliteMemoryDb = ":memory:";
            public static string SqliteFileDir = "~/App_Data/".MapAbsolutePath();
            public static string SqliteFileDb = "~/App_Data/db.sqlite".MapAbsolutePath();
            public static string SqlServerDb = "~/App_Data/Database1.mdf".MapAbsolutePath();

            public static string SqlServerBuildDb =
                "Server={0};Database=test;User Id=test;Password=test;".Fmt(Environment.GetEnvironmentVariable("CI_HOST"));

            //public static string SqlServerBuildDb = "Data Source=localhost;Initial Catalog=TestDb;Integrated Security=SSPI;Connect Timeout=120;MultipleActiveResultSets=True";

            public static string OracleDb = "Data Source=localhost:1521/ormlite;User ID=test;Password=test";
            public static string MySqlDb = "Server=localhost;Database=test;UID=root;Password=test";

            public static string PostgreSqlDb = "Server=localhost;Port=5432;User Id=test;Password=test;Database=test;Pooling=true;MinPoolSize=0;MaxPoolSize=200";

            public static string FirebirdDb = @"User=SYSDBA;Password=masterkey;Database=localhost:C:\src\ServiceStack.OrmLite\tests\ServiceStack.OrmLite.FirebirdTests\App_Data\TEST.FDB;DataSource=localhost;Charset=NONE;Dialect=3;charset=ISO8859_1;MinPoolSize=0;MaxPoolSize=100;";

            public static IOrmLiteDialectProvider DefaultProvider = SqlServerDialect.Provider;
            public static string DefaultConnection = SqlServerBuildDb;

            public static string GetDefaultConnection()
            {
                OrmLiteConfig.DialectProvider = DefaultProvider;
                return DefaultConnection;
            }

            public static IDbConnection OpenDbConnection()
            {
                return GetDefaultConnection().OpenDbConnection();
            }
        }

        public Dialect Dialect;
        protected OrmLiteConnectionFactory DbFactory;
        protected virtual string ConnectionString { get; set; }

        public IDbConnection InMemoryDbConnection { get; set; }

        public virtual IDbConnection OpenDbConnection()
        {
            if (ConnectionString == ":memory:")
            {
                if (InMemoryDbConnection == null || DbFactory.AutoDisposeConnection)
                {
                    InMemoryDbConnection = new OrmLiteConnection(DbFactory);
                    InMemoryDbConnection.Open();
                }
                return InMemoryDbConnection;
            }

            return DbFactory.OpenDbConnection();
        }

        private OrmLiteConnectionFactory Init(string connStr, IOrmLiteDialectProvider dialectProvider)
        {
            ConnectionString = connStr;
            OrmLiteConfig.DialectProvider = dialectProvider;
            DbFactory = new OrmLiteConnectionFactory(ConnectionString, dialectProvider);
            return DbFactory;
        }

        private OrmLiteConnectionFactory Init()
        {
            switch (Dialect)
            {
                case Dialect.Sqlite:
                    var dbFactory = Init(Config.SqliteMemoryDb, SqliteDialect.Provider);
                    dbFactory.AutoDisposeConnection = false;
                    return dbFactory;
                case Dialect.SqlServer:
                    return Init(Config.SqlServerBuildDb, SqlServerDialect.Provider);
                case Dialect.MySql:
                    return Init(Config.MySqlDb, MySqlDialect.Provider);
                case Dialect.PostgreSql:
                    return Init(Config.PostgreSqlDb, PostgreSqlDialect.Provider);
                case Dialect.SqlServerMdf:
                    return Init(Config.SqlServerDb, SqlServerDialect.Provider);
                case Dialect.Firebird:
                    return Init(Config.FirebirdDb, FirebirdDialect.Provider);
                case Dialect.Oracle:
                    return Init(Config.OracleDb, OracleDialect.Provider);
                    //case Dialect.Oracle:
                    //    return Init(Config.OracleDb, OracleDialect.Provider);
                    //case Dialect.VistaDb:
                    //    VistaDbDialect.Provider.UseLibraryFromGac = true;
                    //    var connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["myVDBConnection"];
                    //    var factory = DbProviderFactories.GetFactory(connectionString.ProviderName);
                    //    using (var db = factory.CreateConnection())
                    //    using (var cmd = db.CreateCommand())
                    //    {
                    //        var tmpFile = Path.GetTempPath().CombineWith(Guid.NewGuid().ToString("n") + ".vb5");
                    //        cmd.CommandText = @"CREATE DATABASE '|DataDirectory|{0}', PAGE SIZE 4, LCID 1033, CASE SENSITIVE FALSE;"
                    //            .Fmt(tmpFile);
                    //        cmd.ExecuteNonQuery();
                    //        return Init("Data Source={0};".Fmt(tmpFile), VistaDbDialect.Provider);
                    //    }
            }

            throw new ArgumentException();
        }

        [Test]
        public void Can_insert_update_and_select_AllTypes()
        {
            using (var db = OpenDbConnection())
            {
                db.DropAndCreateTable<AllTypes>();
                db.GetLastSql().Print();

                var rows = 3.Times(i => AllTypes.Create(i));

                db.InsertAll(rows);

                var lastRow = rows.Last();

                var dbRow = db.SingleById<AllTypes>(lastRow.Id);

                Assert.That(dbRow, Is.EqualTo(lastRow));

                Assert.That(db.Single<AllTypes>(x => x.NullableId == lastRow.NullableId), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.Byte == lastRow.Byte), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.Short == lastRow.Short), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.Int == lastRow.Int), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.Long == lastRow.Long), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.UShort == lastRow.UShort), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.UInt == lastRow.UInt), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.ULong == lastRow.ULong), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.Decimal == lastRow.Decimal), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.String == lastRow.String), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.DateTime == lastRow.DateTime), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.TimeSpan == lastRow.TimeSpan), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.DateTimeOffset == lastRow.DateTimeOffset), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.Guid == lastRow.Guid), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.Char == lastRow.Char), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.NullableDateTime == lastRow.NullableDateTime), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.NullableTimeSpan == lastRow.NullableTimeSpan), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.CustomText == lastRow.CustomText), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.MaxText == lastRow.MaxText), Is.EqualTo(lastRow));
                Assert.That(db.Single<AllTypes>(x => x.CustomDecimal == lastRow.CustomDecimal), Is.EqualTo(lastRow));

                Assert.That(
                    db.Single<AllTypes>(q => q.Where(x => x.Bool == lastRow.Bool).OrderByDescending(x => x.Id)),
                    Is.EqualTo(lastRow));

                var updatedRows = 3.Times(i =>
                {
                    var updated = AllTypes.Create(i + 3);
                    updated.Id = i;
                    db.Update(updated);
                    return updated;
                });

                var lastUpdatedRow = updatedRows.Last();
                var dbUpdatedRow = db.SingleById<AllTypes>(lastUpdatedRow.Id);
                Assert.That(dbUpdatedRow, Is.EqualTo(lastUpdatedRow));
            }
        }

        public class Point
        {
            [AutoIncrement]
            public int Id { get; set; }
            public short Width { get; set; }
            public float Height { get; set; }
            public double Top { get; set; }
            public decimal Left { get; set; }
        }

        [Test]
        public void Can_query_using_float_in_alernate_culuture()
        {
            using (var db = OpenDbConnection())
            {
                db.CreateTable<Point>(true);

                db.Insert(new Point { Width = 4, Height = 1.123f, Top = 3.456d, Left = 2.345m });
                db.GetLastSql().Print();

                var sql = Dialect == Dialect.PostgreSql
                    ? "round(cast(Height as numeric),3)={0}"
                    : "round(Height,3)={0}";
                var points = db.SelectFmt<Point>(sql, 1.123);

                Assert.That(points[0].Width, Is.EqualTo(4));
                Assert.That(points[0].Height, Is.EqualTo(1.123f));
                Assert.That(points[0].Top, Is.EqualTo(3.456d));
                Assert.That(points[0].Left, Is.EqualTo(2.345m));
            }
        }

        [Explicit]
        [Test]
        public void Create_Table_with_Composite_indexes()
        {
            using (var db = OpenDbConnection())
            {
                db.DropTable<MyCustomUserAuth>();
                db.DropTable<Company>();
                db.CreateTable<MyCustomUserAuth>();
                db.CreateTable<Company>();
            }
        }

        [Flags]
        public enum FlagsEnum
        {
            FlagOne = 0x0,
            FlagTwo = 0x01,
            FlagThree = 0x02
        }

        public class TypeWithFlagsEnum
        {
            public int Id { get; set; }
            public FlagsEnum Flags { get; set; }
        }

        [Test]
        public void Updates_enum_flags_with_int_value()
        {
            using (var db = OpenDbConnection())
            {
                db.DropAndCreateTable<TypeWithFlagsEnum>();

                db.Insert(new TypeWithFlagsEnum { Id = 1, Flags = FlagsEnum.FlagOne });
                db.Insert(new TypeWithFlagsEnum { Id = 2, Flags = FlagsEnum.FlagTwo });
                db.Insert(new TypeWithFlagsEnum { Id = 3, Flags = FlagsEnum.FlagOne | FlagsEnum.FlagTwo });

                db.Update(new TypeWithFlagsEnum { Id = 1, Flags = FlagsEnum.FlagThree });
                Assert.That(db.GetLastSql(), Is.StringContaining("=@Flags").Or.StringContaining("=:Flags"));
                db.GetLastSql().Print();

                db.UpdateOnly(new TypeWithFlagsEnum { Id = 1, Flags = FlagsEnum.FlagThree }, q => q.Flags);
                db.GetLastSql().Print();

                Assert.That(db.GetLastSql(), Is.StringContaining("=" + (int)FlagsEnum.FlagThree));
                db.GetLastSql().Print();
            }
        }

        public class WithAListOfGuids
        {
            public Guid GuidOne { get; set; }

            public Guid GuidTwo { get; set; }

            public IEnumerable<Guid> TheGuids { get; set; }
        }

        [Test]
        public void Lists_Of_Guids_Are_Formatted_Correctly()
        {
            using (var db = OpenDbConnection())
            {
                db.DropAndCreateTable<WithAListOfGuids>();

                JsConfig<Guid>.RawSerializeFn = x => x.ToString();

                var item = new WithAListOfGuids
                {
                    GuidOne = new Guid("32cb0acb-db43-4061-a6aa-7f4902a7002a"),
                    GuidTwo = new Guid("13083231-b005-4ff4-ab62-41bdc7f50a4d"),
                    TheGuids = new[] { new Guid("18176030-7a1c-4288-82df-a52f71832381"), new Guid("017f986b-f7be-4b6f-b978-ff05fba3b0aa") },
                };

                db.Insert(item);

                var tbl = "WithAListOfGuids".SqlTable();
                var savedGuidOne = db.Select<Guid>("SELECT {0} FROM {1}".Fmt("GuidOne".SqlColumn(), tbl)).First();
                Assert.That(savedGuidOne, Is.EqualTo(new Guid("32cb0acb-db43-4061-a6aa-7f4902a7002a")));

                var savedGuidTwo = db.Select<Guid>("SELECT {0} FROM {1}".Fmt("GuidTwo".SqlColumn(), tbl)).First();
                Assert.That(savedGuidTwo, Is.EqualTo(new Guid("13083231-b005-4ff4-ab62-41bdc7f50a4d")));

                var savedGuidList = db.Select<string>("SELECT {0} FROM {1}".Fmt("TheGuids".SqlColumn(), tbl)).First();
                Assert.That(savedGuidList, Is.EqualTo("[18176030-7a1c-4288-82df-a52f71832381,017f986b-f7be-4b6f-b978-ff05fba3b0aa]"));

                JsConfig.Reset();
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            new IntegrationTest(Dialect.Sqlite).Can_insert_update_and_select_AllTypes();

            Console.ReadLine();
        }
    }
}
