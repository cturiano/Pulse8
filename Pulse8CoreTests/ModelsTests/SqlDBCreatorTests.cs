using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using Pulse8Core.Models;

// ReSharper disable ObjectCreationAsStatement

namespace Pulse8CoreTests.ModelsTests
{
    [TestFixture]
    public class SqlDBManagerTests
    {
        #region Setup/Teardown

        [TearDown]
        public async Task TearDown()
        {
           await SqlDBManager.DropDBAsync(DBName);
        }

        #endregion

        private async Task Setup()
        {
            await SqlDBManager.CreateDBAsync(DBName, _sqlFile);
        }

        private const string DBName = "Pulse8TestDB";
        private readonly string _sqlFile = Path.Combine(AppDomain.CurrentDomain.SetupInformation.ApplicationBase, "..", "..", "..", "Pulse8Core", "Pulse8TestDBSetup.sql");

        [Test]
        public async Task ConstructorAndPropertiesTests()
        {
            try
            {
                new SqlDBManager(null, _sqlFile);
                Assert.Fail("Should have thrown");
            }
            catch (ArgumentNullException)
            {
            }

            try
            {
                new SqlDBManager(DBName, null);
                Assert.Fail("Should have thrown");
            }
            catch (ArgumentNullException)
            {
            }

            try
            {
                new SqlDBManager(DBName, "blah");
                Assert.Fail("Should have thrown");
            }
            catch (ArgumentException)
            {
            }

            var sdc = new SqlDBManager(DBName, _sqlFile);
            Assert.IsNotNull(sdc);
            Assert.IsTrue(await SqlDBManager.DBExistsAsync(DBName));
        }

        [Test]
        public async Task CreateDropDBTest()
        {
            Assert.IsFalse(await SqlDBManager.DBExistsAsync(DBName));
            await SqlDBManager.CreateDBAsync(DBName, _sqlFile);
            Assert.IsTrue(await SqlDBManager.DBExistsAsync(DBName));
            await SqlDBManager.DropDBAsync(DBName);  
            Assert.IsFalse(await SqlDBManager.DBExistsAsync(DBName));
        }

        [Test]
        public async Task DBExistsTest()
        {
            Assert.IsFalse(await SqlDBManager.DBExistsAsync(DBName));
            await Setup();
            Assert.IsTrue(await SqlDBManager.DBExistsAsync(DBName));
        }
        
        [Test]
        public async Task ExecuteNonQueryTest()
        {                                                                                                                                                                       
            await Setup();
            Assert.AreEqual(5, await SqlDBManager.ExecuteScalarAsync("SELECT COUNT(*) FROM Diagnosis;"));
            Assert.AreEqual(1, await SqlDBManager.ExecuteNonQueryAsync("INSERT INTO Diagnosis ( DiagnosisID , DiagnosisDescription )VALUES  ( 6 , 'Test Diagnosis 6' );"));
            Assert.AreEqual(6, await SqlDBManager.ExecuteScalarAsync("SELECT COUNT(*) FROM Diagnosis;"));
        }
        
        [Test]
        public async Task ExecuteScalarTest()
        {                                                                                                                                                                       
            await Setup();
            Assert.AreEqual(3, await SqlDBManager.ExecuteScalarAsync("SELECT COUNT(*) FROM DiagnosisCategory;"));
        }
        
        [Test]
        public async Task ExecuteReaderTest()
        {                                                                                                                                                                       
            await Setup();
            var table = await SqlDBManager.ExecuteReaderAsync("SELECT * FROM DiagnosisCategory;");
            Assert.IsNotNull(table);
            Assert.AreEqual(3, table.Rows.Count);
        }

        [TestCase("SELECT * FROM Member WHERE FirstName='John';", "1", "John", "Smith")]
        [TestCase("SELECT * FROM DiagnosisCategoryMap WHERE DiagnosisID='1';", "1", "1")]
        [TestCase("SELECT * FROM DiagnosisCategory WHERE CategoryScore='10';", "1", "Category A" ,"10")]
        [TestCase("SELECT * FROM Diagnosis WHERE DiagnosisID='1';", "1", "Test Diagnosis 1")]
        [TestCase("SELECT * FROM MemberDiagnosis WHERE DiagnosisID='2';", "1", "2")]
        [TestCase("SELECT * FROM Member WHERE MemberID = (SELECT MemberID FROM MemberDiagnosis WHERE DiagnosisID = 3);", "3", "Will", "Smyth")]
        [TestCase("SELECT * FROM Member WHERE NOT EXISTS (SELECT MemberID FROM MemberDiagnosis WHERE Member.MemberID = MemberDiagnosis.MemberID);", "2", "Jack", "Smith")]
        public async Task MiscQueryTest(string query, params string[] expected)
        {
            await Setup();
            var result = await SqlDBManager.ExecuteReaderAsync(query);

            for (var i = 0; i < expected.Length; i++)
            {
                Assert.AreEqual(expected[i], result.Rows[0][i].ToString());
            }
        }
    }
}