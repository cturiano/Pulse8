using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Pulse8Core.Properties;

namespace Pulse8Core.Models
{
    public class SqlDBManager
    {
        #region Static Fields and Constants

        private static string _dbName;
        private static string _sqlFilePath;

        #endregion

        #region Constructors

        public SqlDBManager(string dbName, string sqlFilePath)
        {
            if (string.IsNullOrEmpty(sqlFilePath))
            {
                throw new ArgumentNullException(nameof(sqlFilePath), Resources.NoSqlFilePathMessage);
            }

            if (string.IsNullOrEmpty(dbName))
            {
                throw new ArgumentNullException(nameof(dbName), Resources.NoDBNameMessage);
            }

            if (!File.Exists(sqlFilePath))
            {
                throw new ArgumentException(nameof(sqlFilePath), string.Format(Resources.SqlFileDoesNotExistMessage, sqlFilePath));
            }

            _dbName = dbName;
            _sqlFilePath = sqlFilePath;

            if (!CreateDBAsync().Result)
            {
                throw new Exception(Resources.CouldNotCreateDBMessage);
            }
        }

        #endregion

        #region Public Methods

        public async Task<bool> CreateDBAsync()
        {
            if (string.IsNullOrEmpty(_dbName) || string.IsNullOrEmpty(_sqlFilePath))
            {
                return false;
            }

            return await CreateDBAsync(_dbName, _sqlFilePath);
        }

        public static async Task<bool> CreateDBAsync(string dbName, string sqlFilePath)
        {
            var retVal = false;
            if (!await DBExistsAsync(dbName))
            {
                using (var connection = new SqlConnection(Resources.SqlConnectionString))
                {
                    try
                    {
                        var sql = CleanScript(sqlFilePath);
                        if (!string.IsNullOrEmpty(sql))
                        {
                            var sqls = sql.Split(';');
                            var cmd = new SqlCommand(sqls[0], connection);
                            
                            await connection.OpenAsync().ConfigureAwait(false);
                            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                            
                            foreach (var s in sqls.Skip(1).Where(s => !string.IsNullOrEmpty(s)))
                            {
                                cmd.CommandText = s;
                                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                            }

                            retVal = true;
                        }
                    }
                    catch (DbException de)
                    {
                        // log the exception
                    }
                    catch (ArgumentNullException ane)
                    {
                        // log the exception
                    }
                }
            }
            else
            {
                retVal = true;
            }

            return retVal;
        }

        public static async Task<bool> DBExistsAsync(string dbName)
        {
            using (var connection = new SqlConnection(Resources.SqlConnectionString))
            {
                using (var command = new SqlCommand($"SELECT db_id('{dbName}')", connection))
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                    return await command.ExecuteScalarAsync().ConfigureAwait(false) != DBNull.Value;
                }
            }
        }

        public static async Task DropDBAsync(string dbName)
        {
            if (await DBExistsAsync(dbName))
            {
                using (var connection = new SqlConnection(Resources.SqlConnectionString))
                {
                    SqlConnection.ClearAllPools();
                    await connection.OpenAsync().ConfigureAwait(false);
                    using (var cmd = new SqlCommand($"ALTER DATABASE {dbName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;", connection) {CommandType = CommandType.Text})
                    {
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    using (var cmd = new SqlCommand($"IF EXISTS (SELECT name FROM sys.databases WHERE name = N'{dbName}') DROP DATABASE {dbName};", connection))
                    {                                                  
                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }
            }
        }

        public static async Task<int> ExecuteNonQueryAsync(string query)
        {
            using (var connection = new SqlConnection(Resources.SqlConnectionString2))
            {
                using (var command = new SqlCommand(query, connection))
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                    return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        public static async Task<DataTable> ExecuteReaderAsync(string query)
        {
            var dt = new DataTable();
            using (var connection = new SqlConnection(Resources.SqlConnectionString2))
            {
                using (var command = new SqlCommand(query, connection))
                {                    
                    await connection.OpenAsync().ConfigureAwait(false);
                    dt.Load(await command.ExecuteReaderAsync().ConfigureAwait(false));
                }
            }

            return dt;
        } 
        
        public static async Task<object> ExecuteScalarAsync(string query)
        {
            using (var connection = new SqlConnection(Resources.SqlConnectionString2))
            {
                using (var command = new SqlCommand(query, connection))
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                    return await command.ExecuteScalarAsync().ConfigureAwait(false);
                }
            }
        } 

        #endregion

        #region Private Methods

        private static string CleanScript(string sqlFilePath)
        {
            try
            {
                var sql = File.ReadAllText(sqlFilePath);
                if (!string.IsNullOrEmpty(sql))
                {
                    return Regex.Replace(sql, "\r\nGO\r\n", ";").Replace("\r\n", string.Empty).Replace("\t", string.Empty);
                }
            }
            catch (Exception e)
            {
                // log the exception
            }

            return string.Empty;
        }

        #endregion
    }
}