using System;
using System.Data;
using System.Data.SqlClient;
using System.Xml.Linq;

namespace Bulk1
{
    internal class CopySpecProcessor
    {
        private string filename;

        public CopySpecProcessor(string filename)
        {
            this.filename = filename;
        }

        internal void Process()
        {
            var doc = XDocument.Load(filename);
            foreach (var sqlNode in doc.Root.Elements("Sql"))
            {
                var server = sqlNode?.Attribute("Server")?.Value;
                var db = sqlNode?.Attribute("Db")?.Value;
                var sql = sqlNode.Value;
                SqlNonQuery(server, db, sql);
            }

            foreach (var sqlNode in doc.Root.Elements("BulkCopy"))
            {
                var sourceServer = sqlNode?.Attribute("SourceServer")?.Value;
                var destServer = sqlNode?.Attribute("DestServer")?.Value;
                var sourceDb = sqlNode?.Attribute("SourceDb")?.Value;
                var destDb = sqlNode?.Attribute("DestDb")?.Value;
                var destTable = sqlNode?.Attribute("DestTable")?.Value;
                var sql = sqlNode.Value;
                DoBulkCopy(destServer, destDb, destTable, sourceServer, sourceDb, sql);
            }
        }

        private void DoBulkCopy(string dserver, string ddb, string dtable, string oserver, string odb, string sql)
        {
            var builderDest = new SqlConnectionStringBuilder
            {
                DataSource = dserver,
                InitialCatalog = ddb,
                IntegratedSecurity = true
            };

            var builderSource = new SqlConnectionStringBuilder
            {
                DataSource = oserver,
                InitialCatalog = odb,
                IntegratedSecurity = true
            };

            using (var sourceConnection = new SqlConnection(builderSource.ToString()))
            using (var command = new SqlCommand(sql, sourceConnection))
            {
                sourceConnection.Open();
                using (var sourceReader = command.ExecuteReader())
                using (var destConnection = new SqlConnection(builderDest.ToString()))
                {
                    destConnection.Open();
                    using (SqlBulkCopy bulkCopy =
                           new SqlBulkCopy(destConnection))
                    {
                        bulkCopy.DestinationTableName = dtable;
                        try
                        {
                            bulkCopy.WriteToServer(sourceReader);
                        }
                        catch (Exception ex)
                        {
                            Console.Error.WriteLine(ex.Message);
                        }
                        sourceReader.Close();
                    }
                }
            }
        }

        private static int SqlNonQuery(string server, string dbName, string sql, Action<IDbCommand> action = null)
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = server,
                InitialCatalog = dbName,
                IntegratedSecurity = true
            };

            using (var connection = new SqlConnection(builder.ToString()))
            using (var command = new SqlCommand(sql, connection))
            {
                connection.Open();
                action?.Invoke(command);
                return command.ExecuteNonQuery();
            }
        }

    }
}