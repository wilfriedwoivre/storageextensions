using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Table;
using AzureStorageExtensions;
using System.Threading;
using System.Diagnostics;

namespace AzureStorageExtensionsTests
{
    [TestClass]
    public class WhereTests
    {
        private static CloudStorageAccount _storageAccount;
        //private static readonly string TableName = Guid.NewGuid().ToString().Replace("-", string.Empty);
        private static readonly string TableName = "a760ef8d529f40969c8a2e5aa2df51b2";
        private static bool TestOnlyFormatQuery = false;
        private int _maxTimeExecution = 10000;

        [ClassInitialize]
        public static void Initialize(TestContext context)
        {
            _storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

            if (!TestOnlyFormatQuery)
            {
                //var tableClient = _storageAccount.CreateCloudTableClient();
                //var table = tableClient.GetTableReference(TableName);
                //table.CreateIfNotExists();

                //InitData();
            }
        }

        private static void InitData()
        {
            var table = _storageAccount.CreateCloudTableClient().GetTableReference(TableName);

            var list = GenerateData.GetDatas().AsParallel();

            list.ForAll(n =>
                            {
                                TableOperation insert = TableOperation.Insert(n);
                                table.Execute(insert);
                            });
        }

        //[ClassCleanup]
        //public static void CleanUp()
        //{
        //    if (!TestOnlyFormatQuery)
        //    {
        //        var tableClient = _storageAccount.CreateCloudTableClient();
        //        tableClient.GetTableReference(TableName).DeleteIfExists(new TableRequestOptions()
        //                                                                    {
        //                                                                        RetryPolicy =
        //                                                                            new ExponentialRetry(
        //                                                                            TimeSpan.FromSeconds(1), 10)
        //                                                                    });
        //    }
        //}

        [TestMethod]
        public void WhereFilterbyPartitionKeyAndRowKey()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.PartitionKey == "42" && n.RowKey == "42");
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42') and (RowKey eq '42')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterByPartitionKeyAndRowKeyInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => "42" == n.PartitionKey && "42" == n.RowKey);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42') and (RowKey eq '42')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterByPartitionKeyAndDifferentRowKey()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.PartitionKey == "42" && n.RowKey != "42");
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42') and (RowKey ne '42')");


            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 99);
            }
        }

        [TestMethod]
        public void WhereFilterByPartitionKeyAndDifferentRowKeyInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => "42" == n.PartitionKey && "42" != n.RowKey);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42') and (RowKey ne '42')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 99);
            }
        }

        [TestMethod]
        public void WhereFilterByPartitionKey()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.PartitionKey == "42");
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 100);
            }
        }

        [TestMethod]
        public void WhereFilterByPartitionKeyInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => "42" == n.PartitionKey);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 100);
            }
        }

        [TestMethod]
        public void WhereFilterWithMultiPartitionKey()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.PartitionKey == "42" || n.PartitionKey == "43");
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42') or (PartitionKey eq '43')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 200);
            }
        }

        [TestMethod]
        public void WhereFilterWithMultiPartitionKeyInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => "42" == n.PartitionKey || "43" == n.PartitionKey);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(PartitionKey eq '42') or (PartitionKey eq '43')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 200);
            }
        }

        [TestMethod]
        public void WhereFilterByIntegerWithTableScan()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.Integer == 21994);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Integer eq 21994)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterByIntegerWithTableScanInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => 21994 == n.Integer);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Integer eq 21994)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterByBooleanWithTableScan()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.IsEnabled == true);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(IsEnabled eq true)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 5003);
            }
        }

        [TestMethod]
        public void WhereFilterByBooleanWithTableScanInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => true == n.IsEnabled);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(IsEnabled eq true)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 5003);
            }
        }

        [TestMethod]
        public void WhereFilterByBooleanMemberWithTrueValue()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.IsEnabled);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(IsEnabled eq true)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 5003);
            }
        }

        [TestMethod]
        public void WhereFilterByBooleanMemberWithFalseValue()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => !n.IsEnabled);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(IsEnabled eq false)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);

                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 4997);
            }
        }

        [TestMethod]
        public void WhereFilterWithNewDateTime()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.Date < new DateTime(2013, 12, 30));
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Date lt datetime'2013-12-30T00:00:00')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 16);
            }
        }

        [TestMethod]
        public void WhereFilterWithNewDateTimeInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => new DateTime(2013, 12, 30) > n.Date);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Date lt datetime'2013-12-30T00:00:00')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 16);
            }
        }

        [TestMethod]
        public void WhereFilterByLongValue()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.LongValue == 85121);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(LongValue eq 85121L)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterByLongValueInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var buildQuery = new TableQuery<TestEntity>().Where(n => 85121 == n.LongValue);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(LongValue eq 85121L)");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterWithDateTime()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var date = new DateTime(2013, 12, 30);
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.Date < date);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Date lt datetime'2013-12-30T00:00:00')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 16);
            }
        }

        [TestMethod]
        public void WhereFilterWithDateTimeInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var date = new DateTime(2013, 12, 30);
            var buildQuery = new TableQuery<TestEntity>().Where(n => date > n.Date);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Date lt datetime'2013-12-30T00:00:00')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 16);
            }
        }


        [TestMethod]
        public void WhereFilterUniqueIdentifier()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var date = new DateTime(2013, 11, 30);
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.UniqueIdentifier == Guid.Parse("187417ee-2a12-4a02-8b2a-ed480af12420"));
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(UniqueIdentifier eq guid'187417ee2a124a028b2aed480af12420')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterUniqueIdentifierInverValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var date = new DateTime(2013, 11, 30);
            var buildQuery = new TableQuery<TestEntity>().Where(n => Guid.Parse("187417ee-2a12-4a02-8b2a-ed480af12420") == n.UniqueIdentifier);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(UniqueIdentifier eq guid'187417ee2a124a028b2aed480af12420')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterStringFormat()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var date = new DateTime(2013, 11, 30);
            var buildQuery = new TableQuery<TestEntity>().Where(n => n.Text == string.Format("Lorem {0} dolor sit amet sit esse vero.", "ipsum"));
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Text eq 'Lorem ipsum dolor sit amet sit esse vero.')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }

        [TestMethod]
        public void WhereFilterStringFormatInvertValues()
        {
            Stopwatch watch = Stopwatch.StartNew();
            var date = new DateTime(2013, 11, 30);
            var buildQuery = new TableQuery<TestEntity>().Where(n => string.Format("Lorem {0} dolor sit amet sit esse vero.", "ipsum") == n.Text);
            watch.Stop();

            Assert.IsTrue(watch.ElapsedMilliseconds < _maxTimeExecution);
            Assert.IsNotNull(buildQuery);
            Assert.IsTrue(buildQuery.FilterString == "(Text eq 'Lorem ipsum dolor sit amet sit esse vero.')");

            if (!TestOnlyFormatQuery)
            {
                var tableClient = _storageAccount.CreateCloudTableClient();
                var table = tableClient.GetTableReference(TableName);

                TableQuery<TestEntity> query = buildQuery;

                var result = table.ExecuteQuery(query);
                Assert.IsNotNull(result);
                Assert.IsTrue(result.Any());
                Assert.IsTrue(result.Count() == 1);
            }
        }
    }
}
