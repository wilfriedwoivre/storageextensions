using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExtensionsTests
{
    public class TestEntity : TableEntity
    {
        public string Text { get; set; }

        public DateTime Date { get; set; }

        public int Integer { get; set; }

        public bool IsEnabled { get; set; }

        public Guid UniqueIdentifier { get; set; }

        public long LongValue { get; set; }
    }

}
