using System.Linq.Expressions;

namespace AzureStorageExtensions
{
    internal class Operation
    {
        internal Expression Expression { get; set; }
        internal ExpressionType TableOperand { get; set; }
    }
}