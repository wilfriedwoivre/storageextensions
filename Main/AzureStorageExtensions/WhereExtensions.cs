using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExtensions
{
    public static class WhereExtensions
    {
        public static TableQuery<T> Where<T>(this TableQuery<T> query, Expression<Func<T, bool>> predicate) where T : ITableEntity, new()
        {
            if (query == null)
                throw new ArgumentNullException("query");

            //Console.WriteLine(predicate.Body.ToString());
            query.FilterString = predicate.Body.ExpressionTransform();
            return query;
        }

        private static string ExpressionTransform(this Expression expression)
        {
            StringBuilder result = new StringBuilder();
            var candidates = new Queue<Operation>(new[] { new Operation { Expression = expression } });

            while (candidates.Count > 0)
            {
                MemberExpression member = null;
                ConstantExpression constant = null;
                BinaryExpression binary = null;

                var ope = candidates.Dequeue();
                //			Console.WriteLine(ope.Expression);
                if (ope.Expression is BinaryExpression)
                {
                    binary = ope.Expression as BinaryExpression;
                    if (binary.Left is MemberExpression && binary.Right is ConstantExpression)
                    {
                        // like n => n.PartitionKey == "42"
                        member = binary.Left as MemberExpression;
                        constant = binary.Right as ConstantExpression;
                        result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(), constant.FormatConstantExpression()).TrimStart());
                    }
                    else if (binary.Left is ConstantExpression && binary.Right is MemberExpression)
                    {
                        // like n => "42" == n.PartitionKey
                        member = binary.Right as MemberExpression;
                        constant = binary.Left as ConstantExpression;
                        result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(true), constant.FormatConstantExpression()).TrimStart());
                    }
                    else if (binary.Left is BinaryExpression && binary.Right is BinaryExpression)
                    {
                        // like n => n.PartitionKey == "42" && n.RowKey == 42
                        candidates.Enqueue(new Operation() { Expression = binary.Right, TableOperand = binary.NodeType });
                        candidates.Enqueue(new Operation() { Expression = binary.Left });
                    }
                    else if (binary.Left is MemberExpression && binary.Right is MemberExpression)
                    {
                        try
                        {
                            // like n => n.Date = DateTime.Now
                            member = binary.Left as MemberExpression;
                            result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(), binary.Right.EvaluateExpression()).TrimStart());
                        }
                        catch
                        {
                            // like n => DateTime.Now = n.Date
                            member = binary.Right as MemberExpression;
                            result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(true), binary.Left.EvaluateExpression()).TrimStart());
                        }
                    }
                    else if (binary.Left is MemberExpression && binary.Right is NewExpression)
                    {
                        // like n => n.Date = new DateTime(2012, 12, 21);
                        member = binary.Left as MemberExpression;
                        result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(), binary.Right.EvaluateExpression()).TrimStart());
                    }
                    else if (binary.Left is NewExpression && binary.Right is MemberExpression)
                    {
                        // like n => new DateTime(2012, 12, 21) == n.Date;
                        member = binary.Right as MemberExpression;
                        result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(true), binary.Left.EvaluateExpression()).TrimStart());
                    }
                    else if (binary.Left is MemberExpression && binary.Right is MethodCallExpression)
                    {
                        // like n => n.UniqueIdentifier == Guid.Parse("52317684-641D-40C0-86C7-9B57DF97AC7F")
                        member = binary.Left as MemberExpression;
                        result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(), binary.Right.EvaluateExpression()).TrimStart());
                    }
                    else if (binary.Left is MethodCallExpression && binary.Right is MemberExpression)
                    {
                        // like n => n.UniqueIdentifier == Guid.Parse("52317684-641D-40C0-86C7-9B57DF97AC7F")
                        member = binary.Right as MemberExpression;
                        result.Insert(0, string.Format("{0} ({1} {2} {3}) ", ope.TableOperand.GetTableOperand(), member.Member.Name, binary.GetOperator(true), binary.Left.EvaluateExpression()).TrimStart());
                    }
                }
                else if (ope.Expression is MemberExpression)
                {
                    // like n => n.IsEnabled
                    member = ope.Expression as MemberExpression;
                    result.Insert(0, string.Format("{0} ({1} eq true) ", ope.TableOperand.GetTableOperand(), member.Member.Name).TrimStart());
                }
                else if (ope.Expression is UnaryExpression)
                {
                    var unary = ope.Expression as UnaryExpression;
                    // like n => !n.IsEnabled
                    if (unary.Operand is MemberExpression)
                    {
                        member = unary.Operand as MemberExpression;
                        result.Insert(0, string.Format("{0} ({1} eq false) ", ope.TableOperand.GetTableOperand(), member.Member.Name).TrimStart());
                    }
                }
            }

            return result.ToString().Trim();
        }

        private static string EvaluateExpression(this Expression expression)
        {
            var lambda = Expression.Lambda(expression);
            var compiled = lambda.Compile();
            var value = compiled.DynamicInvoke(null);

            if (value is DateTime)
                return string.Format("datetime'{0}'", ((DateTime)value).ToString("yyyy-MM-ddTHH:mm:ss"));
            if (value is Guid)
                return string.Format("guid'{0}'", ((Guid)value).ToString().Replace("-", string.Empty));
            if (value is String)
                return string.Format("'{0}'", value.ToString());

            return string.Empty;
        }

        private static string FormatConstantExpression(this ConstantExpression expression)
        {
            if (expression.Type == typeof(String))
                return string.Format("'{0}'", expression.Value.ToString());
            if (expression.Type == typeof(Int32))
                return expression.Value.ToString();
            if (expression.Type == typeof(Int64))
                return string.Format("{0}L", expression.Value.ToString());
            if (expression.Type == typeof(Boolean))
                return expression.Value.ToString().ToLower();
            if (expression.Type == typeof(Guid))
                return string.Format("guid'{0}'", expression.Value.ToString().Replace("-", string.Empty));
            return string.Empty;
        }

        private static string GetOperator(this Expression expression, bool invert = false)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    return "eq";
                case ExpressionType.NotEqual:
                    return "ne";
                case ExpressionType.GreaterThan:
                    return invert ? "lt" : "gt";
                case ExpressionType.GreaterThanOrEqual:
                    return invert ? "le" : "ge";
                case ExpressionType.LessThan:
                    return invert ? "gt" : "lt";
                case ExpressionType.LessThanOrEqual:
                    return invert ? "ge" : "le";
                default:
                    return string.Empty;
            }
        }

        private static string GetTableOperand(this ExpressionType type)
        {
            switch (type)
            {
                case ExpressionType.AndAlso:
                    return "and";
                case ExpressionType.OrElse:
                    return "or";
                default:
                    return string.Empty;
            }
        }
    }
}

