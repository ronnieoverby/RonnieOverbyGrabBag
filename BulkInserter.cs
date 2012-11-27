/* 
 * Written by Ronnie Overby
 * and part of the Ronnie Overby Grab Bag: https://github.com/ronnieoverby/RonnieOverbyGrabBag
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Fasterflect; // http://nuget.org/packages/fasterflect -- PM> Install-Package fasterflect

// ReSharper disable CheckNamespace
namespace Overby.Data
// ReSharper restore CheckNamespace
{
    public class BulkInsertEventArgs<T> : EventArgs
    {
        public BulkInsertEventArgs(IEnumerable<T> items)
        {
            if (items == null) throw new ArgumentNullException("items");
            Items = items.ToArray();
        }

        public T[] Items { get; private set; }
    }

    /// <summary>
    /// Performs buffered bulk inserts into a sql server table using objects instead of DataRows. :)
    /// </summary>
    public class BulkInserter<T> where T : class
    {
        public event EventHandler<BulkInsertEventArgs<T>> PreBulkInsert;
        public void OnPreBulkInsert(BulkInsertEventArgs<T> e)
        {
            var handler = PreBulkInsert;
            if (handler != null) handler(this, e);
        }

        public event EventHandler<BulkInsertEventArgs<T>> PostBulkInsert;
        public void OnPostBulkInsert(BulkInsertEventArgs<T> e)
        {
            var handler = PostBulkInsert;
            if (handler != null) handler(this, e);
        }

        private const int DefaultBufferSize = 2000;
        private readonly SqlConnection _connection;
        private readonly int _bufferSize;
        public int BufferSize { get { return _bufferSize; } }
        public int InsertedCount { get; private set; }

        private readonly Lazy<Dictionary<string, MemberGetter>> _props =
            new Lazy<Dictionary<string, MemberGetter>>(GetPropertyInformation);

        private readonly Lazy<DataTable> _dt;

        private readonly SqlBulkCopy _sbc;
        private readonly List<T> _queue = new List<T>();

        /// <param name="connection">SqlConnection to use for retrieving the schema of sqlBulkCopy.DestinationTableName</param>
        /// <param name="sqlBulkCopy">SqlBulkCopy to use for bulk insert.</param>
        /// <param name="bufferSize">Number of rows to bulk insert at a time. The default is 2000.</param>
        public BulkInserter(SqlConnection connection, SqlBulkCopy sqlBulkCopy, int bufferSize = DefaultBufferSize)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (sqlBulkCopy == null) throw new ArgumentNullException("sqlBulkCopy");

            _bufferSize = bufferSize;
            _connection = connection;
            _sbc = sqlBulkCopy;
            _dt = new Lazy<DataTable>(CreateDataTable);
        }

        /// <param name="connection">SqlConnection to use for retrieving the schema of sqlBulkCopy.DestinationTableName and for bulk insert.</param>
        /// <param name="tableName">The name of the table that rows will be inserted into.</param>
        /// <param name="bufferSize">Number of rows to bulk insert at a time. The default is 2000.</param>
        /// <param name="copyOptions">Options for SqlBulkCopy.</param>
        /// <param name="sqlTransaction">SqlTransaction for SqlBulkCopy</param>
        public BulkInserter(SqlConnection connection, string tableName, int bufferSize = DefaultBufferSize,
                            SqlBulkCopyOptions copyOptions = SqlBulkCopyOptions.Default, SqlTransaction sqlTransaction = null)
            : this(connection, new SqlBulkCopy(connection, copyOptions, sqlTransaction) { DestinationTableName = tableName }, bufferSize)
        {
        }

        /// <summary>
        /// Performs buffered bulk insert of enumerable items.
        /// </summary>
        /// <param name="items">The items to be inserted.</param>
        public void Insert(IEnumerable<T> items)
        {
            if (items == null) throw new ArgumentNullException("items");

            // get columns that have a matching property
            var cols = _dt.Value.Columns.Cast<DataColumn>()
                .Where(x => _props.Value.ContainsKey(x.ColumnName))
                .Select(x => new { Column = x, Getter = _props.Value[x.ColumnName] })
                .Where(x => x.Getter != null)
                .ToArray();

            var groups = items.Select((item, index) => new {item, index}).GroupBy(x => x.index/BufferSize, x => x.item);
            foreach (var group in groups)
            {
                foreach (var item in group)
                {
                    var row = _dt.Value.NewRow();

                    foreach (var col in cols)
                        row[col.Column] = col.Getter(item) ?? DBNull.Value;

                    _dt.Value.Rows.Add(row);
                }

                var bulkInsertEventArgs = new BulkInsertEventArgs<T>(group);
                OnPreBulkInsert(bulkInsertEventArgs);

                _sbc.WriteToServer(_dt.Value);

                OnPostBulkInsert(bulkInsertEventArgs);

                InsertedCount += _dt.Value.Rows.Count;
                _dt.Value.Clear();
            }
        }

        /// <summary>
        /// Queues a single item for bulk insert. When the queue count reaches the buffer size, bulk insert will happen.
        /// Call Flush() to manually bulk insert the currently queued items.
        /// </summary>
        /// <param name="item">The item to be inserted.</param>
        public void Insert(T item)
        {
            if (item == null) throw new ArgumentNullException("item");

            _queue.Add(item);

            if (_queue.Count == _bufferSize)
                Flush();
        }

        /// <summary>
        /// Bulk inserts the currently queued items.
        /// </summary>
        public void Flush()
        {
            Insert(_queue);
            _queue.Clear();
        }

        /// <summary>
        /// Sets the InsertedCount property to zero.
        /// </summary>
        public void ResetInsertedCount()
        {
            InsertedCount = 0;
        }

        private static Dictionary<string, MemberGetter> GetPropertyInformation()
        {
            return typeof(T).Properties().ToDictionary(x => x.Name, x => x.DelegateForGetPropertyValue());
        }

        private DataTable CreateDataTable()
        {
            var dt = new DataTable();
            using (var cmd = _connection.CreateCommand())
            {
                cmd.CommandText = string.Format("select top 0 * from {0}", _sbc.DestinationTableName);

                using (var reader = cmd.ExecuteReader())
                    dt.Load(reader);
            }

            return dt;
        }
    }
}