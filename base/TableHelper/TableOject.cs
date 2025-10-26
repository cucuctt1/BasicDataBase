// table object for query use

// -> table object -> table query => table object new data -> table query -> ...

namespace BasicDataBase.TableHelper
{
    public class TableObject
    {
        public string DatabaseName { get; set; }
        public string TableName { get; set; }

        public object[] RowData { get; set; }

        public TableObject(string databaseName, string tableName)
        {
            DatabaseName = databaseName;
            TableName = tableName;
        }

        public TableObject SelectRow(int rowIndex)
        {
            // load row data from table
            var tableOperate = new TableOperate(DatabaseName, TableName);
            var rowDataArray = tableOperate.ReadRow(rowIndex);
            this.RowData = rowDataArray as object[] ?? new object[0];
            return this;
        }

        public TableQuery Query()
        {
            return new TableQuery(DatabaseName, TableName);
        }
    }
}