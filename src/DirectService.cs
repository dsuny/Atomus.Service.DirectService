using Atomus.Attribute;
using Atomus.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data;

namespace Atomus.Service
{
    /// <summary>
    /// 다이렉트 서비스를 구현합니다.
    /// </summary>
    #region
    [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
    /// <summary>
    /// 다이렉트 서비스를 구현합니다.
    /// </summary>")]
    #endregion
    public class DirectService : IService
    {
        private readonly int serviceTimeout;
        private readonly Database.IDatabaseAdapter databaseAdapter;

        /// <summary>
        /// 생성자 입니다.
        /// 서비스 타임 시간(ms)을 가져옵니다.
        /// databaseAdapter를 생성합니다.
        /// </summary>
        #region
        [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
        /// <summary>
        /// 생성자 입니다.
        /// 서비스 타임 시간(ms)을 가져옵니다.
        /// databaseAdapter를 생성합니다.
        /// </summary>")]
        [HistoryComment("권대선", 2017, 10, 9, AuthorAttributeType.Modify, @"
        /// <summary>
        /// databaseAdapter 생성 추가
        /// </summary>")]
        #endregion
        public DirectService()
        {
            try
            {
                this.serviceTimeout = this.GetAttribute("ServiceTimeout").ToInt();
            }
            catch (Exception exception)
            {
                DiagnosticsTool.MyTrace(exception);
                this.serviceTimeout = 60000;
            }

            try
            {
                databaseAdapter = (Database.IDatabaseAdapter)Factory.CreateInstance("Atomus.Database.DatabaseAdapter");
            }
            catch (Exception exception)
            {
                DiagnosticsTool.MyTrace(exception);
            }
        }

        #region
        [HistoryComment("권대선", 2017, 10, 9, AuthorAttributeType.Modify, @"
        /// <summary>
        /// databaseAdapter를 멤버로 이동하여 중복적인 인스턴스 생성를 방지
        ///
        /// ServiceDataSet 속성 값 중에 ""GetDatabaseNames""이 있으면 DatabaseNames을 가져옵니다.
        /// </summary>")]
        #endregion
        Response IService.Request(ServiceDataSet serviceDataSet)
        {
            try
            {
                if (!serviceDataSet.ServiceName.Equals("Atomus.Service.DirectService"))
                    throw new Exception("Not Atomus.Service.DirectService");

                if (((IServiceDataSet)serviceDataSet).GetAttribute("GetDatabaseNames") != null)
                    return GetDatabaseConnectionNames();

                ((IServiceDataSet)serviceDataSet).CreateServiceDataTable();

                return (Response)this.Excute(serviceDataSet);
            }
            catch (AtomusException exception)
            {
                DiagnosticsTool.MyTrace(exception);
                return (Response)Factory.CreateInstance("Atomus.Service.Response", false, true, exception);
            }
            catch (Exception exception)
            {
                DiagnosticsTool.MyTrace(exception);
                return (Response)Factory.CreateInstance("Atomus.Service.Response", false, true, exception);
            }
        }

        #region
        [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
        /// <summary>
        /// 다이렉트 서비스를 수행 합니다.
        /// </summary>
        /// <returns>서비스 처리 결과를 반환합니다.</returns>")]
        #endregion
        IResponse Excute(ServiceDataSet serviceDataSet)
        {
            Dictionary<string, Database.IDatabase> databaseList;
            Database.IDatabase database;
            IResponse response;
            DataSet dataSet;
            DataTable dataTable;
            int tableCount;
            string databaseName;
            IServiceDataTable serviceDataTable;

            databaseList = new Dictionary<string, Database.IDatabase>();

            try
            {
                //ServiceDataSet에 DatabaseName 값이 없으면 ServiceDataTable에 DatabaseName이 있음
                if (((IServiceDataSet)serviceDataSet).GetAttribute("DatabaseName") == null)
                    for (int i = 0; i < ((IServiceDataSet)serviceDataSet).Count; i++)
                    {
                        //((IServiceDataSet)serviceDataSet).CurrentIndex = i;
                        //databaseName = ((IServiceDataSet)serviceDataSet).ServiceDataTable.GetAttribute("DatabaseName").ToString();

                        databaseName = ((IServiceDataSet)serviceDataSet)[i].GetAttribute("DatabaseName").ToString();
                        if (!databaseList.ContainsKey(databaseName))
                            databaseList.Add(databaseName, this.databaseAdapter.CreateDatabase(databaseName));
                    }
                else
                {
                    databaseName = ((IServiceDataSet)serviceDataSet).GetAttribute("DatabaseName").ToString();

                    if (!databaseList.ContainsKey(databaseName))
                        databaseList.Add(databaseName, this.databaseAdapter.CreateDatabase(databaseName));
                }

                database = null;

                //ServiceDataSet에 DatabaseName 값이 없으면 ServiceDataTable에 DatabaseName이 있음
                if (((IServiceDataSet)serviceDataSet).GetAttribute("DatabaseName") != null)
                {
                    database = databaseList[((IServiceDataSet)serviceDataSet).GetAttribute("DatabaseName").ToString()];
                    database.Command.CommandTimeout = serviceTimeout;
                    database.Command.CommandType = System.Data.CommandType.Text;
                    database.Connection.Open();
                }

                response = (IResponse)Factory.CreateInstance("Atomus.Service.Response", false, true);
                tableCount = 0;
                foreach (DataTable table in ((IServiceDataSet)serviceDataSet).DataTables)
                {
                    if (database == null)
                    {
                        serviceDataTable = (IServiceDataTable)table.ExtendedProperties["ServiceDataTable"];

                        database = databaseList[serviceDataTable.GetAttribute("DatabaseName").ToString()];
                        database.Command.CommandTimeout = serviceTimeout;
                        database.Command.CommandType = System.Data.CommandType.Text;
                        database.Connection.Open();
                    }

                    foreach (DataRow dataRow in table.Rows)
                    {
                        dataSet = new DataSet();
                        database.Command.CommandText = (string)dataRow["Query"];
                        database.DataAdapter.Fill(dataSet);

                        while (dataSet.Tables.Count != 0)
                        {
                            dataTable = dataSet.Tables[0];
                            dataTable.TableName = tableCount.ToString();
                            dataSet.Tables.Remove(dataTable);
                            response.DataSet.Tables.Add(dataTable);

                            tableCount += 1;
                        }
                    }
                }

                if (response.DataSet.Tables.Count < 1)
                    response.DataSet = null;

                response.Status = Status.OK;
            }
            finally
            {
                foreach (Database.IDatabase database1 in databaseList.Values)
                    database1.Close();
            }

            return (Response)response;
        }

        #region
        [Author("권대선", 2017, 9, 25, AuthorAttributeType.Create, @"
        /// <summary>
        /// DatabaseNames을 가져옵니다.
        /// </summary>
        /// <returns>IResponse.DataSet.Tables[""DatabaseNames""].Rows[index][""DatabaseNames""]</returns>")]
        #endregion
        Response GetDatabaseConnectionNames()
        {
            IResponse response;
            DataTable dataTable;
            DataRow dataRow;
            string[] databaseConnectionNames;

            response = (IResponse)Factory.CreateInstance("Atomus.Service.Response", false, true);

            dataTable = new DataTable("DatabaseNames");

            dataTable.Columns.Add("DatabaseNames", Type.GetType("System.String"));

            databaseConnectionNames = this.databaseAdapter.DatabaseConnectionNames;

            foreach (string tmp in databaseConnectionNames)
            {
                dataRow = dataTable.NewRow();
                dataRow["DatabaseNames"] = tmp;
                dataTable.Rows.Add(dataRow);
            }

            response.DataSet.Tables.Add(dataTable);

            return (Response)response;
        }
    }
}