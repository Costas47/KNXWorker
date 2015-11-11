using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading.Tasks;
using Knx.Bus.Common.Configuration;
using Knx.Bus.Common;
using Knx.Bus.Common.GroupValues;
using Knx.Bus.Common.Exceptions;
using Knx.Falcon.Sdk;
using KNXWorker.DPT;
using Microsoft.Win32.TaskScheduler;
using System.Numerics;
using System.Collections;
using System.Net.Mail;
using System.Net;

namespace KNXWorker
{
    class Program
    {
        static void switchON(string _taskguid, string _dtable)
        {
            string _TaskID = string.Empty;
            string _channelGroup = string.Empty;
            string _PillarID = string.Empty;
            string _TaskSubject = string.Empty;
            DateTime _TaskStart = DateTime.Now;
            DateTime _TaskEnd = DateTime.Now;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [ID],[ChannelGroup],[PillarID],[Subject],[Start],[End] FROM [db_knx].[dbo].[Scheduler] WHERE [TaskGUID] LIKE N'" + _taskguid + "' AND [Start] = (SELECT MIN([Start]) FROM [db_knx].[dbo].[Scheduler] WHERE [TaskGUID] LIKE N'"+_taskguid+"');";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        _TaskID = _dt["ID"].ToString();
                        _channelGroup = _dt["ChannelGroup"].ToString();
                        _PillarID = _dt["PillarID"].ToString();
                        _TaskSubject = _dt["Subject"].ToString();
                        _TaskStart = (DateTime)_dt["Start"];
                        _TaskEnd = (DateTime)_dt["End"];
                    }

                    _dt.Close();
                }

                conn.Close();
            }

            string[] ChannelsArray = _channelGroup.Split(',');
            for(var i=0; i<ChannelsArray.Count(); i++)
            {
                while(ChannelsArray[i].IndexOf(" ") > -1)
                {
                    ChannelsArray[i] = ChannelsArray[i].Replace(" ", "");
                }
            }

            string AddressIP = string.Empty; string AddressPort = string.Empty; bool AddressNat = false;
            string TypeKNX = string.Empty; string MeterSerial = string.Empty;
            List<string> GroupAddressMeter = new List<string>(); List<string> GroupAddressON = new List<string>();

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [AddressIP],[AddressPort],[AddressNat],[TypeKNX],[MeterSerialNo] FROM [db_knx].[dbo].[Pillars] WHERE [ID] = " + _PillarID + ";";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        AddressIP = _dt["AddressIP"].ToString();
                        AddressPort = _dt["AddressPort"].ToString();
                        AddressNat = (bool)_dt["AddressNat"];
                        TypeKNX = _dt["TypeKNX"].ToString();
                        MeterSerial = _dt["MeterSerialNo"].ToString();
                    }

                    _dt.Close();
                }

                for (var i = 0; i < ChannelsArray.Count(); i++)
                {
                    sqlSelect = "SELECT [On/Off Address],[MeasureCurrent Address] FROM [db_knx].[dbo].[" + TypeKNX + "_" + MeterSerial + "] WHERE [Departure] = " + ChannelsArray[i] + ";";

                    using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                    {
                        SqlDataReader _dt = sqlComm.ExecuteReader();

                        while (_dt.Read())
                        {
                            GroupAddressON.Add(_dt["On/Off Address"].ToString());
                            GroupAddressMeter.Add(_dt["MeasureCurrent Address"].ToString());
                        }

                        _dt.Close();
                    }
                }

                conn.Close();
            }

            Ping pingOrder = new Ping();
            for (var i=0; i<4; i++)
            {
                try
                {
                    pingOrder.Send(AddressIP);
                }
                catch (PingException ex)
                {

                }
            }
            pingOrder = null;

            using (Bus _bus = new Bus(new KnxIpTunnelingConnectorParameters(AddressIP, ushort.Parse(AddressPort), AddressNat)))
            {
                try
                {
                    _bus.Connect();

                    for (var i = 0; i < GroupAddressON.Count; i++)
                    {
                        GroupAddress _groupAddress = GroupAddress.Parse(GroupAddressON[i]);
                        GroupValue _groupValue = new GroupValue(true);
                        _bus.WriteValue(_groupAddress, _groupValue, Priority.Low);

                        //GroupAddress _groupAddressMeter = GroupAddress.Parse(GroupAddressMeter[i]);
                        //GroupValue groupValue = _bus.ReadValue(_groupAddressMeter, Priority.Low);

                        //DataPointTranslator _dpt = new DataPointTranslator();
                        //decimal _value = (decimal)_dpt.FromDataPoint("9.001", groupValue.Value);
                        //storeValueToDB(_value, MeterSerial, ChannelsArray[i]);
                    }

                    _bus.Disconnect();
                    _bus.Dispose();
                    updateSchedulerTable(_TaskID, false);
                    updatePillarDeparturesSQL(_PillarID, _channelGroup, true);
                    updateTask(_taskguid, "On");
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, null, null, null);
                }
                catch (ConnectorException ex)
                {
                    updateSchedulerTable(_TaskID, true);
                    updateReportScheduler1(_TaskID, ex.ErrorReason);
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, ex, null, null);
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                catch (ConnectionException ex)
                {
                    updateSchedulerTable(_TaskID, true);
                    updateReportScheduler2(_TaskID, ex.ErrorReason);
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, null, ex, null);
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                catch (NoResponseReceivedException ex)
                {
                    updateSchedulerTable(_TaskID, true);
                    updateReportScheduler3(_TaskID, ex.ErrorReason);
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, null, null, ex);
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                finally
                {
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
            }
        }

        static void switchOFF(string _taskguid, string _dtable)
        {
            string _TaskID = string.Empty;
            string _channelGroup = string.Empty;
            string _PillarID = string.Empty;
            string _TaskSubject = string.Empty;
            DateTime _TaskStart = DateTime.Now;
            DateTime _TaskEnd = DateTime.Now;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [ID],[ChannelGroup],[PillarID],[Subject],[Start],[End] FROM [db_knx].[dbo].[Scheduler] WHERE [TaskGUID] LIKE N'" + _taskguid + "' AND [Start] = (SELECT MAX([Start]) FROM [db_knx].[dbo].[Scheduler] WHERE [TaskGUID] LIKE N'" + _taskguid + "');";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        _TaskID = _dt["ID"].ToString();
                        _channelGroup = _dt["ChannelGroup"].ToString();
                        _PillarID = _dt["PillarID"].ToString();
                        _TaskSubject = _dt["Subject"].ToString();
                        _TaskStart = (DateTime)_dt["Start"];
                        _TaskEnd = (DateTime)_dt["End"];
                    }

                    _dt.Close();
                }

                conn.Close();
            }

            string[] ChannelsArray = _channelGroup.Split(',');
            for (var i = 0; i < ChannelsArray.Count(); i++)
            {
                while (ChannelsArray[i].IndexOf(" ") > -1)
                {
                    ChannelsArray[i] = ChannelsArray[i].Replace(" ", "");
                }
            }

            string AddressIP = string.Empty; string AddressPort = string.Empty; bool AddressNat = false;
            string TypeKNX = string.Empty; string MeterSerial = string.Empty;
            List<string> GroupAddressPowerMeter = new List<string>(); List<string> GroupAddressOFF = new List<string>();

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [AddressIP],[AddressPort],[AddressNat],[TypeKNX],[MeterSerialNo] FROM [db_knx].[dbo].[Pillars] WHERE [ID] = " + _PillarID + ";";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        AddressIP = _dt["AddressIP"].ToString();
                        AddressPort = _dt["AddressPort"].ToString();
                        AddressNat = (bool)_dt["AddressNat"];
                        TypeKNX = _dt["TypeKNX"].ToString();
                        MeterSerial = _dt["MeterSerialNo"].ToString();
                    }

                    _dt.Close();
                }

                for (var i = 0; i < ChannelsArray.Count(); i++)
                {
                    sqlSelect = "SELECT [On/Off Address],[MeasurePower Address] FROM [db_knx].[dbo].[" + TypeKNX + "_" + MeterSerial + "] WHERE [Departure] = " + ChannelsArray[i] + ";";

                    using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                    {
                        SqlDataReader _dt = sqlComm.ExecuteReader();

                        while (_dt.Read())
                        {
                            GroupAddressOFF.Add(_dt["On/Off Address"].ToString());
                            GroupAddressPowerMeter.Add(_dt["MeasurePower Address"].ToString());
                        }

                        _dt.Close();
                    }
                }

                conn.Close();
            }

            Ping pingOrder = new Ping();
            for (var i = 0; i < 4; i++)
            {
                try
                {
                    pingOrder.Send(AddressIP);
                }
                catch (PingException ex)
                {
                }
            }
            pingOrder = null;

            using (Bus _bus = new Bus(new KnxIpTunnelingConnectorParameters(AddressIP, ushort.Parse(AddressPort), AddressNat)))
            {
                try
                {
                    _bus.Connect();

                    for (var i = 0; i < GroupAddressOFF.Count; i++)
                    {
                        GroupAddress _groupAddressPowerMeter = GroupAddress.Parse(GroupAddressPowerMeter[i]);
                        GroupValue groupValue = _bus.ReadValue(_groupAddressPowerMeter, Priority.Low);

                        //DataPointTranslator _dpt = new DataPointTranslator();
                        //decimal _value = (decimal)_dpt.FromDataPoint("9.001", groupValue.Value);
                        //storeValueToDB(_value, MeterSerial, ChannelsArray[i]);

                        string bits = Convert.ToString(groupValue.Value[0],2).PadLeft(8, '0');
                        bits += Convert.ToString(groupValue.Value[1], 2).PadLeft(8, '0');
                        bits += Convert.ToString(groupValue.Value[2], 2).PadLeft(8, '0');
                        bits += Convert.ToString(groupValue.Value[3], 2).PadLeft(8, '0');
                        int output = Convert.ToInt32(bits, 2);

                        storePowerValueToDB(output, MeterSerial, ChannelsArray[i]);

                        GroupAddress _groupAddress = GroupAddress.Parse(GroupAddressOFF[i]);
                        GroupValue _groupValue = new GroupValue(false);
                        _bus.WriteValue(_groupAddress, _groupValue, Priority.Low);
                    }

                    _bus.Disconnect();
                    _bus.Dispose();
                    updateSchedulerTable(_TaskID,false);
                    updatePillarDeparturesSQL(_PillarID, _channelGroup, false);
                    updateTask(_taskguid, "Off");
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, null, null, null);
                }
                catch(ConnectorException ex){
                    updateSchedulerTable(_TaskID,true);
                    updateReportScheduler1(_TaskID,ex.ErrorReason);
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, ex, null, null);
                    if(_bus != null){
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                catch(ConnectionException ex){
                    updateSchedulerTable(_TaskID,true);
                    updateReportScheduler2(_TaskID,ex.ErrorReason);
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, null, ex, null);
                    if(_bus != null){
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                catch(NoResponseReceivedException ex){
                    updateSchedulerTable(_TaskID,true);
                    updateReportScheduler3(_TaskID,ex.ErrorReason);
                    emailWaring(_TaskID, _TaskSubject, _TaskStart, _TaskEnd, _channelGroup, _PillarID, null, null, ex);
                    if(_bus != null){
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                finally{
                    if(_bus != null){
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
            }
        }

        static void meterCurrent(string _taskguid, string _dtable)
        {
            string _TaskID = string.Empty;
            string _channelGroup = string.Empty;
            string _PillarID = string.Empty;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [ID],[ChannelGroup],[PillarID] FROM [db_knx].[dbo].["+_dtable+"] WHERE [TaskGUID] LIKE N'" + _taskguid + "' AND [Start] = (SELECT MAX([Start]) FROM [db_knx].[dbo].[Scheduler] WHERE [TaskGUID] LIKE N'" + _taskguid + "');";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        _channelGroup = _dt["ChannelGroup"].ToString();
                        _PillarID = _dt["PillarID"].ToString();
                    }

                    _dt.Close();
                }

                conn.Close();
            }

            string[] ChannelsArray = _channelGroup.Split(',');
            for (var i = 0; i < ChannelsArray.Count(); i++)
            {
                while (ChannelsArray[i].IndexOf(" ") > -1)
                {
                    ChannelsArray[i] = ChannelsArray[i].Replace(" ", "");
                }
            }

            string AddressIP = string.Empty; string AddressPort = string.Empty; bool AddressNat = false;
            string TypeKNX = string.Empty; string MeterSerial = string.Empty; List<string> GroupAddressMeasure = new List<string>();

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [AddressIP],[AddressPort],[AddressNat],[TypeKNX],[MeterSerialNo] FROM [db_knx].[dbo].[Pillars] WHERE [ID] = " + _PillarID + ";";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        AddressIP = _dt["AddressIP"].ToString();
                        AddressPort = _dt["AddressPort"].ToString();
                        AddressNat = (bool)_dt["AddressNat"];
                        TypeKNX = _dt["TypeKNX"].ToString();
                        MeterSerial = _dt["MeterSerialNo"].ToString();
                    }

                    _dt.Close();
                }

                for (var i = 0; i < ChannelsArray.Count(); i++)
                {
                    sqlSelect = "SELECT [MeasureCurrent Address] FROM [db_knx].[dbo].[" + TypeKNX + "_" + MeterSerial + "] WHERE [Departure] = " + ChannelsArray[i] + ";";

                    using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                    {
                        SqlDataReader _dt = sqlComm.ExecuteReader();

                        while (_dt.Read())
                        {
                            if (!_dt.IsDBNull(0))
                            {
                                GroupAddressMeasure.Add(_dt["MeasureCurrent Address"].ToString());
                            }
                        }

                        _dt.Close();
                    }
                }

                conn.Close();
            }

            Ping pingOrder = new Ping();
            for (var i = 0; i < 4; i++)
            {
                try
                {
                    pingOrder.Send(AddressIP);
                }
                catch (PingException ex)
                {

                }
            }
            pingOrder = null;

            using (Bus _bus = new Bus(new KnxIpTunnelingConnectorParameters(AddressIP, ushort.Parse(AddressPort), AddressNat)))
            {
                try
                {
                    _bus.Connect();

                    for (var i = 0; i < GroupAddressMeasure.Count; i++)
                    {
                        GroupValue groupValue = _bus.ReadValue(GroupAddress.Parse(GroupAddressMeasure[i]), Priority.Low);
                        if (groupValue != null)
                        {
                            try
                            {
                                DataPointTranslator _dpt = new DataPointTranslator();
                                decimal _value = (decimal)_dpt.FromDataPoint("9.001", groupValue.Value);
                                storeCurrentValueToDB(_value, MeterSerial, ChannelsArray[i]);
                            }
                            catch (Exception exception)
                            {
                                Console.WriteLine(exception);
                            }
                        }
                    }

                    _bus.Disconnect();
                    _bus.Dispose();
                    updateTask(_taskguid, "Meter");
                }
                catch (ConnectorException ex)
                {
                    updateReportScheduler1(_TaskID, ex.ErrorReason);
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                catch (ConnectionException ex)
                {
                    updateReportScheduler2(_TaskID, ex.ErrorReason);
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                catch (NoResponseReceivedException ex)
                {
                    updateReportScheduler3(_TaskID, ex.ErrorReason);
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
                finally
                {
                    if (_bus != null)
                    {
                        _bus.Disconnect();
                        _bus.Dispose();
                    }
                }
            }
        }

        static void storeCurrentValueToDB(decimal _value, string _meterSerial, string _channelID)
        {
            bool MeasurementTableExists = checkMeasurementTable(_meterSerial);

            if(MeasurementTableExists == false)
            {
                createMeasurementTable(_meterSerial);
            }

            insertCurrentValueToDB(_value, _meterSerial, _channelID);
        }

        static void storePowerValueToDB(decimal _value, string _meterSerial, string _channelID)
        {
            bool PowerMeterTableExists = checkPowerMeterTable(_meterSerial);

            if (PowerMeterTableExists == false)
            {
                createPowerMeterTable(_meterSerial);
            }

            insertPowerValueToDB(_value, _meterSerial, _channelID);
        }

        static void createMeasurementTable(string _meter)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlCreateStatement = "USE [db_knx];" +
                    "SET ANSI_NULLS ON;" +
                    "SET QUOTED_IDENTIFIER ON;" +
                    "CREATE TABLE[dbo].[Measurements_"+_meter+"](" +
                    "[ID][int] IDENTITY(1, 1) NOT NULL," +
                    "[DateTime] [datetime] NOT NULL," +
                    "[CurrentValue] [nvarchar](50) NOT NULL," +
                    "[ChannelFK] [int] NOT NULL," +
                    "CONSTRAINT[PK_Measurements_"+_meter+"] PRIMARY KEY CLUSTERED" +
                    "([ID] ASC)WITH(PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON[PRIMARY]" +
                    ") ON[PRIMARY];";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlCreateStatement, conn))
                {
                    sqlComm.ExecuteReader();
                }

                conn.Close();
            }
        }

        static void createPowerMeterTable(string _meter)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlCreateStatement = "USE [db_knx];" +
                    "SET ANSI_NULLS ON;" +
                    "SET QUOTED_IDENTIFIER ON;" +
                    "CREATE TABLE[dbo].[PowerMeter_" + _meter + "](" +
                    "[ID][int] IDENTITY(1, 1) NOT NULL," +
                    "[DateTime] [datetime] NOT NULL," +
                    "[CurrentKW] [nvarchar](50) NOT NULL," +
                    "[ChannelFK] [int] NULL," +
                    "CONSTRAINT[PK_PowerMeter_" + _meter + "] PRIMARY KEY CLUSTERED" +
                    "([ID] ASC)WITH(PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON[PRIMARY]" +
                    ") ON[PRIMARY];";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlCreateStatement, conn))
                {
                    sqlComm.ExecuteReader();
                }

                conn.Close();
            }
        }

        static void insertCurrentValueToDB(decimal _value, string _meterSerial, string _channelID)
        {
            using (SqlConnection DBconnection = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                DBconnection.Open();

                string SQLQuery = "INSERT INTO [db_knx].[dbo].[Measurements_"+_meterSerial+"]" +
                "([DateTime],[CurrentValue],[ChannelFK]) VALUES (@DateTime,@CurrentValue,@ChannelFK);";

                using (SqlCommand cmd = new SqlCommand(SQLQuery, DBconnection))
                {
                    SqlParameter _datetime = new SqlParameter();
                    _datetime.DbType = DbType.DateTime;
                    _datetime.ParameterName = "@DateTime";
                    _datetime.Value = DateTime.Now;
                    cmd.Parameters.Add(_datetime);

                    SqlParameter _CurrentValue = new SqlParameter();
                    _CurrentValue.DbType = DbType.Decimal;
                    _CurrentValue.ParameterName = "@CurrentValue";
                    _CurrentValue.Value = _value;
                    cmd.Parameters.Add(_CurrentValue);

                    SqlParameter _channelFK = new SqlParameter();
                    _channelFK.DbType = DbType.Int32;
                    _channelFK.ParameterName = "@ChannelFK";
                    _channelFK.Value = int.Parse(_channelID);
                    cmd.Parameters.Add(_channelFK);

                    cmd.ExecuteNonQuery();
                }
                
                DBconnection.Close();
            }
        }

        static void insertPowerValueToDB(decimal _value, string _meterSerial, string _channelID)
        {
            using (SqlConnection DBconnection = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                DBconnection.Open();

                string SQLQuery = "INSERT INTO [db_knx].[dbo].[PowerMeter_" + _meterSerial + "]" +
                "([DateTime],[CurrentKW],[ChannelFK]) VALUES (@DateTime,@CurrentKW,@ChannelFK);";

                using (SqlCommand cmd = new SqlCommand(SQLQuery, DBconnection))
                {
                    SqlParameter _datetime = new SqlParameter();
                    _datetime.DbType = DbType.DateTime;
                    _datetime.ParameterName = "@DateTime";
                    _datetime.Value = DateTime.Now;
                    cmd.Parameters.Add(_datetime);

                    SqlParameter _CurrentValue = new SqlParameter();
                    _CurrentValue.DbType = DbType.Decimal;
                    _CurrentValue.ParameterName = "@CurrentKW";
                    _CurrentValue.Value = _value;
                    cmd.Parameters.Add(_CurrentValue);

                    SqlParameter _channelFK = new SqlParameter();
                    _channelFK.DbType = DbType.Int32;
                    _channelFK.ParameterName = "@ChannelFK";
                    _channelFK.Value = int.Parse(_channelID);
                    cmd.Parameters.Add(_channelFK);

                    cmd.ExecuteNonQuery();
                }

                DBconnection.Close();
            }
        }

        static bool checkMeasurementTable(string _meterSerial)
        {
            bool _exists = false;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlExistsStatement = "IF EXISTS(SELECT * FROM db_knx.sys.tables WHERE [name] = 'Measurements_" + _meterSerial + "')" +
                    "SELECT 1 AS Result ELSE SELECT 0 AS Result;";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlExistsStatement, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        if (_dt["Result"].ToString() == "1")
                        {
                            _exists = true;
                        }
                        else
                        {
                            _exists = false;
                        }
                    }

                    _dt.Close();
                }
            }

            return _exists;
        }

        static bool checkPowerMeterTable(string _meterSerial)
        {
            bool _exists = false;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlExistsStatement = "IF EXISTS(SELECT * FROM db_knx.sys.tables WHERE [name] = 'PowerMeter_" + _meterSerial + "')" +
                    "SELECT 1 AS Result ELSE SELECT 0 AS Result;";

                conn.Open();

                using (SqlCommand sqlComm = new SqlCommand(sqlExistsStatement, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        if (_dt["Result"].ToString() == "1")
                        {
                            _exists = true;
                        }
                        else
                        {
                            _exists = false;
                        }
                    }

                    _dt.Close();
                }
            }

            return _exists;
        }

        static void updateSchedulerTable(string taskID, bool hasError)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();

                string sqlUpdate = string.Empty;

                switch(hasError){
                case true:
                        sqlUpdate = "UPDATE [db_knx].[dbo].[Scheduler] SET [HasErrors] = 1 WHERE [ID] = " + taskID + ";";
                    break;
                case false:
                        sqlUpdate = "UPDATE [db_knx].[dbo].[Scheduler] SET [HasErrors] = 0 WHERE [ID] = " + taskID + ";";
                    break;
                }

                using (SqlCommand sqlComm = new SqlCommand(sqlUpdate, conn))
                {
                    sqlComm.ExecuteNonQuery();
                }

                conn.Close();
            }
        }

        static void updatePillarDeparturesSQL(string pillarID, string departures, bool state)
        {
            string TypeKNX = string.Empty;
            string MeterSerial = string.Empty;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();

                string sqlSelect = "SELECT [TypeKNX],[MeterSerialNo] FROM [db_knx].[dbo].[Pillars] WHERE [ID] = "+pillarID+";";

                using (SqlCommand sqlComm = new SqlCommand(sqlSelect, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        TypeKNX = _dt["TypeKNX"].ToString();
                        MeterSerial = _dt["MeterSerialNo"].ToString();
                    }

                    _dt.Close();
                }

                string sqlUpdateDepartures = string.Empty;

                if (departures != "0")
                {
                    switch (state)
                    {
                        case true:
                            sqlUpdateDepartures = "UPDATE [" + TypeKNX + "_" + MeterSerial + "] SET [StateDeparture] = 1 WHERE [Departure] = " + departures + ";";
                            break;
                        case false:
                            sqlUpdateDepartures = "UPDATE [" + TypeKNX + "_" + MeterSerial + "] SET [StateDeparture] = 0 WHERE [Departure] = " + departures + ";";
                            break;
                    }
                }
                else
                {
                    switch (state)
                    {
                        case true:
                            sqlUpdateDepartures = "UPDATE [" + TypeKNX + "_" + MeterSerial + "] SET [StateDeparture] = 1;";
                            break;
                        case false:
                            sqlUpdateDepartures = "UPDATE [" + TypeKNX + "_" + MeterSerial + "] SET [StateDeparture] = 0;";
                            break;
                    }
                }

                using (SqlCommand sqlComm = new SqlCommand(sqlUpdateDepartures, conn))
                {
                    sqlComm.ExecuteNonQuery();
                }

                string sqlUpdateLamps = string.Empty;

                if (departures != "0")
                {
                    switch (state)
                    {
                        case true:
                            sqlUpdateLamps = "UPDATE [PowerControl] SET [State] = 1 WHERE [TasFK] = " + pillarID + " AND [ChannelFK] = " + departures + ";";
                            break;
                        case false:
                            sqlUpdateLamps = "UPDATE [PowerControl] SET [State] = 0 WHERE [TasFK] = " + pillarID + " AND [ChannelFK] = " + departures + ";";
                            break;
                    }
                }
                else
                {
                    switch (state)
                    {
                        case true:
                            sqlUpdateLamps = "UPDATE [PowerControl] SET [State] = 1 WHERE [TasFK] = " + pillarID + ";";
                            break;
                        case false:
                            sqlUpdateLamps = "UPDATE [PowerControl] SET [State] = 0 WHERE [TasFK] = " + pillarID + ";";
                            break;
                    }
                }

                using (SqlCommand sqlComm = new SqlCommand(sqlUpdateLamps, conn))
                {
                    sqlComm.ExecuteNonQuery();
                }

                conn.Close();
            }
        }

        static void updateReportScheduler1(string taskID, ConnectorException.Reason errorReason)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();

                string sqlInsert = "INSERT INTO [db_knx].[dbo].[ReportScheduler] ([TaskID],[ErrorID],[ErrorException]) VALUES "+
                    " (" + taskID + ",N'" + errorReason + "',N'ConnectorException')";

                using (SqlCommand sqlComm = new SqlCommand(sqlInsert, conn))
                {
                    sqlComm.ExecuteNonQuery();
                }

                conn.Close();
            }
        }

        static void updateReportScheduler2(string taskID, ConnectionException.Reason errorReason)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();

                string sqlInsert = "INSERT INTO [db_knx].[dbo].[ReportScheduler] ([TaskID],[ErrorID],[ErrorException]) VALUES " +
                    " (" + taskID + ",N'" + errorReason + "',N'ConnectionException')";

                using (SqlCommand sqlComm = new SqlCommand(sqlInsert, conn))
                {
                    sqlComm.ExecuteNonQuery();
                }

                conn.Close();
            }
        }

        static void updateReportScheduler3(string taskID, NoResponseReceivedException.Reason errorReason)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();

                string sqlInsert = "INSERT INTO [db_knx].[dbo].[ReportScheduler] ([TaskID],[ErrorID],[ErrorException]) VALUES " +
                    " (" + taskID + ",N'" + errorReason + "',N'NoResponseReceivedException')";

                using (SqlCommand sqlComm = new SqlCommand(sqlInsert, conn))
                {
                    sqlComm.ExecuteNonQuery();
                }

                conn.Close();
            }
        }

        static void updateTask(string _taskguid, string _job)
        {
            using (TaskService ts = new TaskService())
            {
                var _task = ts.FindTask("KNXTask" + _job + _taskguid, true);

                bool _taskExists = (_task != null);

                if(_taskExists){
                    _task.Enabled = false;
                }

                var _taskPing = ts.FindTask("KNXTaskPing" + _job + _taskguid, true);

                bool _taskPingExists = (_taskPing != null);

                if (_taskPingExists)
                {
                    _taskPing.Enabled = false;
                    if (_taskPing.State == TaskState.Running)
                    {
                        _taskPing.Stop();
                    }
                }
            }
        }

        static void emailWaring(string _id, string _subject, DateTime _start, DateTime _end, string _channels, string _pillar, ConnectorException connectorException, ConnectionException connectionException, NoResponseReceivedException noResponseException)
        {
            string emailBody = string.Empty;

            if (connectorException != null)
            {
                emailBody = "Γεια σας κ. Administrator.<br /><br />" +
                    "Σας ενημερώνουμε ότι η εργασία " + _id + " με την ημερομηνία έναρξης " + _start.ToString("d/M/yyyy H:mm tt") + " και την ημερομηνία λήξης " + _end.ToString("d/M/yyyy H:mm tt") + " ";
                if (_subject.IndexOf("LIGHTS ON") > -1)
                {
                    emailBody += "δεν άναψε";
                }

                if (_subject.IndexOf("LIGHTS OFF") > -1)
                {
                    emailBody += "δεν έσβησε";
                }

                if (_channels == "0")
                {
                    emailBody += " όλες τις ηλεκτρολογικές αναχωρήσεις";
                }
                else
                {
                    emailBody += " τις ηλεκτρολογικές αναχωρήσεις " + _channels;
                }

                emailBody += " του πυλώνα " + getPillarName(_pillar);

                switch (connectorException.ErrorReason)
                {
                    case ConnectorException.Reason.DeviceNotFound:
                        emailBody += " επειδή <span style=\"color:red;\">Η συσκευή KNX δεν μπορεί να βρεθεί</span>.<br/><br/>";
                        break;
                    case ConnectorException.Reason.DeviceNotRespond:
                        emailBody += " επειδή <span style=\"color:red;\">Η συσκευή KNX δεν ανταποκρίνεται στο αναμενόμενο χρόνο</span>.<br/><br/>";
                        break;
                    case ConnectorException.Reason.NoMoreConnections:
                        emailBody += " επειδή : <span style=\"color:red;\">Η συσκευή KNX δεν μπορεί να δεχτεί τη νέα σύνδεση, διότι το ανώτατο οριο των ταυτόχρονων συνδέσεων έχει υπερβεί ήδη</span>.<br/><br/>";
                        break;
                    default:
                        emailBody += " επειδή <span style=\"color:red;\">" + connectorException.ErrorReason + "</span>.<br/>";
                        break;
                }

                emailBody += "Με εκτίμηση,<br/>Σύστημα Διαχείρισης Ηλεκτροφωτισμού";
            }

            if (connectionException != null)
            {
                emailBody = "Γεια σας κ. Administrator.<br /><br />" +
                    "Σας ενημερώνουμε ότι η εργασία " + _id + " με την ημερομηνία έναρξης " + _start.ToString("d/M/yyyy H:mm tt") + " και την ημερομηνία λήξης " + _end.ToString("d/M/yyyy H:mm tt") + " ";
                if (_subject.IndexOf("LIGHTS ON") > -1)
                {
                    emailBody += "δεν άναψε";
                }

                if (_subject.IndexOf("LIGHTS OFF") > -1)
                {
                    emailBody += "δεν έσβησε";
                }

                if (_channels == "0")
                {
                    emailBody += " όλες τις ηλεκτρολογικές αναχωρήσεις";
                }
                else
                {
                    emailBody += " τις ηλεκτρολογικές αναχωρήσεις " + _channels;
                }

                emailBody += " του πυλώνα " + getPillarName(_pillar);

                switch (connectionException.ErrorReason)
                {
                    case ConnectionException.Reason.NotConnected:
                        emailBody += " επειδή <span style=\"color:red;\">Η σύνδεση δεν εγκαθιδρύθηκε</span>.<br/><br/>";
                        break;
                    case ConnectionException.Reason.NoMoreConnections:
                        emailBody += " επειδή <span style=\"color:red;\">Η συσκευή δεν μπορεί να δεχθεί τη νέα σύνδεση, διότι το ανώτατο ποσό των ταυτόχρονων συνδέσεων χρησιμοποιείται ήδη.</span>.<br/><br/>";
                        break;
                    case ConnectionException.Reason.ConnectionRefused:
                        emailBody += " επειδή <span style=\"color:red;\">Η σύνδεση αρνήθηκε από τη συσκευή προορισμού</span>.<br/><br/>";
                        break;
                    default:
                        emailBody += " επειδή <span style=\"color:red;\">" + connectionException.ErrorReason + "</span>.<br/>";
                        break;
                }

                emailBody += "Με εκτίμηση,<br/>Σύστημα Διαχείρισης Ηλεκτροφωτισμού";
            }

            if (noResponseException != null)
            {
                emailBody = "Γεια σας κ. Administrator.<br /><br />" +
                    "Σας ενημερώνουμε ότι η εργασία " + _id + " με την ημερομηνία έναρξης " + _start.ToString("d/M/yyyy H:mm tt") + " και την ημερομηνία λήξης " + _end.ToString("d/M/yyyy H:mm tt") + " ";
                if (_subject.IndexOf("LIGHTS ON") > -1)
                {
                    emailBody += "δεν άναψε";
                }

                if (_subject.IndexOf("LIGHTS OFF") > -1)
                {
                    emailBody += "δεν έσβησε";
                }

                if (_channels == "0")
                {
                    emailBody += " όλες τις ηλεκτρολογικές αναχωρήσεις";
                }
                else
                {
                    emailBody += " τις ηλεκτρολογικές αναχωρήσεις " + _channels;
                }

                emailBody += " του πυλώνα " + getPillarName(_pillar);

                switch (noResponseException.ErrorReason)
                {
                    case NoResponseReceivedException.Reason.Confirmation:
                        emailBody += " επειδή <span style=\"color:red;\">Δεν λήφθηκε επιβεβαίωση για το τηλεγράφημα</span>.<br/><br/>";
                        break;
                    case NoResponseReceivedException.Reason.NegativeConfirmation:
                        emailBody += " επειδή <span style=\"color:red;\">Λήφθηκε μια αρνητική επιβεβαίωση για το τηλεγράφημα</span>.<br/><br/>";
                        break;
                    case NoResponseReceivedException.Reason.Indication:
                        emailBody += " επειδή <span style=\"color:red;\">Καμία ένδειξη για το τηλεγράφημα</span>.<br/><br/>";
                        break;
                    default:
                        emailBody += " επειδή <span style=\"color:red;\">" + noResponseException.ErrorReason + "</span>.<br/>";
                        break;
                }

                emailBody += "Με εκτίμηση,<br/>Σύστημα Διαχείρισης Ηλεκτροφωτισμού";
            }
            
            if(connectorException == null && connectionException == null && noResponseException == null)
            {
                emailBody = "Γεια σας κ. Administrator.<br /><br />" +
                    "Σας ενημερώνουμε ότι η εργασία " + _id + " με την ημερομηνία έναρξης " + _start.ToString("d/M/yyyy H:mm tt") + " και την ημερομηνία λήξης " + _end.ToString("d/M/yyyy H:mm tt")+" ";
                if (_subject.IndexOf("LIGHTS ON") > -1)
                {
                    emailBody += "άναψε";
                }

                if (_subject.IndexOf("LIGHTS OFF") > -1)
                {
                    emailBody += "έσβησε";
                }

                if (_channels == "0")
                {
                    emailBody += " όλες τις ηλεκτρολογικές αναχωρήσεις";
                }
                else
                {
                    emailBody += " τις ηλεκτρολογικές αναχωρήσεις " + _channels;
                }

                emailBody += " του πυλώνα "+getPillarName(_pillar) + ".<br/><br/>";
                emailBody += "Με εκτίμηση,<br/>Σύστημα Διαχείρισης Ηλεκτροφωτισμού";
            }

            // Send email report to system administrator
            try
            {
                SmtpClient ob = new SmtpClient("smtp.office365.com");
                ob.UseDefaultCredentials = false;
                ob.EnableSsl = true;
                ob.Credentials = new NetworkCredential("crikkou@apopsi.gr", "83Ruid1987!");

                MailMessage obMsg = new MailMessage();
                obMsg.From = new MailAddress("crikkou@apopsi.gr", "Σύστημα Διαχείρισης Ηλεκτροφωτισμού (Do not reply)");
                obMsg.To.Add(new MailAddress("crikkou@apopsi.gr", "Διαχειριστής συστήματος διαχείρισης ηλεκτροφωτισμού"));
                obMsg.Subject = "Ενημέρωση για την εργασία "+_id+".";
                obMsg.IsBodyHtml = true;
                obMsg.Body = emailBody;
                ob.Send(obMsg);
            }
            catch (Exception ex)
            {
                string msg = ex.ToString();
            }
            // Send email report to system administrator
        }

        static string getPillarName(string id)
        {
            string pillarName = string.Empty;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                conn.Open();

                string sqlSelectName = "SELECT [Name] FROM [db_knx].[dbo].[Pillars] WHERE [ID] = " + id + ";";

                using (SqlCommand sqlComm = new SqlCommand(sqlSelectName, conn))
                {
                    SqlDataReader _dt = sqlComm.ExecuteReader();

                    while (_dt.Read())
                    {
                        pillarName = _dt["Name"].ToString();
                    }

                    _dt.Close();
                }

                conn.Close();
            }

            return pillarName;
        }

        static void disableRunningZombieTasks()
        {
            using (TaskService ts = new TaskService())
            {
                RunningTaskCollection runningTasks = ts.GetRunningTasks();
                TaskFolder KNXFolder = ts.GetFolder("KNX");

                for (var i = 0; i < runningTasks.Count; i++)
                {
                    if (runningTasks[i].Folder.Path == "\\KNX")
                    {
                        if (runningTasks[i].NextRunTime < DateTime.Now)
                        {
                            runningTasks[i].Enabled = false;
                            runningTasks[i].Stop();
                        }
                    }
                }
            }
        }

        static void Main(string[] args)
        {
            switch (args[1])
            {
                case "On":
                    disableRunningZombieTasks();
                    switchON(args[0], args[2]);
                    break;
                case "Off":
                    disableRunningZombieTasks();
                    switchOFF(args[0], args[2]);
                    break;
                case "Meter":
                    disableRunningZombieTasks();
                    meterCurrent(args[0], args[2]);
                    break;
            }
        }
    }
}
