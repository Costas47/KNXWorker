using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Knx.Bus.Common.Configuration;
using Knx.Bus.Common;
using Knx.Bus.Common.GroupValues;
using Knx.Bus.Common.Exceptions;
using Knx.Falcon.Sdk;
using KNXWorker.DPT;

namespace KNXWorker
{
    class Program
    {
        static void switchON(string _taskID, string _dtable)
        {
            string _channelGroup = string.Empty;
            string _PillarID = string.Empty;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [ChannelGroup],[PillarID] FROM [db_knx].[dbo].[Scheduler] WHERE [ID] = " + _taskID + ";";

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

            using (Bus _bus = new Bus(new KnxIpTunnelingConnectorParameters(AddressIP, ushort.Parse(AddressPort), AddressNat)))
            { 
                _bus.Connect();

                for (var i = 0; i < GroupAddressON.Count; i++) {
                    GroupAddress _groupAddress = GroupAddress.Parse(GroupAddressON[i]);
                    GroupValue _groupValue = new GroupValue(true);
                    _bus.WriteValue(_groupAddress, _groupValue, Priority.Low);

                    GroupAddress _groupAddressMeter = GroupAddress.Parse(GroupAddressMeter[i]);
                    GroupValue groupValue = _bus.ReadValue(_groupAddressMeter, Priority.Low);

                    DataPointTranslator _dpt = new DataPointTranslator();
                    decimal _value = (decimal)_dpt.FromDataPoint("9.001", groupValue.Value);
                    storeValueToDB(_value, MeterSerial, ChannelsArray[i]);
                }

                _bus.Disconnect();
            }

        }

        static void switchOFF(string _taskID, string _dtable)
        {
            string _channelGroup = string.Empty;
            string _PillarID = string.Empty;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [ChannelGroup],[PillarID] FROM [db_knx].[dbo].[Scheduler] WHERE [ID] = " + _taskID + ";";

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
            string TypeKNX = string.Empty; string MeterSerial = string.Empty;
            List<string> GroupAddressMeter = new List<string>(); List<string> GroupAddressOFF = new List<string>();

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
                            GroupAddressOFF.Add(_dt["On/Off Address"].ToString());
                            GroupAddressMeter.Add(_dt["MeasureCurrent Address"].ToString());
                        }

                        _dt.Close();
                    }
                }

                conn.Close();
            }

            using (Bus _bus = new Bus(new KnxIpTunnelingConnectorParameters(AddressIP, ushort.Parse(AddressPort), AddressNat)))
            {
                _bus.Connect();

                for (var i = 0; i < GroupAddressOFF.Count; i++)
                {
                    GroupAddress _groupAddressMeter = GroupAddress.Parse(GroupAddressMeter[i]);
                    GroupValue groupValue = _bus.ReadValue(_groupAddressMeter, Priority.Low);

                    DataPointTranslator _dpt = new DataPointTranslator();
                    decimal _value = (decimal)_dpt.FromDataPoint("9.001", groupValue.Value);
                    storeValueToDB(_value, MeterSerial, ChannelsArray[i]);

                    GroupAddress _groupAddress = GroupAddress.Parse(GroupAddressOFF[i]);
                    GroupValue _groupValue = new GroupValue(false);
                    _bus.WriteValue(_groupAddress, _groupValue, Priority.Low);
                }

                _bus.Disconnect();
                _bus.Dispose();
            }
        }

        static void meterCurrent(string _taskID, string _dtable)
        {
            string _channelGroup = string.Empty;
            string _PillarID = string.Empty;

            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString))
            {
                string sqlSelect = "SELECT [ChannelGroup],[PillarID] FROM [db_knx].[dbo].["+_dtable+"] WHERE [ID] = " + _taskID + ";";

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

            using (Bus _bus = new Bus(new KnxIpTunnelingConnectorParameters(AddressIP, ushort.Parse(AddressPort), AddressNat)))
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
                            storeValueToDB(_value, MeterSerial, ChannelsArray[i]);
                        }
                        catch (Exception exception)
                        {
                            Console.WriteLine(exception);
                        }
                    }
                }

                _bus.Disconnect();
                _bus.Dispose();
            }
        }

        static void storeValueToDB(decimal _value, string _meterSerial, string _channelID)
        {
            bool MeasurementTableExists = checkMeasurementTable(_meterSerial);

            if(MeasurementTableExists == false)
            {
                createMeasurementTable(_meterSerial);
            }

            insertValueToDB(_value, _meterSerial, _channelID);
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

        static void insertValueToDB(decimal _value, string _meterSerial, string _channelID)
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

        static void Main(string[] args)
        {
            switch (args[1])
            {
                case "On":
                    switchON(args[0], args[2]);
                    break;
                case "Off":
                    switchOFF(args[0], args[2]);
                    break;
                case "Meter":
                    meterCurrent(args[0], args[2]);
                    break;
            }
        }
    }
}
