using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace GGRCSMScheduler
{


    class Program
    {
        public static MySqlConnection conn;
        public static string Address = "Server=wrdb.c4pqfsdaiccz.us-east-1.rds.amazonaws.com;Port=3306;DataBase=wrserver1;" +
            "Uid=weathermaster;Pwd=neon04$WR1;charset=utf8;Allow Zero DateTime=true";
        static ArrayList arrActivityLog = new ArrayList();
        static int AppID = 23;
        public static System.Threading.Mutex mutex = new Mutex(true, "FarmSMScheduler.exe");
        double totalwater = 25;
        static void Main(string[] args)
        {
            Program objProg = new Program();
            bool createdNew;
            Program objProgram = new Program();
            if (mutex.WaitOne(TimeSpan.Zero, true))
            {
                createdNew = true;

            }
            else
                createdNew = false;

            if (createdNew)
            {
                //  arrActivityLog.Add("Started at " + DateTime.Now);
                Console.WriteLine("GGRC SMS Scheduler");
                Console.WriteLine("Start at ... " + DateTime.Now);
                arrActivityLog.Add("Started at " + DateTime.Now);

                // if (DateTime.Now.Hour < 10 || DateTime.Now.Hour > 20)
                //     return;
                // objProg.addToCMLog();

                //objProg.SMSSchedulerMain();
                //objProg.SMSSchedulerMain_GGRC();
                //   arrActivityLog.Add("Finished at " + DateTime.Now);
                // objProg.addToCMLog();
                // objProg.updAppLastRun();
                // objProg.SMSSchedulerMain_GGRC();
                //  objProg.SMSSend();
                // objProg.SMSSendApprovedMessage();
                //objProg.SMSSchedulerMain_Crop();
                //objProg.Fill_VillageID_IN_YfiGGC();
                objProg.SMSDabwaliFarms_Crop();
                // objProg.SMSScheduler_FarmerWise();
            }
            else
            {
                Console.WriteLine("Program is already running");
            }
        }

        void SMSScheduler_FarmerWise()
        {
            DataTable DTPOPMapping = getData("select * from mfi.pop_scheduler_stagemapping");
            DataTable DTPOPStages = new DataTable();
            DataTable DTPOPWork = new DataTable();
            DTPOPStages = getData("select * from mfi.pop_stages where CropID = 12");
            DTPOPWork = getData("select * from mfi.pop_work where StageID in (select ID from mfi.pop_stages where CropID = 12)");

            DataTable DTAllFarmers = getDabwaliFarms("100894", "");
            for (int i = 0; i < DTAllFarmers.Rows.Count; i++)
            {
                DateTime SowingDate = new DateTime();
                SowingDate = Convert.ToDateTime(DTAllFarmers.Rows[i]["sowingdate"].ToString());
                string FarmID = DTAllFarmers.Rows[i]["FarmID"].ToString();
                Console.WriteLine("Starting for " + (i + 1) + "/" + DTAllFarmers.Rows.Count);

                if (SowingDate.Year == 1)
                    continue;

                for (int j = 0; j < DTPOPStages.Rows.Count; j++)
                {
                    int StageID = DTPOPStages.Rows[j]["ID"].ToString().intTP();
                    string StageName = DTPOPStages.Rows[j]["StageName"].ToString();
                    int DayFrom = DTPOPStages.Rows[j]["DayFrom"].ToString().intTP();
                    int DayTo = DTPOPStages.Rows[j]["DayTo"].ToString().intTP();
                    DateTime dtStageFrom = SowingDate.AddDays(DayFrom);
                    DateTime dtStageTo = SowingDate.AddDays(DayTo);
                    if (!(DateTime.Now >= dtStageFrom && DateTime.Now <= dtStageTo))
                        continue;

                    var DRStageWork = DTPOPWork.Select("StageID = " + StageID);

                    foreach (var CurWork in DRStageWork)
                    {
                        string WorkName = CurWork["WorkName"].ToString();
                        string Message = CurWork["Message"].ToString();
                        string Work = CurWork["Work"].ToString();
                        var DRTypeID = DTPOPMapping.Select("POPStage = '" + WorkName + "'");


                        if (DRTypeID.Count() == 0 || Message == "")
                            continue;
                        DataTable DTExist = getData("select * from mfi.farm_sms_status_master where FarmID='" + FarmID + "' and Message='" + Message + "'");

                        if (DTExist.Rows.Count > 0)
                            continue;
                        execQuery("insert into mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate,MsgStatus, Message, MessageType) values ('" + FarmID + "', '" + dtStageFrom.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + Message + "', '" + StageName + "-" + Work + "')");
                        Console.WriteLine("Added " + StageName + "-" + Work);
                    }
                }
            }
        }


        public void Fill_VillageID_IN_YfiGGC()
        {
            DataTable dtvillage = getYfiGGRCData();

            for (int i = 0; i < dtvillage.Rows.Count; i++)
            {
                Update_YfiGGRCData_ByID(dtvillage.Rows[i]["ID"].ToString(), dtvillage.Rows[i]["District"].ToString(), dtvillage.Rows[i]["Village"].ToString());
                Console.WriteLine(i);
            }

        }

        public string ConDateTime(DateTime Dt)
        {
            string year;
            string month;
            string Day;
            string hr;
            string mn;
            string sec;

            string FinalDt;

            year = Dt.Year.ToString();
            month = Dt.Month.ToString(); if (month.Length < 2) { month = "0" + month; }
            Day = Dt.Day.ToString(); if (Day.Length < 2) { Day = "0" + Day; }
            hr = Dt.Hour.ToString(); if (hr.Length < 2) { hr = "0" + hr; }
            mn = Dt.Minute.ToString(); if (mn.Length < 2) { mn = "0" + mn; }
            sec = Dt.Second.ToString(); if (sec.Length < 2) { sec = "0" + sec; }
            FinalDt = year + "-" + month + "-" + Day + " " + hr + ":" + mn + ":" + sec;

            return FinalDt;


        }

        public bool addToCMLog()
        {
            conn = new MySqlConnection(Address);
            conn.Open();

            string LogDate = ConDateTime(DateTime.Now);
            string sql = "";
            string sqlhead = "insert into appManager.cm_log (AppID, Priority, Status, LogDate) values ";
            string sqlbody = "";
            try
            {
                for (int i = 0; i < arrActivityLog.Count; i++)
                {
                    if (sqlbody != "")
                        sqlbody = sqlbody + ", ";
                    string Status = arrActivityLog[i].ToString();
                    string Priority = "Normal";
                    if (Status.Contains("(ERR)"))
                    {
                        Priority = "Error";
                        Status = Status.Replace("(ERR)", "");
                    }
                    else if (Status.Contains("(WAR)"))
                    {
                        Priority = "Warning";
                        Status = Status.Replace("(WAR)", "");
                    }
                    if (Status.Contains("'"))
                        Status = Status.Replace("'", "''");
                    sqlbody = sqlbody + "(" + AppID + ", '" + Priority + "', '" + Status + "', '" + LogDate + "')";
                }
                sql = sqlhead + sqlbody;
            }
            catch (Exception ex)
            {

            }

            try
            {
                MySqlCommand Cmd = new MySqlCommand(sql, conn);
                if (Cmd.ExecuteNonQuery() > 0)
                {
                    conn.Close();
                    return true;
                }
                else
                {
                    conn.Close();
                    return false;
                }

            }
            catch (Exception e)
            {
                conn.Close();
                return false;
            }

        }
        public bool updAppLastRun()
        { // Last App running time
            //  MySqlConnection conn = (MYINGEN.DBEngine()).getconn();
            string sql = "update appManager.cm_apps set LastRanAt = '" + ConDateTime(DateTime.Now) + "' where ID = " + AppID;
            try
            {

                //  MySqlCommand Cmd = new MySqlCommand(sql, conn);
                execQuery(sql);
                return true;

            }
            catch (Exception e)
            {
                //conn.Close();
                return false;
            }

        }

        public bool Connection()
        {
            bool retval = false;
            conn = new MySqlConnection(Address);
            int ctr = 0;
            while (retval == false && ctr <= 5)
            {
                try
                {
                    if (conn.State == ConnectionState.Closed) conn.Open();
                    retval = true;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    arrActivityLog.Add("(ERR)Connection error");
                    Console.WriteLine("Connection error");
                    ctr = ctr + 1;
                    retval = false;
                }
            }
            return retval;
        }

        public DataTable getGGRCMoisture(string VillageID, DateTime dtFrom, DateTime dtTo)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();
            try
            {
                if (ConnFound)
                {
                    string sql = "select * from test.sentinel_village_soil where Village_ID = " + VillageID +
                     " and Start_Date >= '" + dtFrom.ToString("yyyy-MM-dd") + "' and Start_Date <= '" + dtTo.ToString("yyyy-MM-dd") + "'";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }

        public DataTable getGGRCNDVI(string VillageID, DateTime dtFrom, DateTime dtTo)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();
            try
            {
                if (ConnFound)
                {
                    string sql = "select * from test.sentinel_farm_values where Farm_id = " + VillageID +
                     " and Start_Date >= '" + dtFrom.ToString("yyyy-MM-dd") + "' and Start_Date <= '" + dtTo.ToString("yyyy-MM-dd") + "' and ndvi_value!='9999999'";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }


        public bool chkVillageProcessed(string VillageID, string MessageType)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();

            try
            {
                if (ConnFound)
                {
                    string sql = "select LogDate from mfi.sms_lastsend where VillageID = " + VillageID + " and MessageType = '" + MessageType + "'";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return false;
                    }
                    DT = DataSet.Tables[0];
                    conn.Close();
                    DateTime dtLogDate = new DateTime();
                    DateTime.TryParse(DT.Rows[0]["LogDate"].ToString(), out dtLogDate);
                    if (dtLogDate.Date == DateTime.Now.Date)
                        return true;
                    else
                        return false;

                }
                else
                {
                    return false;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return false;
            }

        }

        public DataTable getVillageLastMessage(string VillageID, string MessageType)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();

            try
            {
                if (ConnFound)
                {
                    string sql = "select * from mfi.ggrcvillagesms where VillageID = " + VillageID + " and MessageType = '" + MessageType + "' and Status != 'Expire' ";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;
                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }
        public DataTable getData(string sql)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();


            try
            {
                if (ConnFound)
                {


                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }


        public DataTable getYfiGGRCData()
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();


            try
            {
                if (ConnFound)
                {
                    string sql = " select * from wrserver1.yfi_ggrc where villageId is null order by Id ";


                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }

        public DataTable Update_YfiGGRCData_ByID(string Id, string District, string village)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();


            try
            {
                if (ConnFound)
                {
                    string sql = " select * from test.sentinel_village_master where District='" + District + "' and Village_Final='" + village + "' ";


                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    sql = "update wrserver1.yfi_ggrc set VillageID='" + DataSet.Tables[0].Rows[0]["Village_ID"] + "' where ID='" + Id + "';";
                    execQuery(sql);

                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }
        public DataTable getSMSMaster_SoilLab(string Client)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();


            try
            {
                if (ConnFound)
                {
                    string sql = "select * from mfi.ggrcsmsmaster where 1 ";

                    sql += "and MessageTypeName = 'Soil lab'";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }


        public DataTable getSMSMaster(string Client)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();
            string sql = "";

            try
            {
                if (ConnFound)
                {
                    sql = "select * from mfi.ggrcsmsmaster where (SendingDate is null or SendingDate>DATE_ADD(now(),INTERVAL -5 DAY)) and (CLient is null or Client = '' ";
                    if (Client != "")
                        sql += " or Client ='" + Client + "') ";
                    sql += " order by Priority";


                    //if (Client == "ggrc")
                    //    sql = "select * from mfi.ggrcsmsmaster where (SendingDate is null or SendingDate>DATE_ADD(now(),INTERVAL -5 DAY)) and Client ='ggrc' order by Priority ";


                    //string sql = "select * from mfi.ggrcsmsmaster where CLient='"+Client+"'";
                    //string sql = "select * from mfi.ggrcsmsmaster where CLient='test'";
                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }

        public DataTable getGGRCVillages()
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();


            try
            {
                if (ConnFound)
                {
                    //string sql = "select ggrc.*,Date_format(sen.SowingDate,'%Y-%m-%d') SowingDateValue,(ymin+ymax)/2 Latitude, (xmin+xmax)/2 Longitude,CropID from wrserver1.yfi_ggrc ggrc left join test.sentinel_village_master sen on ggrc.VillageID = sen.Village_ID";
                    string sql = "select Village,WRMS_StateID,Village_ID,Date_format(sen.SowingDate,'%Y-%m-%d') SowingDateValue,(ymin+ymax)/2 Latitude, (xmin+xmax)/2 Longitude,CropID from  test.sentinel_village_master sen where sen.Village_ID in (1,2) ";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    Adpter.SelectCommand.CommandTimeout = 1800;
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }


        public DataTable getSentinelVillages(string CLients, string NotClients)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();


            try
            {
                if (ConnFound)
                {
                    string sql = "select VIllage_ID ID, VIllage_Final Name,(ymin+ymax)/2 Latitude, (xmin+xmax)/2 Longitude, Client, District_ID from test.sentinel_village_master where 1 ";

                    if (CLients != "")
                        sql += " and Client in (" + CLients + ")";
                    if (NotClients != "")
                        sql += " and Client not in (" + NotClients + ")";


                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }

        }

        public string getGGRCForecastSMS(double Latitude, double Longitude, string Village, string Client, string prefforcastlanguage, out string rainalert, string ID,out string rainalrt)
        {
            string result = "";
            rainalert = "";
            rainalrt = "no";
            string apiAddr = "";
            try
            {
                WebClient wc = new WebClient();
                wc.Encoding = Encoding.UTF8;
                //  string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.AddDays(1).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(3).ToString("yyyy-MM-dd") + "/District/json/internal";
                // string newapi = "http://54.174.231.79:82/wdrest.svc/WZDailyForecast_v2/2018-06-26/2018-06-27/25.4670/91.3662/English/json/wrinternal";
                
                if (prefforcastlanguage.ToLower() == "hindi")
                    apiAddr = "http://54.174.231.79:82/wdrest.svc/MergeWZDailyForecast_v3/" + DateTime.Now.AddDays(1).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(3).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/Hindi/json/wrinternal";
                else
                    apiAddr = "http://54.174.231.79:82/wdrest.svc/MergeWZDailyForecast_v3/" + DateTime.Now.AddDays(1).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(3).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/English/json/wrinternal";

                string strForecast = wc.DownloadString(apiAddr);

                //string date = DateTime.Now.Date.ToString("yyyy-MM-dd");

                //string filename = "File_"+ date +"_Lat" +Latitude+"_Lon"+Longitude+ "_FarmID"+ID+"_.txt";
                //if (!File.Exists(@"D:\work\Apps\WinApps\Forecast\" + filename + ""))
                //{
                //    System.IO.FileStream f = System.IO.File.Create(@"D:\work\Apps\WinApps\Forecast\" + filename + "");
                //    f.Close();
                //    //File.Create(@"D:\Arun_Agnhotri\SMSLog\" + filename + "");

                //}

                //     File.AppendAllText(@"D:\work\Apps\WinApps\Forecast\" + filename + "", "Date ===>" + DateTime.Now + "==>Data==>" + strForecast + Environment.NewLine);





                // File.AppendAllText(@"D:\Arun_Agnhotri\SMSLog\SMSLog.txt","Date ===>"+ DateTime.Now + "==>Data==>" +strForecast+ Environment.NewLine);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");
                DataTable DTForecastFUll = new DataTable();
                DataTable DTForecast = new DataTable();
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                if (objFOrecast != "no data")
                {
                    DTForecastFUll = JsonConvert.DeserializeObject<DataTable>(objFOrecast);
                    double MaxTemp_Max = 0;
                    double MaxTemp_Min = 0;
                    double MinTemp_Max = 0;
                    double MinTemp_Min = 0;
                    double Humidity_Max = 0;
                    double Humidity_Min = 0;
                    double Rain_High = 0;
                    double Rain_Low = 0;

                   

                    MaxTemp_Max = DTForecastFUll.Rows[0]["MaxTemp_High"].ToString().doubleTP();
                    MaxTemp_Min = DTForecastFUll.Rows[0]["MaxTemp_Low"].ToString().doubleTP();
                    MinTemp_Max = DTForecastFUll.Rows[0]["MinTemp_High"].ToString().doubleTP();
                    MinTemp_Min = DTForecastFUll.Rows[0]["MinTemp_Low"].ToString().doubleTP();
                    Rain_High = DTForecastFUll.Rows[0]["Rain_High"].ToString().doubleTP();
                    Rain_Low = DTForecastFUll.Rows[0]["Rain_Low"].ToString().doubleTP();
                    Humidity_Max = DTForecastFUll.Rows[0]["HumMor_High"].ToString().doubleTP();
                    Humidity_Min = DTForecastFUll.Rows[0]["HumEve_Low"].ToString().doubleTP();
                    rainalert = DTForecastFUll.Rows[0]["RainAlert"].ToString();
                    if (Rain_High >0 || Rain_Low >0)
                        rainalrt = "yes";
                    //for (int i = 0; i < DTForecastFUll.Rows.Count; i++)
                    //{
                    //    if (DTForecastFUll.Rows[i]["MaxTemp"].ToString() != "")
                    //    {
                    //        double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                    //        if (MaxTemp > MaxTemp_Max)
                    //            MaxTemp_Max = MaxTemp;
                    //        if (MaxTemp < MaxTemp_Min)
                    //            MaxTemp_Min = MaxTemp;

                        //    }

                        //    if (DTForecastFUll.Rows[i]["MinTemp"].ToString() != "")
                        //    {
                        //        double MinTemp = DTForecastFUll.Rows[i]["MinTemp"].ToString().doubleTP();
                        //        if (MinTemp > MinTemp_Max)
                        //            MinTemp_Max = MinTemp;
                        //        if (MinTemp < MinTemp_Min)
                        //            MinTemp_Min = MinTemp;

                        //    }

                        //    if (DTForecastFUll.Rows[i]["Humidity"].ToString() != "")
                        //    {
                        //        double Humidity = DTForecastFUll.Rows[i]["Humidity"].ToString().doubleTP();
                        //        if (Humidity > Humidity_Max)
                        //            Humidity_Max = Humidity;
                        //        if (Humidity < Humidity_Min)
                        //            Humidity_Min = Humidity;

                        //    }

                        //    TotRain = TotRain + DTForecastFUll.Rows[i]["Rain"].ToString().doubleTP();

                        //}
                        //if (Client == "ggrc")
                        //    result = "?????? ?????: ????? ? ???? ???? ?????? ?????? " + MaxTemp_Min + " - " +
                        //             MaxTemp_Max + " ??. ??????? ?????? " + MinTemp_Min + " - " + MinTemp_Max + " ??. ?????? (????? ??? ) " + Humidity_Min + " - " + Humidity_Max +
                        //             "%, ?????- " + TotRain + " ??.??.";
                    if (rainalert != "" && prefforcastlanguage.ToLower() != "gujrati")
                        rainalert = " में" + " " + rainalert;
                    if (prefforcastlanguage.ToLower() == "hindi")
                        result = " में अगले 3 दिनों का मौसम: अधिकतम तापमान " + MaxTemp_Min + " - " +
                             MaxTemp_Max + " C, न्यूनतम तापमान " + MinTemp_Min + " - " + MinTemp_Max + " C, वर्षा " + Rain_Low + " - " + Rain_High + " मिमी, आद्रता " + Humidity_Min + " - " + Humidity_Max + "%";

                    else if (prefforcastlanguage.ToLower() == "gujrati")

                        result = " હવામાન આગાહી: આગામી ૩ દિવસ માટે મહત્તમ તાપમાન " + MaxTemp_Min + " - " +
                            MaxTemp_Max + " સે. લઘુત્તમ તાપમાન " + MinTemp_Min + " - " + MinTemp_Max + " સે. આદ્રતા (હવાનો ભેજ ) " + Humidity_Min + " - " + Humidity_Max +
                            "%, વરસાદ- " + Rain_Low + " - " + Rain_High + " મી.મી.";

                    else if (prefforcastlanguage.ToLower() == "english")
                        result = " Weather Forecast: Next 3 days MaxTemp " + MaxTemp_Min + " - " +
                             MaxTemp_Max + " C MinTemp " + MinTemp_Min + " - " + MinTemp_Max + " C Rain " + Rain_Low + " - " + Rain_High + " mm Humidity " + Humidity_Min + " - " + Humidity_Max;

                    else if (prefforcastlanguage.ToLower() == "bengali")

                        result = "  আবহাওয়ার পূর্বাভাস: পরবর্তী 3 দিনের সর্বোচ্চ তাপমাত্রা " + MaxTemp_Min + " - " + MaxTemp_Min + " C, সর্বনিম্ন তাপমাত্রা " + MinTemp_Min + " - " + MinTemp_Max + " C, বৃষ্টিপাত " + Rain_Low + " - " + Rain_High + " MM,  আর্দ্রতা " + Humidity_Min + " - " + Humidity_Max + "";
                }
                return result;
            }
            catch (Exception ex)
            {
                
                    SendMailForAlert(apiAddr+"-"+ex.Message);
                return result;
            }
        }


        public void SendMailForAlert(string msgbody)
        {
            try
            {
                using (System.Net.Mail.MailMessage message = new System.Net.Mail.MailMessage("system@weather-risk.com", "mehdi.abbas@theingen.com "))
                {
                    
                    message.CC.Add("md.ayaz@theingen.com");
                    message.CC.Add("md.javed@theingen.com");
                    message.CC.Add("agnihothriarun730@gmail.com");
                    message.CC.Add("ranjeet.giri@theingen.com");
                    message.CC.Add("dhirendra.singh@theingen.com");
                    message.IsBodyHtml = true;
                    message.Subject = "Forecast Data Not Found at this lat lon please rectify it";
                    string body = msgbody;
                    message.Body = body;
                    using (System.Net.Mail.SmtpClient smtp = new System.Net.Mail.SmtpClient("smtp.gmail.com", 587))
                    {
                        // smtp.Port = 25;
                        smtp.UseDefaultCredentials = false;
                        smtp.EnableSsl = true;
                        // smtp.DeliveryMethod = SmtpDeliveryMethod.Network;
                        smtp.Credentials = new NetworkCredential("system@weather-risk.com", "weather258cold");
                        smtp.Send(message);
                    }
                }
            }
            catch (Exception ex)
            {
                //Console.WriteLine(ex.Message + "\n " + ex.StackTrace);
            }

        }










        public string getGGRCSoilAnalysis(string VillageID, double Latitude, double Longitude, string SoilFactorType)
        {
            string result = "";
            try
            {
                WebClient wc = new WebClient();
                string apiAddr = "http://myfarminfo.com//yfirest.svc/Soil/Info/" + Latitude + "/" + Longitude;
                string strForecast = wc.DownloadString(apiAddr);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");
                DataTableAndColDesc DTForecastFUll = new DataTableAndColDesc();
                DataTable DTPH = new DataTable();
                DataTable DTSandyTexture = new DataTable();
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                if (objFOrecast != "no data")
                {
                    DTForecastFUll = JsonConvert.DeserializeObject<DataTableAndColDesc>(objFOrecast);
                }
                DTPH = DTForecastFUll.DTChartDesc;
                DTSandyTexture = DTForecastFUll.DT;
                if (SoilFactorType == "PH")
                {
                    result = DTPH.Rows[0]["PH"].ToString();
                }
                else if (SoilFactorType == "SoilTexture")
                {
                    result = DTSandyTexture.Rows[0]["Name"].ToString();
                }


                return result;
            }
            catch (Exception ex)
            {
                return result;
            }
        }

        public string getGGRCMoistureSMS(string VillageID, string Village, string Client)
        {
            DateTime dtFrom = new DateTime();
            DateTime dtTo = new DateTime();

            string result = "";
            DataTable DTMoisture = getGGRCMoisture(VillageID, dtFrom, dtTo);

            for (int i = 0; i < DTMoisture.Rows.Count; i++)
            {

                string strSM = DTMoisture.Rows[i]["Village_soilmean"].ToString();
                double SM = 0;
                double.TryParse(strSM, out SM);
                if (SM < -0.0125)
                {
                    if (Client == "ggrc")
                        result = " ??????????? ??? ???? ??????? ???  ????? ???????? ????? ?????? ???? ??? ??? ?? ?????????  1-2%  ??? ?????? ??????? ?? ?????? ?????? ???? ??????? ??? ??.";
                    else
                        result = "Foliar application of Urea @ 1-2 % is advisable if leaves are yellowing due to nitrogen deficiency or in low Soil moisture condition";
                }
            }

            return result;
        }

        void SMSSchedulerMain()
        {
            execQuery("delete from mfi.ggrcvillagesms where LogDate < '" + DateTime.Now.AddDays(-14).ToString("yyyy-MM-dd") + "'");
            execQuery("delete from mfi.sms_lastsend where date(LogDate) < '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");

            DataTable DTVillages = new DataTable();
            DTVillages = getSentinelVillages("", "'ggrc','pepsico','pepsico_old'");

            DataTable DTSMSTypes = getSMSMaster("jalna");


            for (int i = 0; i < DTVillages.Rows.Count; i++)
            {
                string VillageID = DTVillages.Rows[i]["ID"].ToString();
                string Village = DTVillages.Rows[i]["Name"].ToString();
                string DistrictID = DTVillages.Rows[i]["District_ID"].ToString();
                string Client = DTVillages.Rows[i]["Client"].ToString();
                Console.WriteLine("Starting for " + Village + "(" + i + " / " + DTVillages.Rows.Count + ") at " + DateTime.Now);
                double Latitude = DTVillages.Rows[i]["Latitude"].ToString().doubleTP();
                double Longitude = DTVillages.Rows[i]["Longitude"].ToString().doubleTP();

                for (int j = 0; j < DTSMSTypes.Rows.Count; j++)
                {

                    string MessageID = DTSMSTypes.Rows[j]["ID"].ToString();

                    string MessageVillageID = DTSMSTypes.Rows[j]["VillageID"].ToString();
                    string MessageDistrict = DTSMSTypes.Rows[j]["DistrictID"].ToString();

                    string MessageType = DTSMSTypes.Rows[j]["MessageType"].ToString();
                    string SendingType = DTSMSTypes.Rows[j]["SendingType"].ToString();

                    if (MessageDistrict != "" && MessageDistrict != "0" && DistrictID != MessageDistrict)
                        continue;
                    int SendingFrequency = 0;
                    int.TryParse(DTSMSTypes.Rows[j]["SendingFrequency"].ToString(), out SendingFrequency);
                    DateTime SendingDate = new DateTime();
                    DateTime.TryParse(DTSMSTypes.Rows[j]["SendingDate"].ToString(), out SendingDate);
                    DateTime dtLastSentDate = new DateTime();
                    string CustomMessage = DTSMSTypes.Rows[j]["Message"].ToString();
                    string Status = "";
                    string SMS = "";
                    string SMS2 = "";
                    if (chkVillageProcessed(VillageID, MessageType))
                        continue;

                    DataTable DTLastMessage = getVillageLastMessage(VillageID, MessageType);

                    if (DTLastMessage.Rows.Count > 0)
                    {
                        DateTime.TryParse(DTLastMessage.Rows[0]["ScheduleDate"].ToString(), out dtLastSentDate);
                        Status = DTLastMessage.Rows[0]["Status"].ToString();
                    }
                    string ss;
                    bool flgDoProcess = false;

                    if (SendingType == "Day")
                        flgDoProcess = (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency)));
                    else
                        flgDoProcess = (DateTime.Now >= SendingDate && Status == "" && SendingDate.AddDays(4) > DateTime.Now);

                    if (!flgDoProcess)
                        continue;

                    if (MessageType == "forecast")
                    {
                        SMS = getGGRCForecastSMS(Latitude, Longitude, Village, Client, "", out SMS2, "",out ss);
                    }
                    else if (MessageType == "weather")
                    {
                        SMS = getGGRCWeatherDataSMS(Latitude, Longitude, Village, Client, "");
                    }
                    else if (MessageType == "disease")
                    {
                        List<string> lstSMS = getGGRCDiseaseSMS(Latitude, Longitude, Village, Client);
                        if (lstSMS.Count > 0)
                        {
                            SMS = lstSMS[0];
                            SMS2 = lstSMS[1];
                        }
                    }
                    else if (MessageType == "disease2")
                    {
                        if (chkLateBlightCondition(Latitude, Longitude, Village, Client))
                        {
                            SMS = CustomMessage;
                        }
                    }
                    else if (MessageType == "planthealth")
                    {
                        SMS = getGGRCMoistureSMS(VillageID, Village, Client);
                    }
                    else if (MessageType == "raindeficit")
                    {
                        if (Client == "ggrc")
                            SMS = "????? ????? ???????????? ?????? ?????? ??????? ????? ????? ?? ?? ???? ??????? ??? (???? ???- ???? ?? ??? ?? ?? ?? ????? ????) ?? ??? ???? ?????????? ??.";
                        else
                            SMS = "Repeated hoeing is advisable to conserve the soil moisture under scanty rainfall conditions.";
                    }
                    else if (MessageType == "leafreddening")
                    {
                        if (Client == "ggrc")
                            SMS = "??? ???? ??? ?????? ??? ????? ???? ?.?% ?????????? ?????? ?????? ??? ?? ???? ??? ??? ????.";
                        else
                            SMS = "Application of 0.2 % Magnesium sulphate at square formation and boll formation to reduce leaf reddening.";
                    }
                    else if (MessageType == "flower1" || MessageType == "flower2" || MessageType == "flower3")
                    {
                        if (Client == "ggrc")
                            SMS = " ?????? ??? ????, ??? ?????? ????????? ?????? ???????? ???????? 2 ????????? / 100 ???? ??????  ??????? ?? ?????? ??????? ?????.";
                        else
                            SMS = "For yield maximization, foliar application of Potassium Nitrate @ 2kg/100 lit of water at flower initiation stage would be benificial.";
                    }
                    else if (MessageType == "weedemergence")
                    {
                        if (Client == "ggrc")
                            SMS = "????? ???????: ???? ??????? ????????? ???? ???? ??? ??? ??????? ???? ????.";
                        else
                            SMS = "Weed Management: Intercultural operations should be carried out to control emerging weeds.";
                    }
                    else if (MessageType == "flowertoball")
                    {
                        if (Client == "ggrc")
                            SMS = "??? ??? ??? ??? ??? ????? : NAA (???????????) ?? ?????? : ???? ??? ??? ?????? ??? ???????? ????? ????? ??? ??? ?????? ???? ??? ?? 15 ???? ???? ???????  3.5 ???? NAA ?? ?????? ????.";
                        else
                            SMS = "Flower/ Bud dropping: Spray Of NAA(Planofix): Spray NAA @ 3.5 ml per 15 litres of water on the crop to retain more number of squares and flowers and to increase the yield.";
                    }
                    else if (MessageType.ToLower().Contains("custom"))
                        SMS = CustomMessage;



                    if (SendingType == "Date" && DateTime.Now >= SendingDate && Status == "")
                    {
                        if (SMS != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType, DistrictID) values ('" + VillageID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "'," + DistrictID + ")");
                        if (SMS2 != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType, DistrictID) values ('" + VillageID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "'," + DistrictID + ")");
                        if (SMS != "" && SMS2 != "")
                            Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    }
                    else if (SendingType == "Day" && (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency))))
                    {
                        execQuery("update mfi.ggrcvillagesms set Status = 'Expired' where VillageID = " + VillageID + " and MessageType = '" + MessageType + "'");


                        if (SMS != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate, Status, Message, MessageType, DistrictID) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "'," + DistrictID + ")");
                        if (SMS2 != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType, DistrictID) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "'," + DistrictID + ")");

                        if (SMS != "" && SMS2 != "")
                            Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    }

                    execQuery("insert into mfi.sms_lastsend (VillageID, LogDate, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + MessageType + "')");

                }
            }
        }

        public List<WrmsAdmin> FillWrmsAdmin()
        {
            List<WrmsAdmin> lstwrmsadmin = new List<WrmsAdmin>();
            DataTable dt = getData("Select * from mfi.wrmsadminMsg where date(ddat)='" + DateTime.Now.ToString("yyyy-dd-MM") + "'");
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                WrmsAdmin wa = new WrmsAdmin();
                DateTime ddt = new DateTime();
                DateTime.TryParse(dt.Rows[i]["ddat"].ToString(), out ddt);
                wa.Msg = dt.Rows[i]["Msg"].ToString();
                wa.PhoneNo = dt.Rows[i]["PhoneNo"].ToString();
                wa.ddat = ddt;
                lstwrmsadmin.Add(wa);
            }
            return lstwrmsadmin;
        }
        void SMSSend()
        {

            DataTable DTVillageSMS = getData(" select sms.* from mfi.ggrcvillagesms sms  left join mfi.ggrcsmsmaster mas on mas.MessageType = sms.MessageType  where mas.messagetypename = 'Land Preparation-Stage-II'");
            int FarmerCnt = 0;
            string insertString = "insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel,  OutDate,ccsid) values ";
            string bodyquery = "";
            for (int i = 0; i < DTVillageSMS.Rows.Count; i++)
            {
                string VillageID = DTVillageSMS.Rows[i]["VillageID"].ToString();
                string Message = DTVillageSMS.Rows[i]["Message"].ToString();
                string MessageType = DTVillageSMS.Rows[i]["MessageType"].ToString();
                string sql = "select far.ID, far.Name, PhoneNo, District, Taluka, Village,far.CLusterID  from wrserver1.yfi_ggrc_farmers far left join yfi_ggrc loc on far.CLusterID = loc.ID where 1 ";

                sql = sql + " and loc.ID = " + VillageID;

                DataTable DTFarmers = getData(sql);


                for (int k = 0; k < DTFarmers.Rows.Count; k++)
                {
                    FarmerCnt++;
                    string PhoneNo = DTFarmers.Rows[k]["PhoneNo"].ToString();
                    if (bodyquery != "")
                        bodyquery += ",";
                    bodyquery += "('GGRCFarmers', '" + PhoneNo + "', 'GGRC', 'GGRC Subject', '" + Message + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss") + "','" + MessageType + "')";

                    if (FarmerCnt % 1000 == 0)
                    {
                        execQuery(insertString + bodyquery);
                        Console.WriteLine("Inserted " + i + "/" + DTVillageSMS.Rows.Count);
                        bodyquery = "";
                    }
                }

                Console.WriteLine(i + "/" + DTVillageSMS.Rows.Count);
            }
            execQuery(insertString + bodyquery);

        }

        void SMSSendApprovedMessage()
        {
            List<WrmsAdmin> lstwrmsadmin = FillWrmsAdmin();
            List<DistricCoordinator> lstdic = new List<DistricCoordinator>();
            lstdic.Add(new DistricCoordinator() { Block = "Vadodara", PhoneNo = "9979853448" });
            lstdic.Add(new DistricCoordinator() { Block = "Vadodara", PhoneNo = "9820578984" });
            lstdic.Add(new DistricCoordinator() { Block = "Vadodara", PhoneNo = "9909986773" });
            lstdic.Add(new DistricCoordinator() { Block = "Vadodara", PhoneNo = "9904072825" });
            lstdic.Add(new DistricCoordinator() { Block = "Vadodara", PhoneNo = "9725001323" });
            lstdic.Add(new DistricCoordinator() { Block = "Vadodara", PhoneNo = "9725151239" });
            lstdic.Add(new DistricCoordinator() { Block = "Rajkot", PhoneNo = "9909099220" });
            lstdic.Add(new DistricCoordinator() { Block = "Amreli", PhoneNo = "9979859661" });
            lstdic.Add(new DistricCoordinator() { Block = "Rajpipla", PhoneNo = "9909971553" });
            lstdic.Add(new DistricCoordinator() { Block = "Jamnagar", PhoneNo = "9909971592" });
            lstdic.Add(new DistricCoordinator() { Block = "Junagadh", PhoneNo = "9909971573" });
            lstdic.Add(new DistricCoordinator() { Block = "Himatnagar", PhoneNo = "9909971883" });
            lstdic.Add(new DistricCoordinator() { Block = "Bharuch", PhoneNo = "9979883565" });

            List<string> lstSupport = new List<string>() { "9794585750", "9711931599", "8800559475" };

            DataTable DTVillageSMS = getData(" select *,ggrc.District mDistrict,sms.ID smsID from mfi.ggrcvillagesms sms  left join mfi.ggrcsmsmaster mas on mas.MessageType = sms.MessageType left join wrserver1.yfi_ggrc ggrc on ggrc.ID=sms.VillageID  where sms.status='Approve'");

            string insertwrmsadmin = "insert into mfi.wrmsadminmsg(Msg, PhoneNo, ddat) values ";
            string bodyquerywrmsadmin = "";
            int FarmerCnt = 0;
            string insertString = "insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel,  OutDate,ccsid) values ";
            string updatestring = "update mfi.ggrcvillagesms set status='Send' where 1 ";
            string updatepart = "";
            string bodyquery = "";
            string smsID = "";
            for (int i = 0; i < DTVillageSMS.Rows.Count; i++)
            {
                if (smsID != "")
                    smsID = ",";
                smsID += "'" + DTVillageSMS.Rows[i]["smsID"].ToString() + "'";
                string VillageID = DTVillageSMS.Rows[i]["VillageID"].ToString();
                string Message = DTVillageSMS.Rows[i]["Message"].ToString();
                string districtId = DTVillageSMS.Rows[i]["mDistrict"].ToString();
                string MessageType = DTVillageSMS.Rows[i]["MessageType"].ToString();
                List<DistricCoordinator> lstdc = lstdic.FindAll(a => a.Block.ToLower() == districtId.ToLower());
                for (int j = 0; j < lstdc.Count; j++)
                {
                    if (lstwrmsadmin.FindAll(a => a.Msg == Message && a.PhoneNo == lstdc[j].PhoneNo).Count == 0)
                    {
                        if (bodyquery != "")
                            bodyquery += ",";
                        bodyquery += "('GGRCFarmers', '" + lstdc[j].PhoneNo + "', 'GGRC', 'GGRC Subject', '" + Message + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + MessageType + "')";
                        lstwrmsadmin.Add(new WrmsAdmin() { Msg = Message, PhoneNo = lstdc[j].PhoneNo, ddat = DateTime.Now });
                        if (bodyquerywrmsadmin != "")
                            bodyquerywrmsadmin += ",";
                        bodyquerywrmsadmin += "( '" + Message + "','" + lstdc[j].PhoneNo + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')";
                    }
                }
                for (int j = 0; j < lstSupport.Count; j++)
                {
                    if (lstwrmsadmin.FindAll(a => a.Msg == Message && a.PhoneNo.ToString() == lstSupport[j]).Count == 0)
                    {
                        if (bodyquery != "")
                            bodyquery += ",";
                        bodyquery += "('GGRCFarmers', '" + lstSupport[j] + "', 'GGRC', 'GGRC Subject', '" + Message + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + MessageType + "')";
                        lstwrmsadmin.Add(new WrmsAdmin() { Msg = Message, PhoneNo = lstSupport[j], ddat = DateTime.Now });
                        if (bodyquerywrmsadmin != "")
                            bodyquerywrmsadmin += ",";
                        bodyquerywrmsadmin += "( '" + Message + "','" + lstSupport[j] + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')";
                    }
                }
                string sql = "select far.ID, far.Name, PhoneNo, District, Taluka, Village,far.CLusterID  from wrserver1.yfi_ggrc_farmers far left join yfi_ggrc loc on far.CLusterID = loc.ID where 1 ";
                sql = sql + " and loc.ID = " + VillageID;
                DataTable DTFarmers = getData(sql);
                for (int k = 0; k < DTFarmers.Rows.Count; k++)
                {
                    FarmerCnt++;
                    string PhoneNo = DTFarmers.Rows[k]["PhoneNo"].ToString();
                    if (bodyquery != "")
                        bodyquery += ",";
                    bodyquery += "('GGRCFarmers', '" + PhoneNo + "', 'GGRC', 'GGRC Subject', '" + Message + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + MessageType + "')";
                    if (FarmerCnt % 1000 == 0)
                    {
                        updatepart = " and ID in (" + smsID + ")";
                        execQuery(updatestring + updatepart);
                        smsID = "";
                        execQuery(insertString + bodyquery);
                        Console.WriteLine("Inserted " + i + "/" + DTVillageSMS.Rows.Count);
                        bodyquery = "";
                    }
                }
                Console.WriteLine(i + "/" + DTVillageSMS.Rows.Count);
            }
            if (smsID != "")
            {
                updatepart = " and ID in (" + smsID + ")";
                execQuery(updatestring + updatepart);
                smsID = "";
            }
            if (bodyquery != "")
                execQuery(insertString + bodyquery);
            if (bodyquerywrmsadmin.Trim() != "")
            {
                execQuery(insertwrmsadmin + bodyquerywrmsadmin);
                bodyquerywrmsadmin = "";
            }
        }


        void SMSSchedulerMain_Crop()
        {
            execQuery("delete from mfi.ggrcvillagesms where LogDate < '" + DateTime.Now.AddDays(-14).ToString("yyyy-MM-dd") + "'");
            execQuery("delete from mfi.sms_lastsend where date(LogDate) < '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");

            DataTable DTVillages = new DataTable();
            DTVillages = getGGRCVillages();
            //string Client1 = "ggrc";
            string stateID = "";
            //if (Client1 == "ggrc")
            //    stateID = "17";
            string Client = "test";

            DataTable DTSMSTypes = getSMSMaster(Client);


            for (int i = 0; i < DTVillages.Rows.Count; i++)
            {
                string SowingDate = DTVillages.Rows[i]["SowingDateValue"].ToString();
                string VillageID = DTVillages.Rows[i]["Village_ID"].ToString();
                string Village = DTVillages.Rows[i]["Village"].ToString();

                // string District = DTVillages.Rows[i]["District"].ToString();
                //string Block = DTVillages.Rows[i]["Taluka"].ToString();
                string cropid = DTVillages.Rows[i]["CropID"].ToString();
                Console.WriteLine("Starting for " + Village + "(" + i + " / " + DTVillages.Rows.Count + ") at " + DateTime.Now);
                double Latitude = DTVillages.Rows[i]["Latitude"].ToString().doubleTP();
                double Longitude = DTVillages.Rows[i]["Longitude"].ToString().doubleTP();
                //if (!(District == "PORBANDAR"))
                //    continue;
                string cropname = "";
                if (cropid != "")
                {
                    try
                    {
                        conn.Open();
                        string getcropname = "SELECT CropName FROM wrserver1.cropmaster where CropID='" + cropid + "'";
                        MySqlCommand command = new MySqlCommand(getcropname, conn);
                        MySqlDataReader rd1 = command.ExecuteReader();
                        if (rd1.HasRows)
                        {
                            while (rd1.Read())
                            {
                                cropname = rd1["CropName"].ToString();
                            }
                        }
                        conn.Close();
                    }
                    catch (Exception ex)
                    {

                    }
                }

                for (int j = 0; j < DTSMSTypes.Rows.Count; j++)
                {


                    string month = "";
                    string MessageType = DTSMSTypes.Rows[j]["MessageType"].ToString();
                    //if (!(MessageType == "Custom_ZY4BBB5RF9JHWPJV"))
                    //    continue;
                    string MessageVillageID = DTSMSTypes.Rows[j]["VillageID"].ToString();
                    string MessageDistrict = DTSMSTypes.Rows[j]["DistrictID"].ToString();
                    string MessageBlock = DTSMSTypes.Rows[j]["BlockID"].ToString();
                    if (!(MessageType.ToLower().Contains("custom")))
                    {
                        if (Latitude == 0)
                            continue;
                    }

                    DateTime DTSowingDate = new DateTime();
                    if (SowingDate != "")
                    {
                        string[] ArrSowingDate = SowingDate.Split('-');
                        month = ArrSowingDate[1];
                        DTSowingDate = new DateTime(Convert.ToInt32(ArrSowingDate[0]), Convert.ToInt32(ArrSowingDate[1]), Convert.ToInt32(ArrSowingDate[2]));
                    }

                    //if (MessageType.ToLower().Contains("custom"))
                    //{
                    //    bool flgMessageSend = false;
                    //    if (VillageID == MessageVillageID || (MessageVillageID == "0" && MessageBlock == Block) || (MessageVillageID == "0" && MessageBlock == "0" && MessageDistrict == District) || (MessageVillageID == "0" && MessageBlock == "0" && MessageDistrict == "0"))
                    //        flgMessageSend = true;

                    //    if (!flgMessageSend)
                    //        continue;
                    //}
                    //else
                    //  continue;
                    string SendingType = DTSMSTypes.Rows[j]["SendingType"].ToString();
                    int SendingFrequency = 0;
                    int.TryParse(DTSMSTypes.Rows[j]["SendingFrequency"].ToString(), out SendingFrequency);
                    DateTime SendingDate = new DateTime();
                    DateTime.TryParse(DTSMSTypes.Rows[j]["SendingDate"].ToString(), out SendingDate);
                    DateTime dtLastSentDate = new DateTime();
                    string CustomMessage = DTSMSTypes.Rows[j]["Message"].ToString();
                    string FloatingDays = DTSMSTypes.Rows[j]["FloatingDays"].ToString();
                    string Status = "";
                    string SMS = "";
                    string SMS2 = "";
                    double totalrain = 0;
                    int floatdays = 0;
                    double EVRate = 0;
                    DateTime onsetdate = new DateTime();
                    DateTime d = DateTime.Now.Date;
                    string smsstatus = "";
                    Boolean flg = false;
                    Boolean step = false;
                    DateTime rundate = new DateTime();

                    if (cropname == "Cotton" && SowingDate != "")
                    {
                        //Check For SmS Status//  
                        DateTime check_date = new DateTime();
                        if (MessageType == "Cotton_Irrigation")
                        {
                            string TableName = "mfi.cotton_cropmessage_village";

                            if (FloatingDays != "")
                            {
                                int.TryParse(FloatingDays, out floatdays);
                            }
                            smsstatus = GetSmsStatus(VillageID, MessageType, TableName);
                            if (smsstatus == "Sent")
                            {
                                continue;
                            }

                            check_date = GetdateOfCrop(VillageID, MessageType, TableName);
                            onsetdate = Convert.ToDateTime(SowingDate).AddDays(floatdays);
                            EVRate = GetMeanEt(month, stateID);
                            totalrain = GetCropRainData(Latitude, Longitude, Village, Client);
                            totalrain = totalrain + EVRate;
                            if (check_date.Year == 1)
                                rundate = onsetdate;
                            if (smsstatus == "step2_1")
                                rundate = check_date.AddDays(3);
                            else if (smsstatus == "step2")
                                rundate = check_date.AddDays(3);
                            //////////////step1 Start                               
                            if (DateTime.Now.Date >= rundate && !smsstatus.Contains("step2"))//if not step2
                            {
                                if (step1(VillageID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, TableName))
                                    continue;

                            }

                            //////////////step1 End

                            if ((DateTime.Now.Date >= rundate && smsstatus.Contains("step2_")))
                                Step2(VillageID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, TableName, totalwater);

                            if (smsstatus == "step2" && DateTime.Now.Date >= rundate)//if satus==2 && DateTime.now.date>=dtchckdate+3
                            {//step3                            
                                double tolrenec = 0;
                                double rain = GetCropRainData(Latitude, Longitude, Village, Client);
                                if (rain + tolrenec > 0)
                                {
                                    if (step1(VillageID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, TableName))
                                        continue;
                                }

                                else if ((DateTime.Now.Date - onsetdate.Date).Days < 7)
                                {
                                    Step2(VillageID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, TableName, totalwater);
                                }
                                else
                                {//step4
                                    if ((DateTime.Now.Date - onsetdate.Date).Days > 7 && (DateTime.Now.Date - onsetdate.Date).Days < 12)
                                    { Step2(VillageID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, TableName, totalwater); }
                                    else
                                    {
                                        double amountofwater = GetRemainamountofwater(cropid, VillageID);
                                        if (amountofwater > totalwater)
                                        { amountofwater = amountofwater - totalwater; }
                                        SMS = "Release '" + amountofwater + "'mm amount of water";
                                        updatecropsmsstatus(VillageID, "step4", MessageType, TableName);
                                    }
                                }
                            }

                        }

                    }



                    if (FloatingDays != "")
                    {
                        int.TryParse(FloatingDays, out floatdays);
                    }





                    //if (chkVillageProcessed(VillageID, MessageType))
                    //    continue;

                    //   DataTable DTLastMessage = getVillageLastMessage(VillageID, MessageType);

                    ////   if (DTLastMessage.Rows.Count > 0)
                    //   {
                    //       DateTime.TryParse(DTLastMessage.Rows[0]["ScheduleDate"].ToString(), out dtLastSentDate);
                    //       Status = DTLastMessage.Rows[0]["Status"].ToString();
                    //   }

                    bool flgDoProcess = false;

                    if (SendingType == "Day")
                        flgDoProcess = (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency)));
                    else if (SendingType == "Floating")
                        flgDoProcess = (DateTime.Now.Date == DTSowingDate.AddDays(floatdays) && Status == "");
                    else
                        flgDoProcess = (DateTime.Now >= SendingDate && SendingDate > DateTime.Now.AddDays(-15) && Status == "");

                    if (!flgDoProcess)
                        continue;
                    if (MessageType == "Spray1")
                        SMS = CustomMessage;
                    if (MessageType == "Spray2")
                        SMS = CustomMessage;
                    if (MessageType == "Spray3")
                        SMS = CustomMessage;
                    if (MessageType == "Spray4")
                        SMS = CustomMessage;
                    if (MessageType == "Spray5")
                        SMS = CustomMessage;
                    if (MessageType == "Spray6")
                        SMS = CustomMessage;
                    string ss = "";
                    if (MessageType == "forecast")
                    {
                        SMS = getGGRCForecastSMS(Latitude, Longitude, Village, Client, "", out SMS2, "",out ss);
                    }
                    if (MessageType == "PHLow")
                    {
                        string Ph = "";
                        Ph = getGGRCSoilAnalysis(Village, Latitude, Longitude, "PH");
                        if (!(Ph.doubleTP() < 5.5))
                            continue;
                        SMS = CustomMessage;

                    }
                    if (MessageType == "PHHigh")
                    {
                        string Ph = "";
                        Ph = getGGRCSoilAnalysis(Village, Latitude, Longitude, "PH");
                        if (!(Ph.doubleTP() > 8.0))
                            continue;
                        SMS = CustomMessage;
                    }
                    if (MessageType == "SoilTexture")
                    {
                        string st = "";
                        st = getGGRCSoilAnalysis(Village, Latitude, Longitude, "SoilTexture");
                        if (!(st.ToLower().Contains("sandy")))
                            continue;
                        SMS = CustomMessage;
                    }
                    //if (MessageType == "NDVI")
                    //{
                    //    DataTable DTNDVI = new DataTable();
                    //    DTNDVI = getGGRCNDVI(VillageID, DTSowingDate.AddDays(90), DTSowingDate.AddDays(120));
                    //    string NDVIValue = DTNDVI.Rows[0][0].ToString().Trim();
                    //    if (NDVIValue == "")
                    //        continue;
                    //    double NDVI = NDVIValue.doubleTP();
                    //    if (!(NDVI < 0.5))
                    //        continue;
                    //    SMS = CustomMessage;
                    //}
                    else if (MessageType == "weather")
                    {
                        SMS = getGGRCWeatherDataSMS(Latitude, Longitude, Village, Client, "");
                    }
                    else if (MessageType == "disease")
                    {
                        List<string> lstSMS = getGGRCDiseaseSMS(Latitude, Longitude, Village, Client);
                        if (lstSMS.Count > 0)
                        {
                            SMS = lstSMS[0];
                            SMS2 = lstSMS[1];
                        }
                    }
                    //else if (MessageType == "planthealth")
                    //{
                    //    SMS = getGGRCMoistureSMS(VillageID, Village, Client);
                    //}
                    else if (MessageType == "raindeficit")
                    {
                        if (Client == "ggrc")
                            SMS = "????? ????? ???????????? ?????? ?????? ??????? ????? ????? ?? ?? ???? ??????? ??? (???? ???- ???? ?? ??? ?? ?? ?? ????? ????) ?? ??? ???? ?????????? ??.";
                        else
                            SMS = "Repeated hoeing is advisable to conserve the soil moisture under scanty rainfall conditions.";
                    }
                    else if (MessageType == "leafreddening")
                    {
                        if (Client == "ggrc")
                            SMS = "??? ???? ??? ?????? ??? ????? ???? ?.?% ?????????? ?????? ?????? ??? ?? ???? ??? ??? ????.";
                        else
                            SMS = "Application of 0.2 % Magnesium sulphate at square formation and boll formation to reduce leaf reddening.";
                    }
                    else if (MessageType == "flower1" || MessageType == "flower2" || MessageType == "flower3")
                    {
                        if (Client == "ggrc")
                            SMS = " ?????? ??? ????, ??? ?????? ????????? ?????? ???????? ???????? 2 ????????? / 100 ???? ??????  ??????? ?? ?????? ??????? ?????.";
                        else
                            SMS = "For yield maximization, foliar application of Potassium Nitrate @ 2kg/100 lit of water at flower initiation stage would be benificial.";
                    }
                    else if (MessageType == "weedemergence")
                    {
                        if (Client == "ggrc")
                            SMS = "????? ???????: ???? ??????? ????????? ???? ???? ??? ??? ??????? ???? ????.";
                        else
                            SMS = "Weed Management: Intercultural operations should be carried out to control emerging weeds.";
                    }
                    else if (MessageType == "flowertoball")
                    {
                        if (Client == "ggrc")
                            SMS = "??? ??? ??? ??? ??? ????? : NAA (???????????) ?? ?????? : ???? ??? ??? ?????? ??? ???????? ????? ????? ??? ??? ?????? ???? ??? ?? 15 ???? ???? ???????  3.5 ???? NAA ?? ?????? ????.";
                        else
                            SMS = "Flower/ Bud dropping: Spray Of NAA(Planofix): Spray NAA @ 3.5 ml per 15 litres of water on the crop to retain more number of squares and flowers and to increase the yield.";
                    }
                    else if (MessageType.ToLower().Contains("custom"))
                        SMS = CustomMessage;


                    //if (SendingType == "Date" && DateTime.Now >= SendingDate && Status == "")
                    //{
                    //    if (SMS != "")
                    //        execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "')");
                    //    if (SMS2 != "")
                    //        execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "')");
                    //    if (SMS != "" && SMS2 != "")
                    //        Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    //}
                    //else if (SendingType == "Day" && (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency))))
                    //{
                    //    execQuery("update mfi.ggrcvillagesms set Status = 'Expired' where VillageID = " + VillageID + " and MessageType = '" + MessageType + "'");


                    //    if (SMS != "")
                    //        execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate, Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "')");
                    //    if (SMS2 != "")
                    //        execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "')");

                    //    if (SMS != "" && SMS2 != "")
                    //        Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    //}
                    else if (SendingType == "Floating" && (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= DTSowingDate.AddDays(floatdays))))
                    {
                        //execQuery("update mfi.ggrcvillagesms set Status = 'Expired' where VillageID = " + VillageID + " and MessageType = '" + MessageType + "'");


                        //if (SMS != "")
                        //    execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate, Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "')");
                        //if (SMS2 != "")
                        //    execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "')");

                        //if (SMS != "" && SMS2 != "")
                        //    Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    }
                    // execQuery("insert into mfi.sms_lastsend (VillageID, LogDate, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + MessageType + "')");

                }
            }
        }




        void SMSDabwaliFarms_Crop()
        {
            execQuery("delete from mfi.farm_sms_status_master where LogDate < '" + DateTime.Now.AddDays(-14).ToString("yyyy-MM-dd") + "'");
            // execQuery("delete from mfi.farm_sms_lstsend where date(LogDate) < '" + DateTime.Now.ToString("yyyy-MM-dd") + "' and (FarmID,MessageType) not in (SELECT FarmID,MessageType FROM mfi.farm_sms_status_master where MsgStatus='cancel' and date(DATE_ADD(ScheduleDate, INTERVAL 15 DAY))>date(Now())");
            execQuery("delete from mfi.farm_sms_lstsend where date(LogDate) < '" + DateTime.Now.ToString("yyyy-MM-dd") + "' and (FarmID,MessageType) " +
                " not in (SELECT FarmID,MessageType FROM mfi.farm_sms_status_master where MsgStatus='cancel')");
            execQuery("delete  from  mfi.farm_sms_status_master  where Date(StateWeatherExpiry) < '" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "' and ID>0 ");




            //for (int i = 0; i < dt.Rows.Count; i++)
            //{
            //    Agronimist objagro = new Agronimist();
            //    string Ids = dt.Rows[i]["UserID"].ToString();
            //    if (Ids.Trim() == "181185")
            //    {
            //        objagro.Id = Ids;

            //    }
            //}

            //string[] file = Directory.GetFiles(@"D:\work\Apps\WinApps\Forecast\");
            //for (int k = 0; k < file.Length; k++)
            //{
            //    string filename = Path.GetFileNameWithoutExtension(@"" + file[k] + "");
            //    string[] datepart = filename.Split('_');
            //    string DateOfDelete = Convert.ToDateTime(datepart[1]).AddDays(15).ToString("yyyy-MM-dd");
            //    if (DateOfDelete == DateTime.Now.Date.ToString("yyyy-MM-dd"))
            //        File.Delete(@"" + file[k] + "");
            //}
            List<StringString> listdisnameID = new List<StringString>();

            string diseaseqry = "select ID,Name from wrserver1.yfi_diseasemaster";
            DataTable DiseaseNameID = getData(diseaseqry);
            Dictionary<string, string> ClientIDDict = new Dictionary<string, string>();
            for (int j = 0; j < DiseaseNameID.Rows.Count; j++)
            {
                StringString names = new StringString();
                names.Str1 = DiseaseNameID.Rows[j]["ID"].ToString();
                names.Str2 = DiseaseNameID.Rows[j]["Name"].ToString();
                listdisnameID.Add(names);
            }


            string clientmsgqry = "select UserID,VisibleName,c.ClientName from mfi.usermaster u join mfi.clientcode c on " +
                "u.UserID=c.ClientID where u.UserTypeID=5 and u.role='admin' and u.smsflag='yes' ;";
            DataTable clientmsg = getData(clientmsgqry);
            for (int l = 0; l < clientmsg.Rows.Count; l++)
            {
                string visiblename = clientmsg.Rows[l]["ClientName"].ToString();
                string userid = clientmsg.Rows[l]["UserID"].ToString();
                if (userid != "100615")
                    ClientIDDict.Add(visiblename, userid);
            }



            //Dictionary<string, string> ClientIDDict = new Dictionary<string, string>()
            //{
            //    {"samunnati_gujarat","150665" },
            //     {"GIZ","105438" },
            //    {"cottonadvisory18","100894"},
            //    {"bayer","105477"},
            //    {"ggrc","100467" },
            //    { "isconbalaji","150442"},
            //    {"WRMS","100689"},
            //    {"wheatadvisory18","181458" }


            //};

            foreach (var pair in ClientIDDict)
            {

                string ID = pair.Value;
                string Client = pair.Key;
                Console.WriteLine("ID is==>" + ID + "==>client is==>" + Client);

                //if (ID != "194020")
                //    continue;
                List<StringStringStringString> LstRefID = new List<StringStringStringString>();
                List<DataTableAndColDescForecastData> LstsDataForecast = new List<DataTableAndColDescForecastData>();
                DataTable DTDabwalfarms = new DataTable();
                DTDabwalfarms = getDabwaliFarms(ID, Client);

                DataTable DTSMSTypes = getSMSMaster(Client);
                string tablename = "mfi.cotton_cropmessage_farms";

                List<WrmsAdmin> lstwrmsadmin = new List<WrmsAdmin>();

                List<string> lstSupport = new List<string>(); /*{ "9879010580", "8141897979", "9033635723", "9033765801", "7600616585", "9956128514" };*/
                List<Agronimist> lstAgro = new List<Agronimist>();

                DataTable preflan = GetPrefredlanguage(ID);
                string prefforecastlan = "";
                string[] forecastlan = preflan.Rows[0]["forecast_Preferredlan"].ToString().Split(';');
                List<forecastlan> forecastlangauge = new List<forecastlan>();
                if (forecastlan.Length > 1)
                {
                    for (int g = 0; g < forecastlan.Length; g++)
                    {
                        string[] stateidlang = forecastlan[g].Split('-');
                        forecastlangauge.Add(new forecastlan { stateID = stateidlang[0], forcastlan = stateidlang[1] });
                    }
                }
                else
                    prefforecastlan = forecastlan[0];



                string prefPoplan = preflan.Rows[0]["Pop_Preferredlan"].ToString();
                string forecastweatherflg = preflan.Rows[0]["forecastweatherflg"].ToString();

                DataTable dtAdmin = new DataTable();
                if (forecastweatherflg.ToLower() == "yes")
                {
                    lstwrmsadmin = FillWrmsAdmin();

                    dtAdmin = getData(" select * from mfi.sms_phoneno where BaseClientId='" + ID + "' or Type in ('Admin','Moderator')");
                    if (dtAdmin.Rows.Count > 0)
                    {
                        DataRow[] drAdmin = dtAdmin.Select("Type='Admin'");
                        for (int i = 0; i < drAdmin.Count(); i++)
                            lstSupport.Add(drAdmin[i]["PhoneNo"].ToString().Trim());

                        DataRow[] drClient = dtAdmin.Select("Type='Client'");
                        for (int i = 0; i < drClient.Count(); i++)
                            lstSupport.Add(drClient[i]["PhoneNo"].ToString().Trim());
                    }
                }



                if (dtAdmin.Rows.Count > 0)
                {
                    DataRow[] drAgronomist = dtAdmin.Select("Type='Agronomist'");
                    Dictionary<string, string> objdic = new Dictionary<string, string>();
                    if (drAgronomist.Count() > 0)
                    {
                        string Agronomist = "";
                        for (int k = 0; k < drAgronomist.Count(); k++)
                        {
                            if (Agronomist != "")
                                Agronomist += ",";
                            Agronomist += drAgronomist[k]["Clientid"].ToString();

                            objdic.Add(drAgronomist[k]["Clientid"].ToString(), drAgronomist[k]["PhoneNo"].ToString());
                        }
                        DataTable dt = getData(" Select * from mfi.kvk_staff_village_map where UserId in (" + Agronomist + ") and VillageID!='0'");

                        foreach (var dicpair in objdic)
                        {
                            DataRow[] dr = dt.Select("UserID='" + dicpair.Key + "'");
                            Agronimist objagro = new Agronimist();
                            objagro.Id = dicpair.Key;
                            objagro.PhoneNo = dicpair.Value;
                            List<string> lstdata = new List<string>();
                            for (int j = 0; j < dr.Count(); j++)
                                lstdata.Add(dr[j]["VillageID"].ToString());

                            objagro.lstIds = lstdata;
                            lstAgro.Add(objagro);
                        }

                    }
                }




                for (int j = 0; j < DTSMSTypes.Rows.Count; j++)
                {


                    string MessageType = DTSMSTypes.Rows[j]["MessageType"].ToString();
                   
                    string msgstateID = DTSMSTypes.Rows[j]["StateID"].ToString();
                    if ((MessageType == "PHLow" || MessageType == "PHHigh") && ID == "181857")
                        continue;
                    if (MessageType.ToLower().Contains("custom"))
                        continue;

                    string msgexprydate = DTSMSTypes.Rows[j]["StateWeatherExpriyDate"].ToString();
                    DateTime stateweatherExpriy = new DateTime();
                    DateTime.TryParse(msgexprydate, out stateweatherExpriy);
                    //if (MessageType.ToLower() != "weather")
                    //    continue;
                    //if (MessageType != "forecast")
                    //    continue;
                    Console.WriteLine("Starting for " + MessageType + " (" + j + "/" + DTSMSTypes.Rows.Count + ")");

                    string MessageCrops = DTSMSTypes.Rows[j]["CropIds"].ToString().Trim();
                    string MessageFarmers = DTSMSTypes.Rows[j]["FarmerIds"].ToString().Trim();

                    List<string> lstCrops = new List<string>();
                    List<string> lstFarmers = new List<string>();
                    if (MessageCrops != "")
                        lstCrops = JsonConvert.DeserializeObject<List<string>>(MessageCrops);

                    if (MessageFarmers != "")
                        lstFarmers = JsonConvert.DeserializeObject<List<string>>(MessageFarmers);

                    for (int i = 0; i < DTDabwalfarms.Rows.Count; i++)
                    {
                        //if (i <= 11)
                        //    continue;
                        string District = "";
                        string hindivillage = "";
                        string VillageID = "";
                        string stateID = "";
                        string Village = "";
                        string farmername = "";
                        string FarmID = DTDabwalfarms.Rows[i]["FarmID"].ToString();
                        string RefID = DTDabwalfarms.Rows[i]["RefId"].ToString();
                        farmername = DTDabwalfarms.Rows[i]["FarmerName"].ToString();
                        District = DTDabwalfarms.Rows[i]["District"].ToString();
                        string mobileno = DTDabwalfarms.Rows[i]["PhoneNumber"].ToString();
                        string DistrictSVM = DTDabwalfarms.Rows[i]["District"].ToString();
                        string SubDistrictSVM = DTDabwalfarms.Rows[i]["Sub_District"].ToString();
                        string VillageId = DTDabwalfarms.Rows[i]["VillageId"].ToString();
                        string WRMS_StateID = DTDabwalfarms.Rows[i]["WRMS_StateID"].ToString();
                        string cropid = DTDabwalfarms.Rows[i]["cropid"].ToString();

                        if (dtAdmin.Rows.Count > 0)
                        {
                            DataRow[] drModerator = dtAdmin.Select("Type='Moderator'");
                            for (int k = 0; k < drModerator.Count(); k++)
                            {
                                string stateid = drModerator[k]["StateId"].ToString().Trim();
                                string[] wrms_stateid_array = stateid.Split(',');
                                if (wrms_stateid_array.Length > 0)
                                {
                                    for (int l = 0; l < wrms_stateid_array.Length; l++)
                                    {
                                        if (wrms_stateid_array[l] == WRMS_StateID)
                                        {
                                            if (!(lstSupport.FindAll(a => a == drModerator[k]["PhoneNo"].ToString().Trim()).Count > 0))
                                                lstSupport.Add(drModerator[k]["PhoneNo"].ToString().Trim());
                                        }
                                    }

                                }
                            }
                        }
                        if (Client != "ggrc")
                        {

                            hindivillage = DTDabwalfarms.Rows[i]["VillageName_Hindi"].ToString();

                            stateID = DTDabwalfarms.Rows[i]["state"].ToString();

                        }
                        VillageID = DTDabwalfarms.Rows[i]["VillageID"].ToString();
                        Village = DTDabwalfarms.Rows[i]["VillageName"].ToString();
                        double Latitude = DTDabwalfarms.Rows[i]["latitude"].ToString().doubleTP();
                        double Longitude = DTDabwalfarms.Rows[i]["longitude"].ToString().doubleTP();


                        if (Client == "cottonadvisory18" && hindivillage != "")
                        {
                            Village = hindivillage;
                            Village = Village.Trim();
                        }
                        DateTime SowingDate = new DateTime();
                        //Console.WriteLine(DTDabwalfarms.Rows[i]["sowingdate"].ToString());
                        if (DTDabwalfarms.Rows[i]["sowingdate"].ToString() != "")
                            DateTime.TryParse(DTDabwalfarms.Rows[i]["sowingdate"].ToString(), out SowingDate);


                        if (stateID == "" || stateID.ToLower() == "0")
                            continue;


                        DataTable DTMicronuterentvalue = new DataTable();
                        int entry = 0;


                        var clientlanguage = forecastlangauge.Find(a => a.stateID == stateID);
                        if (clientlanguage != null)
                        {
                            prefforecastlan = clientlanguage.forcastlan;
                        }

                        
                        string MessageVillageID = DTSMSTypes.Rows[j]["VillageID"].ToString();
                        string MessageDistrict = DTSMSTypes.Rows[j]["DistrictID"].ToString();
                        string MessageBlock = DTSMSTypes.Rows[j]["BlockID"].ToString();
                        
                        //if (MessageType.ToLower().Contains("custom"))
                        {
                            bool flgMessageSend = false;
                            if (VillageID == MessageVillageID || (MessageVillageID == "0" && MessageDistrict.ToLower() == District.ToLower()) ||
                                (MessageVillageID == "0" && MessageDistrict == "0") || (MessageVillageID == "" && MessageDistrict == "") ||
                                (MessageVillageID == "" && MessageDistrict.ToLower() == District.ToLower()) || (MessageVillageID == "" && MessageDistrict == "0") ||
                                (MessageVillageID == "0" && MessageDistrict == ""))
                                flgMessageSend = true;

                            if (!flgMessageSend)
                                continue;

                            if (lstCrops.Count > 0)
                            {
                                if (!(lstCrops.FindAll(a => a.Trim() == cropid.Trim()).Count > 0))
                                    continue;
                            }

                            if (lstFarmers.Count > 0)
                            {
                                if (!(lstFarmers.FindAll(a => a.Trim() == FarmID.Trim()).Count > 0))
                                    continue;
                            }

                            if (msgstateID != "")
                            {
                                if (msgstateID != WRMS_StateID)
                                    continue;
                            }
                        }

                        string SendingType = DTSMSTypes.Rows[j]["SendingType"].ToString();
                        int SendingFrequency = 0;
                        int.TryParse(DTSMSTypes.Rows[j]["SendingFrequency"].ToString(), out SendingFrequency);

                        DateTime SendingDate = new DateTime();
                        DateTime.TryParse(DTSMSTypes.Rows[j]["SendingDate"].ToString(), out SendingDate);
                        DateTime dtLastSentDate = new DateTime();
                        string CustomMessage = DTSMSTypes.Rows[j]["Message"].ToString();
                        string FloatingDays = DTSMSTypes.Rows[j]["FloatingDays"].ToString();
                        string FloatingDaysTo = DTSMSTypes.Rows[j]["FloatDaysTo"].ToString();
                        string Status = "";
                        string SMS = "";
                        string SMS2 = "";
                        double totalrain = 0;
                        int floatdays = 0;
                        int floatdaysto = 0;
                        double EVRate = 0;
                        DateTime onsetdate = new DateTime();
                        DateTime d = DateTime.Now.Date;
                        string smsstatus = "";
                        Boolean flg = false;
                        Boolean step = false;
                        DateTime rundate = new DateTime();

                        string forecastdayflag = "";


                        if (forecastweatherflg == "yes" && (MessageType == "weather" || MessageType == "forecast"))
                        {
                           // SendingFrequency = 3;
                            string dayooweek = "";
                            forecastdayflag = "no";
                            dayooweek = DateTime.Now.DayOfWeek.ToString();
                            if (dayooweek == "Tuesday" || dayooweek == "Thursday")
                                forecastdayflag = "yes";
                        }

                        string flagoutturnforecast = "no";




                        Console.WriteLine(j);



                        if (FloatingDays != "")
                        {
                            int.TryParse(FloatingDays, out floatdays);
                        }
                        int.TryParse(FloatingDaysTo, out floatdaysto);


                        if (chkFarmProcessed(FarmID, MessageType, stateweatherExpriy))
                            continue;

                        DataTable DTLastMessage = getFarmLastMessage(FarmID, MessageType);

                        if (DTLastMessage.Rows.Count > 0)
                        {
                            DateTime.TryParse(DTLastMessage.Rows[0]["ScheduleDate"].ToString(), out dtLastSentDate);
                            Status = DTLastMessage.Rows[0]["MsgStatus"].ToString();
                        }
                        if (SowingDate.Year != 1)
                        {
                            Console.WriteLine("Float Days From " + SowingDate.AddDays(floatdays).Date.ToString("yyyy-MM-dd"));
                            Console.WriteLine("Float Days TO " + SowingDate.AddDays(floatdaysto).Date.ToString("yyyy-MM-dd"));
                        }



                        bool flgDoProcess = false;

                        if (SendingType == "Day")
                        {
                            flgDoProcess = (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency)));
                            if (stateweatherExpriy.Year != 1)
                                flgDoProcess = true;
                        }
                        else if (SendingType == "Floating")
                        {
                            if (MessageType != "Cotton_Irrigation" && SowingDate.Year != 1)
                                flgDoProcess = (DateTime.Now.Date >= SowingDate.AddDays(floatdays).Date && Status == "" && (floatdaysto == 0 || DateTime.Now.Date <= SowingDate.AddDays(floatdaysto)));
                            else if (MessageType != "Cotton_Irrigation" && SowingDate.Year == 1)
                                flgDoProcess = false;
                            else
                                flgDoProcess = true;
                        }
                        else
                            flgDoProcess = (DateTime.Now >= SendingDate && SendingDate > DateTime.Now.AddDays(-15) && Status == "");

                        if (!flgDoProcess)
                            continue;
                        if (CustomMessage != "")
                            SMS = CustomMessage;


                        string rainalert = "no";

                        if (MessageType == "forecast" && DateTime.Now.Hour >= 10 && DateTime.Now.Hour <= 24)
                        {
                            var ObjRefIDData = LstRefID.Find(a => a.Str1 == RefID );
                            if (ObjRefIDData == null)
                            {

                                SMS = getGGRCForecastSMS(Latitude, Longitude, Village, Client, prefforecastlan, out SMS2, FarmID,out rainalert);
                                string myrainalert = rainalert;
                                if (SMS != "" || SMS2 != "")
                                {
                                    if (prefforecastlan.ToLower() != "gujrati")
                                    {
                                        if (SMS != "")
                                            SMS = Village + SMS;
                                        if (SMS2 != "")
                                            SMS2 = Village + SMS2;
                                        if (RefID != "" && RefID != "0")
                                            LstRefID.Add(new StringStringStringString { Str1 = RefID, Str2 = SMS, Str3 = SMS2,Str4= myrainalert });
                                    }
                                    else
                                    {
                                        if (RefID != "" && RefID != "0")
                                            LstRefID.Add(new StringStringStringString { Str1 = RefID, Str2 = SMS, Str3 = SMS2 ,Str4= myrainalert });
                                    }
                                }
                                else
                                {
                                    continue;
                                }
                            }
                            else
                            {
                                SMS = ObjRefIDData.Str2;
                                SMS2 = ObjRefIDData.Str3;
                                rainalert= ObjRefIDData.Str4;
                            }

                        }
                        else if (MessageType == "PHLow")
                        {
                            string Ph = "";
                            Ph = getGGRCSoilAnalysis(Village, Latitude, Longitude, "PH");
                            if (!(Ph.doubleTP() < 5.5))
                                continue;
                            // SMS = CustomMessage;
                            if (prefPoplan == "Kannada")
                                SMS = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರೇ,ನಿಮ್ಮ ಜಮೀನಿನಲ್ಲಿರುವ ಮಣ್ಣಿನ ರಸಸಾರವು ಕಡಿಮೆಇರುವುದರಿಂದ,ಮಣ್ಣು ಆಲೂಗಡ್ಡೆ ಬೆಳೆಯನ್ನು ಬೆಳೆಯಲು ಸೂಕ್ತವಾಗಿರುವುದಿಲ್ಲ.ದಯವಿಟ್ಟು ನಿಮ್ಮ ಹತ್ತಿರದ ಮಣ್ಣಿನ ತಜ್ಞರನ್ನು ಸಂಪರ್ಕಿಸಿ, ಮಣ್ಣಿನ ಪರೀಕ್ಷೆ ಮಾಡಿಸಿ.";
                            else if (prefPoplan.ToLower() == "hindi")
                                SMS = "श्रीमान्, आपके खेत की मिटटी कम pH के कारण कपास फसल की खेती के लिए उपयुक्त नहीं है। कृपया अपने निकटतम मृदा विशेषज्ञ से संपर्क करें और अपनी खेत की मिटटी की जाँच करायें|";
                            else if (prefPoplan.ToLower() == "english")
                                SMS = "Dear Sir, Soil of your field is not feasible for cotton crop cultivation due to low pH. Please contact your nearest soil expert and get your soil tested.";
                        }
                        else if (MessageType == "PHHigh")
                        {
                            string Ph = "";
                            Ph = getGGRCSoilAnalysis(Village, Latitude, Longitude, "PH");
                            if (!(Ph.doubleTP() > 8.0))
                                continue;
                            // SMS = CustomMessage;
                            if (prefPoplan == "Kannada")
                                SMS = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರೇ,ನಿಮ್ಮ ಜಮೀನಿನಲ್ಲಿರುವ ಮಣ್ಣಿನ ರಸಸಾರವು ಹೆಚ್ಚಾಗಿರುವುದರಿಂದ,ಮಣ್ಣು ಆಲೂಗಡ್ಡೆ ಬೆಳೆಯನ್ನು ಬೆಳೆಯಲು ಸೂಕ್ತವಾಗಿರುವುದಿಲ್ಲ.ದಯವಿಟ್ಟು ನಿಮ್ಮ ಹತ್ತಿರದ ಮಣ್ಣಿನ ತಜ್ಞರನ್ನು ಸಂಪರ್ಕಿಸಿ, ಮಣ್ಣಿನ ಪರೀಕ್ಷೆ ಮಾಡಿಸಿ.";
                            else if (prefPoplan.ToLower() == "hindi")
                                SMS = "श्रीमान्, आपके खेत की मिटटी अधिक pH के कारण कपास  फसल की खेती के लिए उपयुक्त नहीं है। कृपया अपने निकटतम मृदा विशेषज्ञ से संपर्क करें और अपनी खेत की मिटटी की जाँच करायें|";
                            else if (prefPoplan.ToLower() == "english")
                                SMS = " Dear Sir, Soil of your field is not feasible for cotton crop cultivation due to high pH. Please contact your nearest soil expert and get your soil tested.";
                        }
                        else if (MessageType == "SoilTexture")
                        {
                            string st = "";
                            st = getGGRCSoilAnalysis(Village, Latitude, Longitude, "SoilTexture");
                            if (!(st.ToLower().Contains("sandy")))
                                continue;
                            SMS = CustomMessage;
                            if (prefPoplan == "Kannada")
                                SMS = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರೆ,ನಿಮ್ಮ ಜಮೀನಿನಲ್ಲಿರುವ ಮಣ್ಣಿನ ರಚನೆಯು ಮರಳು ಮಿಶ್ರಿತ ಮಣ್ಣಾಗಿದ್ದರೆ, ಮಣ್ಣು ಆಲೂಗಡ್ಡೆ ಬೆಳೆಯನ್ನು ಬೆಳೆಯಲು ಸೂಕ್ತವಾಗಿರುವುದಿಲ್ಲ.ದಯವಿಟ್ಟು ನಿಮ್ಮ ಹತ್ತಿರದ ಮಣ್ಣಿನ ತಜ್ಞರಿಂದ ಮಣ್ಣಿನ ರಚನೆಯ ವಿಚಾರವಾಗಿ,ಮಣ್ಣಿನ ಪರೀಕ್ಷೆ ಮಾಡಿಸಿ.";
                        }
                        //else if (MessageType == "NDVI" && SowingDate.Year!=1)
                        //{
                        //    DataTable DTNDVI = new DataTable();
                        //    DTNDVI = getGGRCNDVI(VillageID, SowingDate.AddDays(floatdays), SowingDate.AddDays(floatdays + 30));
                        //    if (DTNDVI.Rows.Count > 0)
                        //    {
                        //        string NDVIValue = DTNDVI.Rows[0][0].ToString().Trim();
                        //        if (NDVIValue == "")
                        //            continue;
                        //        double NDVI = NDVIValue.doubleTP();
                        //        if (!(NDVI < 0.5))
                        //            continue;
                        //        SMS = CustomMessage;
                        //        if (prefPoplan == "Kannada")
                        //            SMS = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರ್ರೇ,ಬಿತ್ತನೆ ಮಾಡಿದ ದಿನದಿಂದ ೩೦ ದಿನಗಳ ನಂತರದ ಬೆಳೆಯ ಕ್ಲೋರೊಫಿಲ್ ನ ಸೂಚ್ಯಂಕವು ೦.೫ ಕ್ಕಿಂತ ಕಡಿಮೆಯಿದ್ದರೆ,ಇದರಿಂದ ಆಲೂಗಡ್ಡೆ ಇಳುವರಿಯು ಕಡಿಮೆಯಾಗುತ್ತದೆ.ದಯವಿಟ್ಟು ನಿಮ್ಮ ಜಮೀನನ್ನು ಯಾವುದೆ ರೋಗಗಳು ಮತ್ತು ಪೌಷ್ಟಿಕಾಂಶಗಳ ಕೊರತೆಗಳ ಸಲುವಾಗಿ ಕೃಷಿ  ತಜ್ಞರನ್ನು ಸಂಪರ್ಕಿಸಿ";
                        //    }
                        //}

                        else if (MessageType == "NDVI" && Client == "cottonadvisory18")
                        {
                            DateTime date = DateTime.Now.Date;
                            DataTable DTNDVI = new DataTable();
                            DTNDVI = getGGRCNDVI(FarmID, date, date.AddDays(7));

                            if (DTNDVI.Rows.Count > 0)
                            {
                                string NDVIValue = DTNDVI.Rows[0]["ndvi_value"].ToString().Trim();

                                if (NDVIValue == "")
                                    continue;
                                double NDVI = NDVIValue.doubleTP();
                                if (NDVI < 0.03 && (DateTime.Now.Date == SowingDate.AddDays(0)))
                                    SMS = "Low NDVI";
                                else if (NDVI < 0.07 && (DateTime.Now.Date == SowingDate.AddDays(30)))
                                    SMS = "Low NDVI";
                                else if (NDVI < 0.22 && (DateTime.Now.Date == SowingDate.AddDays(60)))
                                    SMS = "Low NDVI";
                                else if (NDVI < 0.39 && (DateTime.Now.Date == SowingDate.AddDays(90)))
                                    SMS = "Low NDVI";
                                else if (NDVI < 0.50 && (DateTime.Now.Date == SowingDate.AddDays(120)))
                                    SMS = "Low NDVI";
                                else
                                    continue;
                            }
                            else
                                continue;
                        }
                        else if (MessageType == "NDVI" && Client == "GIZ")
                        {
                            DataTable DTNDVI = new DataTable();
                            DateTime date = DateTime.Now.Date;
                            DTNDVI = getGGRCNDVI(FarmID, date, date.AddDays(7));
                            if (DTNDVI.Rows.Count > 0)
                            {
                                string NDVIValue = DTNDVI.Rows[0]["ndvi_value"].ToString().Trim();
                                if (NDVIValue == "")
                                    continue;
                                double NDVI = NDVIValue.doubleTP();
                                if (!(NDVI < 0.5))
                                    continue;
                                if (prefPoplan == "Kannada")
                                    SMS = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರ್ರೇ,ಬಿತ್ತನೆ ಮಾಡಿದ ದಿನದಿಂದ ೩೦ ದಿನಗಳ ನಂತರದ ಬೆಳೆಯ ಕ್ಲೋರೊಫಿಲ್ ನ ಸೂಚ್ಯಂಕವು ೦.೫ ಕ್ಕಿಂತ ಕಡಿಮೆಯಿದ್ದರೆ,ಇದರಿಂದ ಆಲೂಗಡ್ಡೆ ಇಳುವರಿಯು ಕಡಿಮೆಯಾಗುತ್ತದೆ.ದಯವಿಟ್ಟು ನಿಮ್ಮ ಜಮೀನನ್ನು ಯಾವುದೆ ರೋಗಗಳು ಮತ್ತು ಪೌಷ್ಟಿಕಾಂಶಗಳ ಕೊರತೆಗಳ ಸಲುವಾಗಿ ಕೃಷಿ  ತಜ್ಞರನ್ನು ಸಂಪರ್ಕಿಸಿ";
                            }
                        }


                        else if (MessageType == "weather" && DateTime.Now.Hour >= 17 && DateTime.Now.Hour <= 24)
                        {
                            SMS = getGGRCWeatherDataSMS(Latitude, Longitude, Village, Client, prefforecastlan);
                        }


                        //else if (MessageType == "disease_PinkBollworm" && DateTime.Now.Hour >= 17 && DateTime.Now.Hour <= 24)
                        //{
                        //    string DISName = "Pink Bollworm";
                        //    DataTable DTForecastFUll = new DataTable();
                        //    var objrefid = LstsDataForecast.Find(a => a.RID == RefID);
                        //    if(objrefid==null)
                        //    {
                        //        DTForecastFUll = getforecastdata(Latitude, Longitude, "", "");
                        //        if (RefID != "" && RefID != "0")
                        //        {
                        //            LstsDataForecast.Add(new DataTableAndColDescForecastData { RID = RefID, DTForecast = DTForecastFUll });
                        //        }
                        //    }
                        //    else
                        //    {
                        //        DTForecastFUll = LstsDataForecast.Find(a => a.RID == RefID).DTForecast;
                        //    }
                        //    List<DieaseDates> lstSMS = getGGRCDiseasePinkBollwormSMS(Latitude, Longitude, Village, Client, DTForecastFUll);


                        //    execQuery("delete from mfi.diseasetracker where Date(DiseaseDates)>'"+DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd")+ "' and Date(DiseaseDates)<'"+ DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "' and FarmID='"+FarmID+"'");
                        //    if (lstSMS.Count > 0)
                        //    {
                        //        string query = "Insert Into mfi.diseasetracker(FarmID, MaxTemp, MaxHumidity, MinHumidity, DiseaseDates, DiseaseID, RH, MinTemp, AvgTemp) Values ";
                        //        DateTime date = new DateTime();
                        //        string querypart = "";
                        //        string DID = listdisnameID.Find(a => a.Str2 == DISName).Str1;
                        //        string dishist = "insert into mfi.disease_historytracker(FarmID, LogDate, DiseaseID) values ('"+FarmID+"','"+DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")+"','"+DID+"')";
                        //        execQuery(dishist);
                        //        for (int g = 0; g < lstSMS.Count; g++)
                        //        {
                        //            SMS = lstSMS[g].SMSPinkBollWorm;

                        //            string maxtemp = lstSMS[g].MaxTemperature;
                        //            string maxhumid = lstSMS[g].MaxHumid;
                        //            string minhumid = lstSMS[g].MinHumid;
                        //            string Rh = lstSMS[g].Rh;
                        //            string mintemp = lstSMS[g].MinTemperature;
                        //            DateTime.TryParse(lstSMS[g].Date.ToString(), out date);
                        //            string avgtemperature = lstSMS[g].AverageTemp;

                        //            if (querypart != "")
                        //                querypart += ",";
                        //            querypart += "('" + FarmID + "','" + maxtemp + "','" + maxhumid + "','" + minhumid + "','" + date.ToString("yyyy-MM-dd") + "','" + DID + "','" + Rh + "','" + mintemp + "','" + avgtemperature + "')";

                        //        }
                        //        if (querypart != "")
                        //        {
                        //            execQuery(query + querypart);
                        //        }



                        //    }



                        //}
                        //else if (MessageType == "disease_WhiteFly" && DateTime.Now.Hour >= 17 && DateTime.Now.Hour <= 24)
                        //{
                        //    string DISName = "Whitefly";

                        //    DataTable DTForecastFUll = new DataTable();
                        //    var objrefid = LstsDataForecast.Find(a => a.RID == RefID);
                        //    if (objrefid == null)
                        //    {
                        //        DTForecastFUll = getforecastdata(Latitude, Longitude, "", "");
                        //        if (RefID != "" && RefID != "0")
                        //        {
                        //            LstsDataForecast.Add(new DataTableAndColDescForecastData { RID = RefID, DTForecast = DTForecastFUll });
                        //        }
                        //    }
                        //    else
                        //    {
                        //        DTForecastFUll = LstsDataForecast.Find(a => a.RID == RefID).DTForecast;
                        //    }

                        //    List<DieaseDates> lstSMS = getWhiteFlyDiseaseSMS(Latitude, Longitude, Village, Client, DTForecastFUll);
                        //    execQuery("delete from mfi.diseasetracker where Date(DiseaseDates)>'" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "' and Date(DiseaseDates)<'" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "' and FarmID='" + FarmID + "'");
                        //    if (lstSMS.Count > 0)
                        //    {
                        //        string query = "Insert Into mfi.diseasetracker(FarmID, MaxTemp, MaxHumidity, MinHumidity, DiseaseDates, DiseaseID, RH, MinTemp, AvgTemp) Values ";
                        //        DateTime date = new DateTime();
                        //        string querypart = "";
                        //        string DID = listdisnameID.Find(a => a.Str2 == DISName).Str1;
                        //        string dishist = "insert into mfi.disease_historytracker(FarmID, LogDate, DiseaseID) values ('" + FarmID + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + DID + "')";
                        //        execQuery(dishist);
                        //        for (int g = 0; g < lstSMS.Count; g++)
                        //        {
                        //            SMS = lstSMS[g].SMSWhiteFly;

                        //            string maxtemp = lstSMS[g].MaxTemperature;
                        //            string maxhumid = lstSMS[g].MaxHumid;
                        //            string minhumid = lstSMS[g].MinHumid;
                        //            string Rh = lstSMS[g].Rh;
                        //            string mintemp = lstSMS[g].MinTemperature;
                        //            DateTime.TryParse(lstSMS[g].Date.ToString(), out date);
                        //            string avgtemperature = lstSMS[g].AverageTemp;                                  
                        //            if (querypart != "")
                        //                querypart += ",";
                        //            querypart += "('" + FarmID + "','" + maxtemp + "','" + maxhumid + "','" + minhumid + "','" + date.ToString("yyyy-MM-dd") + "','" + DID + "','" + Rh + "','" + mintemp + "','" + avgtemperature + "')";

                        //        }
                        //        if (querypart != "")
                        //        {
                        //            execQuery(query + querypart);
                        //        }

                        //    }



                        //}


                        //else if (MessageType == "disease_Blight" && DateTime.Now.Hour >= 17 && DateTime.Now.Hour <= 24)
                        //{
                        //    string DISName = "Blight";
                        //    execQuery("delete from mfi.diseasetracker where Date(DiseaseDates)>'" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "' and Date(DiseaseDates)<'" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "' and FarmID='" + FarmID + "'");
                        //    DataTable DTForecastFUll = new DataTable();
                        //    var objrefid = LstsDataForecast.Find(a => a.RID == RefID);
                        //    if (objrefid == null)
                        //    {
                        //        DTForecastFUll = getforecastdata(Latitude, Longitude, "", "");
                        //        if (RefID != "" && RefID != "0")
                        //        {
                        //            LstsDataForecast.Add(new DataTableAndColDescForecastData { RID = RefID, DTForecast = DTForecastFUll });
                        //        }
                        //    }
                        //    else
                        //    {
                        //        DTForecastFUll = LstsDataForecast.Find(a => a.RID == RefID).DTForecast;
                        //    }



                        //    List<DieaseDates> lstSMS = getBlightDiseaseSMS(Latitude, Longitude, Village, Client,DTForecastFUll);
                        //    if (lstSMS.Count > 0)
                        //    {
                        //        string query = "Insert Into mfi.diseasetracker(FarmID, MaxTemp, MaxHumidity, MinHumidity, DiseaseDates, DiseaseID, RH, MinTemp, AvgTemp,Rain) Values ";
                        //        DateTime date = new DateTime();
                        //        string querypart = "";
                        //        string DID = listdisnameID.Find(a => a.Str2 == DISName).Str1;
                        //        string dishist = "insert into mfi.disease_historytracker(FarmID, LogDate, DiseaseID) values ('" + FarmID + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + DID + "')";
                        //        execQuery(dishist);
                        //        for (int g = 0; g < lstSMS.Count; g++)
                        //        {
                        //            SMS = lstSMS[g].SMSBlight;

                        //            string maxtemp = lstSMS[g].MaxTemperature;
                        //            string maxhumid = lstSMS[g].MaxHumid;
                        //            string minhumid = lstSMS[g].MinHumid;
                        //            string Rh = lstSMS[g].Rh;
                        //            string mintemp = lstSMS[g].MinTemperature;
                        //            DateTime.TryParse(lstSMS[g].Date.ToString(), out date);
                        //            string avgtemperature = lstSMS[g].AverageTemp;
                        //            string rain = lstSMS[g].Rain;
                        //            if (querypart != "")
                        //                querypart += ",";
                        //            querypart += "('" + FarmID + "','" + maxtemp + "','" + maxhumid + "','" + minhumid + "','" + date.ToString("yyyy-MM-dd") + "','" + DID + "','" + Rh + "','" + mintemp + "','" + avgtemperature + "','" + rain + "')";

                        //        }
                        //        if (querypart != "")
                        //        {
                        //            execQuery(query + querypart);
                        //        }

                        //    }

                        //}

                        else if (MessageType == "Cotton_Irrigation")
                        {
                            DateTime check_date = new DateTime();
                            if (SowingDate.Year != 1)
                            {

                                if (FloatingDays != "")
                                {
                                    int.TryParse(FloatingDays, out floatdays);
                                }
                                smsstatus = GetSmsStatus(FarmID, MessageType, tablename);
                                if (smsstatus == "sent")
                                {
                                    continue;
                                }

                                check_date = GetdateOfCrop(FarmID, MessageType, tablename);
                                onsetdate = Convert.ToDateTime(SowingDate).AddDays(floatdays);
                                EVRate = GetMeanEt(SowingDate.Month.ToString(), stateID);
                                totalrain = GetCropRainData(Latitude, Longitude, Village, Client);
                                totalrain = totalrain + EVRate;
                                if (check_date.Year == 1)
                                    rundate = onsetdate;
                                if (smsstatus == "step2_1")
                                    rundate = check_date.AddDays(3);
                                else if (smsstatus == "step2")
                                    rundate = check_date.AddDays(3);
                                //////////////step1 Start                               
                                if (DateTime.Now.Date >= rundate.Date && !smsstatus.Contains("step2"))//if not step2
                                {
                                    if (step1(FarmID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, tablename))
                                        continue;

                                }

                                //////////////step1 End

                                if ((DateTime.Now.Date >= rundate.Date && smsstatus.Contains("step2_")))
                                    Step2(FarmID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, tablename, totalwater);

                                if (smsstatus == "step2" && DateTime.Now.Date >= rundate.Date)//if satus==2 && DateTime.now.date>=dtchckdate+3
                                {//step3                            
                                    double tolrenec = 0;
                                    double rain = GetCropRainData(Latitude, Longitude, Village, Client);

                                    if (rain + tolrenec > 0)
                                    {
                                        if (step1(FarmID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, tablename))
                                            continue;
                                    }

                                    else if ((DateTime.Now.Date - onsetdate.Date).Days < 7)
                                    {
                                        Step2(FarmID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, tablename, totalwater);
                                    }
                                    else
                                    {//step4

                                        if ((DateTime.Now.Date - onsetdate.Date).Days >= 7 && (DateTime.Now.Date - onsetdate.Date).Days <= 12)
                                        { Step2(FarmID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsstatus, tablename, totalwater); }
                                        else
                                        {

                                            double amountofwater = GetRemainamountofwater(FarmID, tablename);
                                            if (totalwater - amountofwater > 0)
                                                SMS = "Release " + (totalwater - amountofwater) + " mm amount of water";
                                            updatecropsmsstatus(FarmID, "step4", MessageType, tablename);
                                        }
                                    }
                                }
                            }

                        }
                        else if (MessageType == "SoilDeficient_Zn")
                        {
                            if (entry == 0)
                                DTMicronuterentvalue = GetMicrnutrientValues(VillageID);
                            if (DTMicronuterentvalue.Rows.Count > 0)
                            {
                                string val = DTMicronuterentvalue.Rows[0]["zn_mean"].ToString();
                                if (val != "" && val != null)
                                {
                                    if (DTMicronuterentvalue.Rows.Count > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["zn_mean"]) > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["zn_mean"]) < 15)
                                    {
                                        SMS = "श्रीमान्,आपके गांव में मिट्टी जिंक में कमी पाई गई है। इससे कपास की फसल के उत्पादन में कमी आ सकती  है। यदि आपने बेसल डोज़ नहीं लगायी है,तो अपनी मिट्टी की जाँच करायें। लक्षण दिखाई देने की स्थिति में हमारे विशेषज्ञ के संपर्क करें।";

                                    }
                                }
                                entry = 1;
                            }
                        }

                        else if (MessageType == "SoilDeficient_Su")
                        {
                            if (entry == 0)
                                DTMicronuterentvalue = GetMicrnutrientValues(VillageID);
                            if (DTMicronuterentvalue.Rows.Count > 0)
                            {
                                string val = DTMicronuterentvalue.Rows[0]["s_mean"].ToString();
                                if (val != "" && val != null)
                                {
                                    if (DTMicronuterentvalue.Rows.Count > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["s_mean"]) > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["s_mean"]) < 20)
                                    {
                                        //Check For mgs sent form farmID//
                                        SMS = "Soil may be deficient in Sulphur, this can lead to insufficient chlorophyll  in leaf causing reduced yields";
                                        //   InsertInFarmsatausMaster(FarmID, MessageType);
                                    }
                                }
                                entry = 1;
                            }
                        }


                        else if (MessageType == "SoilDeficient_Fe")
                        {
                            if (entry == 0)
                                DTMicronuterentvalue = GetMicrnutrientValues(VillageID);
                            if (DTMicronuterentvalue.Rows.Count > 0)
                            {
                                string val = DTMicronuterentvalue.Rows[0]["fe_mean"].ToString();
                                if (val != "" && val != null)
                                {
                                    if (DTMicronuterentvalue.Rows.Count > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["fe_mean"]) > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["fe_mean"]) < 15)
                                    {
                                        SMS = "Soil may be deficient in Iron, this can lead to Interveinal chlorosis of young leaves which progresses over entire leaf leading to reduced yield";
                                        //  InsertInFarmsatausMaster(FarmID, MessageType);
                                    }
                                }
                                entry = 1;
                            }
                        }
                        else if (MessageType == "SoilDeficient_Cu")
                        {
                            if (entry == 0)
                                DTMicronuterentvalue = GetMicrnutrientValues(VillageID);
                            if (DTMicronuterentvalue.Rows.Count > 0)
                            {
                                string val = DTMicronuterentvalue.Rows[0]["cu_mean"].ToString();
                                if (val != "" && val != null)
                                {
                                    if (DTMicronuterentvalue.Rows.Count > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["cu_mean"]) > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["cu_mean"]) < 10)
                                    {
                                        SMS = "Soil may be deficient in Copper, this may lead to young leaves at the growing point become chlorotic.";
                                        //  InsertInFarmsatausMaster(FarmID, MessageType);
                                    }
                                }
                                entry = 1;
                            }
                        }
                        else if (MessageType == "SoilDeficient_B")
                        {
                            if (entry == 0)
                                DTMicronuterentvalue = GetMicrnutrientValues(VillageID);
                            if (DTMicronuterentvalue.Rows.Count > 0)
                            {
                                string val = DTMicronuterentvalue.Rows[0]["b_mean"].ToString();
                                if (val != "" && val != null)
                                {
                                    if (DTMicronuterentvalue.Rows.Count > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["b_mean"]) > 0 && Convert.ToDouble(DTMicronuterentvalue.Rows[0]["b_mean"]) < 10)
                                    {
                                        if (prefPoplan == "English")
                                            SMS = "श्रीमान्, आपके गांव में मिट्टी बोरॉन में कमी पाई गई है। इससे कपास का फल गिर सकता है। यदि आपने बेसल डोज़ नहीं लगायी है,तो अपनी मिट्टी की जाँच करायें। लक्षण दिखाई देने की स्थिति में हमारे विशेषज्ञ के संपर्क करें।";
                                        if (prefPoplan == "Kannada")
                                            SMS = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರೇ,ನಿಮ್ಮ ಹಳ್ಳಿಯಲ್ಲಿರುವ ಮಣ್ಣಿನಲ್ಲಿ  ಬೋರಾನ್ ನ ಕೊರತೆ ಇದ್ದರೆ,ನೀವು ಇನ್ನು ಗೊಬ್ಬರವನ್ನು ಹಾಕಿಲ್ಲವಾದರೆ,ಮಣ್ಣಿನ ಪರೀಕ್ಷೆ ಮಾಡಿಸಿ,ಏನಾದರು ಕೊರತೆಯ ಸೂಚನೆ ಕಂದು ಬಂದಲ್ಲಿ ನಮ್ಮ  ಕೃಷಿ  ತಜ್ಞರನ್ನು ಸಂಪರ್ಕಿಸಿ.";
                                    }
                                }
                                entry = 1;
                            }
                        }



                        else if (MessageType == "Cotton_15_1")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_2")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_3")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_4")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_5")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_6")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_7")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_8")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_9")
                            SMS = CustomMessage;
                        else if (MessageType == "Cotton_15_10")
                            SMS = CustomMessage;




                        else if (MessageType == "Cotton_Pop_104")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_107")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_108")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_109")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_112")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_113")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_115")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_118")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_122")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_124")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_125")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_131")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_134")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_141")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_182")

                            SMS = CustomMessage;

                        else if (MessageType == "Cotton_Pop_183")
                            SMS = CustomMessage;


                        else if (MessageType == "Cotton_Pop_253")

                            SMS = CustomMessage;

                        if (forecastdayflag == "no" && rainalert == "yes" && forecastdayflag!="")
                        {
                            flagoutturnforecast = "yes";
                        }

                        else if (forecastdayflag == "no" && rainalert == "no" && forecastdayflag != "")
                            continue;




                        if (SMS == "")
                        {
                            int a = 0;
                        }
                        if (ID == "181857" && SMS != "" && (MessageType == "weather" || MessageType == "forecast"))
                        {
                            SMS = SMS + " IFPRI";
                        }


                        if (SendingType == "Date" && DateTime.Now >= SendingDate && Status == "")
                        {
                            if (SMS != "")

                                execQuery("insert into mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate,MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS.Trim() + "', '" + MessageType + "', '" + MessageType + "')");
                            if (SMS2 != "")
                                execQuery("insert into mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate,MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2.Trim() + "', '" + MessageType + "', '" + MessageType + "')");
                            if (SMS != "" && SMS2 != "")
                                Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                        }
                        else if (SendingType == "Day" && (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency))) && stateweatherExpriy.Year == 1)
                        {

                            execQuery("update mfi.farm_sms_status_master set MsgStatus = 'Expired' where FarmID = " + FarmID + " and MessageType = '" + MessageType + "' and MsgStatus='Pending'");

                            string forecassmsstats = "Pending";
                            //forecastweatherflg == "yes" && (MessageType == "weather" || MessageType == "forecast")
                            if ((forecastdayflag.ToLower() == "yes" || flagoutturnforecast.ToLower() == "yes") && forecastweatherflg == "yes")
                            {
                                forecassmsstats = "send";

                                if (mobileno != "" && mobileno.Length == 10)
                                {
                                    if (SMS != "")
                                        execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel,  OutDate,FarmID) values ('" + Client + "Farmers', '" + mobileno + "', '" + Client + "', '" + Client + " Subject', '" + SMS + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + FarmID + "')");
                                    if (SMS2 != "")
                                        execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel,  OutDate,FarmID) values ('" + Client + "Farmers', '" + mobileno + "', '" + Client + "', '" + Client + " Subject', '" + SMS2 + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','" + FarmID + "')");
                                    for (int k = 0; k < lstSupport.Count; k++)
                                    {
                                        mobileno = lstSupport[k];
                                        if (SMS != "")
                                        {
                                            List<WrmsAdmin> lst = lstwrmsadmin.FindAll(a => a.PhoneNo == mobileno && a.Msg == MessageType + "_SMSSender");
                                            if (!(lst.Count > 0))
                                            {
                                                execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel, " +
                                                    " OutDate) values ('" + Client + "Farmers', '" + mobileno + "', '" + Client + "', '" + Client + " Subject'," +
                                                    " '" + SMS + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                                                lstwrmsadmin.Add(new WrmsAdmin() { ddat = DateTime.Now, PhoneNo = mobileno, Msg = MessageType + "_SMSSender" });
                                                execQuery("insert into mfi.wrmsadminmsg(Msg, PhoneNo, ddat) values ('" + MessageType + "_SMSSender', '" + mobileno + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                                            }
                                        }
                                        if (SMS2 != "")
                                        {
                                            List<WrmsAdmin> lst = lstwrmsadmin.FindAll(a => a.PhoneNo == mobileno && a.Msg == MessageType + "_SMSSender2");
                                            if (!(lst.Count > 0))
                                            {
                                                execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel, " +
                                                    " OutDate) values ('" + Client + "Farmers', '" + mobileno + "', '" + Client + "', " +
                                                    "'" + Client + " Subject', '" + SMS + "', 'Pending', 'Unicode', 'Gateway2', " +
                                                    "'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                                                lstwrmsadmin.Add(new WrmsAdmin() { ddat = DateTime.Now, PhoneNo = mobileno, Msg = MessageType + "_SMSSender2" });
                                                execQuery("insert into mfi.wrmsadminmsg(Msg, PhoneNo, ddat) values ('" + MessageType + "_SMSSender2', '" + mobileno + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                                            }
                                        }
                                    }
                                    string phoneno = "";
                                    for (int k = 0; k < lstAgro.Count; k++)
                                    {
                                        List<string> lststring = lstAgro[k].lstIds;
                                        if (lststring.Count > 0)
                                        {
                                            List<string> lstfind = lststring.FindAll(a => a == VillageId);
                                            if (lstfind.Count > 0)
                                            {
                                                phoneno = lstAgro[k].PhoneNo;
                                                break;
                                            }
                                        }
                                    }
                                    if (phoneno != "")
                                    {
                                        mobileno = phoneno;
                                        if (SMS != "")
                                        {
                                            List<WrmsAdmin> lst = lstwrmsadmin.FindAll(a => a.PhoneNo == mobileno && a.Msg == MessageType + "_SMSSender");
                                            if (!(lst.Count > 0))
                                            {
                                                execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel, " +
                                                " OutDate) values ('" + Client + "Farmers', '" + mobileno + "', '" + Client + "', '" + Client + " Subject', " +
                                                "'" + SMS + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                                                lstwrmsadmin.Add(new WrmsAdmin() { ddat = DateTime.Now, PhoneNo = mobileno, Msg = MessageType + "_SMSSender" });
                                                execQuery("insert into mfi.wrmsadminmsg(Msg, PhoneNo, ddat) values ('" + MessageType + "_SMSSender', '" + mobileno + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");


                                            }
                                        }
                                        if (SMS2 != "")
                                        {
                                            List<WrmsAdmin> lst = lstwrmsadmin.FindAll(a => a.PhoneNo == mobileno && a.Msg == MessageType + "_SMSSender2");
                                            if (!(lst.Count > 0))
                                            {
                                                execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel, " +
                                               " OutDate) values ('" + Client + "Farmers', '" + mobileno + "', '" + Client + "', '" + Client + " Subject', " +
                                               "'" + SMS2 + "', 'Pending', 'Unicode', 'Gateway2', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                                                lstwrmsadmin.Add(new WrmsAdmin() { ddat = DateTime.Now, PhoneNo = mobileno, Msg = MessageType + "_SMSSender2" });
                                                execQuery("insert into mfi.wrmsadminmsg(Msg, PhoneNo, ddat) values ('" + MessageType + "_SMSSender2', '" + mobileno + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");

                                            }
                                        }

                                    }

                                }
                            }

                            if (SMS != "" )
                                execQuery("insert into mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate, MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + forecassmsstats + "', '" + SMS.Trim() + "', '" + MessageType + "', '" + MessageType + "')");
                            if (SMS2 != "")
                                execQuery("insert into mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate,MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + forecassmsstats + "', '" + SMS2.Trim() + "', '" + MessageType + "','forecastRain')");

                            if (SMS != "" && SMS2 != "")
                                Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                        }
                        else if (SendingType == "Day" && stateweatherExpriy.Year != 1 && SMS != "" && stateweatherExpriy.Date>DateTime.Now.AddDays(1).Date)
                        {

                            DataTable frmstmsg = new DataTable();
                            //Check Message and Expriy date exists in farmsmsmaster
                            frmstmsg = getData("select * from mfi.farm_sms_status_master where FarmID='" + FarmID + "' and Message='" + SMS + "' and Date(StateWeatherExpiry)='" + stateweatherExpriy.Date.ToString("yyyy-MM-dd") + "'");
                            if (frmstmsg.Rows.Count == 0)
                            {

                                //frmstmsg = getData("select * from mfi.farm_sms_status_master where FarmID='" + FarmID + "' and Message='" + SMS + "' and MsgStatus='Pending'");
                                //if (frmstmsg.Rows.Count > 0)
                                //{
                                //    string updatefrmsg = "update mfi.farm_sms_status_master set StateWeatherExpiry='" + stateweatherExpriy.Date.ToString("yyyy-MM-dd") + "',MsgStatus='Pending' where FarmID='" + FarmID + "' and Message='" + SMS + "'";
                                //    execQuery(updatefrmsg);
                                //}
                               
                                   execQuery("Delete from mfi.farm_sms_status_master  where farmid = '" + FarmID + "' and Date(StateWeatherExpiry) != '' and Date(StateWeatherExpiry) is not null and MessageType='"+ MessageType + "' and Id > 0");
                                    string insertfrmmsg = "insert into mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate,MsgStatus, Message, MessageType,DummyMessageType,StateWeatherExpiry) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', 'Pending', '" + SMS.Trim() + "', '" + MessageType + "','" + MessageType + "','" + stateweatherExpriy.Date.ToString("yyyy-MM-dd") + "')";
                                    execQuery(insertfrmmsg);
                               
                            }

                        }

                        else if (SendingType == "Floating" && SowingDate.Year != 1 && (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= SowingDate.AddDays(floatdays))))
                        {

                            if (SMS != "")
                            {
                                string[] popmessage = SMS.Split(';');
                                if (popmessage.Length == 1)
                                {
                                    execQuery("insert mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate, MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', 'Pending', '" + SMS.Trim() + "', '" + MessageType + "', '" + MessageType + "')");
                                }
                                if (popmessage.Length > 1)
                                {
                                    for (int l = 0; l < popmessage.Length; l++)
                                    {


                                        execQuery("insert mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate, MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', 'Pending', '" + popmessage[l].Trim() + "', '" + MessageType + "', '" + MessageType + "')");

                                    }
                                }
                            }
                            if (SMS2 != "")
                                execQuery("insert mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate,MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', 'Pending', '" + SMS2.Trim() + "', '" + MessageType + "', '" + MessageType + "')");

                            if (SMS != "" && SMS2 != "")
                                Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                        }
                        if (SMS != "" && stateweatherExpriy.Year==1 && flagoutturnforecast.ToLower() == "no")
                            execQuery("insert into mfi.farm_sms_lstsend (FarmID, LogDate, MessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + MessageType + "')");
                        if (SMS != "" && stateweatherExpriy.Year != 1 && flagoutturnforecast.ToLower() == "no")
                            execQuery("insert into mfi.farm_sms_lstsend (FarmID, LogDate, MessageType,StateExperiyDate) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', '" + MessageType + "','"+stateweatherExpriy.ToString("yyyy-MM-dd")+"')");

                    }
                }
            }
        }






        public DataTable GetPrefredlanguage(string id)
        {
            string qry = "SELECT forecast_Preferredlan,Pop_Preferredlan,forecastweatherflg FROM mfi.clientcode where ClientID='" + id + "'";
            DataTable DT = new DataTable();
            bool ConnFound = Connection();

            try
            {
                MySqlDataAdapter Adpter = new MySqlDataAdapter(qry, conn);
                DataSet DataSet = new DataSet();
                Adpter.Fill(DataSet);

                if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                {
                    conn.Close();
                    return DT;
                }
                DT = DataSet.Tables[0];

                conn.Close();
                return DT;


            }
            catch (Exception ex)
            {
                conn.Close();
                return DT;
            }
            finally { conn.Close(); };
        }

        private DataTable getFarmLastMessage(string farmid, string messageType)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();

            try
            {
                if (ConnFound)
                {
                    string sql = "select * from mfi.farm_sms_status_master where FarmID = " + farmid + " and MessageType = '" + messageType + "' and MsgStatus != 'Expired' order by ScheduleDate desc limit 1 ";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;
                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }
        }

        private bool chkFarmProcessed(string farmid, string messageType,DateTime stateexpDate)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();
            string sql = "";
            try
            {
                if (ConnFound)
                {
                    if(stateexpDate.Year==1)
                     sql = "select LogDate from mfi.farm_sms_lstsend where FarmID = " + farmid + " and MessageType = '" + messageType + "'";
                    else if(stateexpDate.Year != 1)
                        sql = "select LogDate from mfi.farm_sms_lstsend where FarmID = " + farmid + " and MessageType = '" + messageType + "' and Date(StateExperiyDate)='"+ stateexpDate.ToString("yyyy-MM-dd") + "'";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return false;
                    }
                    DT = DataSet.Tables[0];
                    conn.Close();
                    DateTime dtLogDate = new DateTime();
                    DateTime.TryParse(DT.Rows[0]["LogDate"].ToString(), out dtLogDate);
                    if (dtLogDate.Date == DateTime.Now.Date)
                        return true;
                    else
                        return false;

                }
                else
                {
                    return false;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return false;
            }

            throw new NotImplementedException();
        }

        private void InsertInFarmsatausMaster(string farmID, string messageType)
        {
            string insertdate = "insert into mfi.farm_sms_status_master (FarmID,MessageType,MsgStatus) values ('" + farmID + "','" + messageType + "','sent')";
            execQuery(insertdate);

        }

        private DataTable GetMicrnutrientValues(string villageID)
        {
            DataTable DT = new DataTable();
            bool ConnFound = Connection();


            try
            {
                if (ConnFound)
                {
                    //string sql = "select ggrc.*,Date_format(sen.SowingDate,'%Y-%m-%d') SowingDateValue,(ymin+ymax)/2 Latitude, (xmin+xmax)/2 Longitude,CropID from wrserver1.yfi_ggrc ggrc left join test.sentinel_village_master sen on ggrc.VillageID = sen.Village_ID";
                    string sql = "SELECT zn_mean,s_mean,fe_mean,cu_mean,b_mean FROM mfi.soil_data_mean where village_code=(select village_code from mfi.soil_village_master where Sentinel_VillageID ='" + villageID + "');";

                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    Adpter.SelectCommand.CommandTimeout = 1800;
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }






            throw new NotImplementedException();
        }

        private string CheckSmsSatausForsoilDeficeint(string farmID, string mstype)
        {
            bool ConnFound = Connection();
            string curtstatus = "";
            try
            {
                if (ConnFound)
                {

                    string checktableentry = "SELECT MsgStatus FROM mfi.farm_sms_status_master where FarmID='" + farmID + "' and MessageType='" + mstype + "'";
                    MySqlCommand command = new MySqlCommand(checktableentry, conn);
                    MySqlDataReader rd1 = command.ExecuteReader();
                    if (rd1.HasRows)
                    {
                        while (rd1.Read())
                        {
                            curtstatus = rd1["Current_Status"].ToString();
                        }
                    }
                }
                conn.Close();
            }
            catch (Exception ex)
            {

            }

            return curtstatus;


        }

        private DataTable getDabwaliFarms(string ID, string client)
        {


            DataTable DT = new DataTable();
            bool ConnFound = Connection();
            string sql = "";

            try
            {
                if (ConnFound)
                {
                    //string sql = "select ggrc.*,Date_format(sen.SowingDate,'%Y-%m-%d') SowingDateValue,(ymin+ymax)/2 Latitude, (xmin+xmax)/2 Longitude,CropID from wrserver1.yfi_ggrc ggrc left join test.sentinel_village_master sen on ggrc.VillageID = sen.Village_ID";
                    //  string sql = "select info.ID as FarmID,info.District,info.VillageID,vmaster.Village_Final as VillageName,info.state,info.FarmerName,(info.MaxLat+info.MinLat)/2 as latitude, (info.MaxLon+info.MinLon)/2 as longitude,Date(fcrp.CropFrom) as sowingdate from mfi.clientfarmmapping2 as map,wrserver1.yfi_farminfo as info,wrserver1.yfi_farmcrop as fcrp ,test.sentinel_village_master vmaster  where map.FarmID=info.id and map.FarmID=fcrp.FarmID and map.ClientID='100894' and info.VillageID=vmaster.Village_ID";



                    sql = "select fcrop.cropid,map.FarmID,gfs.RefId,vm.District,info.VillageID,vm.Village_Final as VillageName,vm.VillageName_Hindi,vm.District,vm.Sub_district,vm.WRMS_StateID," +
                        "info.state,info.FarmerName,info.PhoneNumber,(info.MaxLat+info.MinLat)/2 as latitude, (info.MaxLon+info.MinLon)/2 as " +
                        "longitude,Date(fcrop.CropFrom) as sowingdate from mfi.clientfarmmapping2 map left join wrserver1.yfi_farminfo info " +
                        "on map.FarmID=info.ID left	join wrserver1.yfi_farmcrop fcrop on fcrop.FarmID=map.FarmID left join test.sentinel_village_master vm " +
                        "on vm.Village_ID=info.VillageID left join wrwdata.mfi_gfs_farm as gfs on info.ID = gfs.Id  where map.ClientID='" + ID + "'";

                    //else
                    //{
                    //    sql = "select distinct info.ID as FarmID,(info.MaxLat+info.MinLat)/2 as latitude, (info.MaxLon+info.MinLon)/2 as longitude," +
                    //        "Date(fcrop.CropFrom) as sowingdate from mfi.clientfarmmapping2 map left join wrserver1.yfi_farminfo info on " +
                    //        "map.FarmID=info.ID left join wrserver1.yfi_farmcrop fcrop on fcrop.FarmID=map.FarmID " +
                    //        "where map.ClientID='"+ID+"'";
                    //}


                    MySqlDataAdapter Adpter = new MySqlDataAdapter(sql, conn);
                    Adpter.SelectCommand.CommandTimeout = 1800;
                    DataSet DataSet = new DataSet();
                    Adpter.Fill(DataSet);

                    if (DataSet.Tables.Count == 0 || DataSet.Tables[0].Rows.Count == 0)
                    {
                        conn.Close();
                        return DT;
                    }
                    DT = DataSet.Tables[0];

                    conn.Close();
                    return DT;

                }
                else
                {
                    return DT;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return DT;
            }


        }

        private double GetRemainamountofwater(string fID, string tbname)
        {
            bool ConnFound = Connection();
            double water = 0; ;
            try
            {
                if (ConnFound)
                {

                    string checktableentry = "SELECT Supplied_Water FROM " + tbname + " where FarmID='" + fID + "'";
                    MySqlCommand command = new MySqlCommand(checktableentry, conn);
                    MySqlDataReader rd1 = command.ExecuteReader();
                    if (rd1.HasRows)
                    {
                        while (rd1.Read())
                        {
                            water = Convert.ToDouble(rd1["Supplied_Water"].ToString());
                        }
                    }
                }
                conn.Close();
            }
            catch (Exception ex)
            {

            }

            return water;
        }

        private void updatecropcurrentstatus(string cropid, string villageID, string ctsataus)
        {
            bool ConnFound = Connection();

            try
            {
                if (ConnFound)
                {

                    string updatecurrenstatus = "update mfi.cotton_cropmessage set Current_Status='" + ctsataus + "'  where CropID='" + cropid + "' and VillageID='" + villageID + "' ";
                    execQuery(updatecurrenstatus);
                }
                conn.Close();
            }
            catch (Exception ex)
            {

            }


        }

        private string GetCropCurrentsataus(string cropid, string villageID)
        {
            bool ConnFound = Connection();
            string curtstatus = "";
            try
            {
                if (ConnFound)
                {

                    string checktableentry = "SELECT Current_Status FROM mfi.cotton_cropmessage where CropID='" + cropid + "' and VillageID='" + villageID + "' ";
                    MySqlCommand command = new MySqlCommand(checktableentry, conn);
                    MySqlDataReader rd1 = command.ExecuteReader();
                    if (rd1.HasRows)
                    {
                        while (rd1.Read())
                        {
                            curtstatus = rd1["Current_Status"].ToString();
                        }
                    }
                }
                conn.Close();
            }
            catch (Exception ex)
            {

            }

            return curtstatus;
        }

        private void Step2(string farmID, double Latitude, double Longitude, string Village, string Client, DateTime chkdate, double EVrate, out string SMS, string smsstatus, string tbname, double totwater)
        {
            SMS = "";
            double rainrange = 25;
            double balancewater = 0;
            double currentwater = 0;
            double watertobesupplied = 0;
            double totalrain = GetCropRainData(Latitude, Longitude, Village, "test");

            double totalrainforcast = getGGRCCropForecastSMS(Latitude, Longitude, Village, "test", 2);
            //totalrainforcast=20;
            if (totalrainforcast > 0)
            {
                if (smsstatus == "" || smsstatus == "step2")
                {
                    updatecropsmsstatus(farmID, "step2_1", "Cotton_Irrigation", tbname);
                    SMS = "Rainfall expected, please do not irrigate in next 3 days";
                }
                else if (smsstatus == "step2_1")
                {
                    updatecropsmsstatus(farmID, "step2", "Cotton_Irrigation", tbname);
                    SMS = "Rainfall expected, please do not irrigate in next 3 days";
                }

            }

            else
            {
                double watersupplied = GetRemainamountofwater(farmID, tbname);
                if (watersupplied < totalwater)
                {
                    currentwater = .5 * (rainrange + EVrate - totalrain);
                    balancewater = totalwater - watersupplied;
                    if (currentwater <= balancewater)
                    {
                        watertobesupplied = currentwater;
                        watersupplied = watersupplied + currentwater;
                    }
                    else
                    {
                        watertobesupplied = currentwater - balancewater;
                        watersupplied = watersupplied + watertobesupplied;
                    }
                    SMS = "Please release '" + Math.Round(watertobesupplied, 0) + "' mm of rainfall";
                    updatebalancecropwater(farmID, watersupplied, tbname);
                    updatecropsmsstatus(farmID, "step2", "Cotton_Irrigation", tbname);
                }

            }

        }

        private void updatebalancecropwater(string ID, double balance, string table)
        {
            bool ConnFound = Connection();

            try
            {
                if (ConnFound)
                {
                    string balanceststus = "update " + table + " set Supplied_Water='" + balance + "' where FarmID='" + ID + "'";
                    execQuery(balanceststus);
                }
                conn.Close();
            }
            catch (Exception ex)
            {

            }
            finally { conn.Close(); }

        }




        private Boolean step1(string farmID, double Latitude, double Longitude, string Village, string Client, DateTime check_date, double EVRate, out string SMS, string smsStatus, string tblname)
        {
            SMS = "";
            Boolean flag = false;
            double totalrain = GetCropRainData(Latitude, Longitude, Village, "test");
            if (totalrain > 25)
            {
                SMS = "Please irrigate according to soil moisture in your farm as rainfall could have happened in last few days";
                updatecropsmsstatus(farmID, "step1", "Cotton_Irrigation", tblname);
                flag = true;
                return flag;
            }
            else
            {
                Step2(farmID, Latitude, Longitude, Village, Client, check_date, EVRate, out SMS, smsStatus, tblname, totalwater);
            }
            return flag;
        }



        private void updatecropsmsstatus(string fID, string step, string msgtype, string tbname)
        {
            string updatesmsstaus = "";

            try
            {

                if (step == "step1" || step == "step4")
                {
                    updatesmsstaus = "sent";
                }
                if (step == "step2_1")
                {
                    updatesmsstaus = "step2_1";
                }
                if (step == "step2_2")
                {
                    updatesmsstaus = "step2_2";
                }
                if (step == "step2_3")
                {
                    updatesmsstaus = "step2";
                }
                if (step == "step2")
                {
                    updatesmsstaus = "step2";
                }
                string checksmsstatus = "update " + tbname + " set SMS_Status='" + updatesmsstaus + "',Check_Date='" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' where  FarmID='" + fID + "'";
                execQuery(checksmsstatus);
            }
            catch (Exception ex)
            {

            }



        }

        private string GetSmsStatus(string farmID, string msgtype, string tbname)
        {
            bool ConnFound = Connection();
            string sms = "";
            try
            {
                if (ConnFound)
                {
                    string checksmsstatus = "select SMS_Status from " + tbname + " where  FarmID='" + farmID + "'";
                    MySqlCommand sqlCommand = new MySqlCommand(checksmsstatus, conn);
                    MySqlDataReader dataReader = sqlCommand.ExecuteReader();
                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            sms = dataReader["SMS_Status"].ToString();
                        }
                    }
                }
                conn.Close();
            }
            catch (Exception ex)
            {

            }
            finally { conn.Close(); }

            return sms;

        }

        private DateTime GetdateOfCrop(string farmID, string msgtpe, string tblname)
        {
            bool ConnFound = Connection();
            DateTime date = new DateTime();
            try
            {
                if (ConnFound)
                {

                    string checktableentry = "SELECT Check_Date FROM " + tblname + " where  FarmID='" + farmID + "'";
                    MySqlCommand command = new MySqlCommand(checktableentry, conn);
                    MySqlDataReader rd1 = command.ExecuteReader();
                    if (rd1.HasRows)
                    {
                        while (rd1.Read())
                        {
                            date = Convert.ToDateTime(rd1["Check_Date"].ToString());
                        }
                    }
                    rd1.Close();
                }

                conn.Close();
            }
            catch (Exception ex)
            {

            }
            if (date.Year == 1)
            {
                string insertdate = "insert into " + tblname + " (FarmID,Check_Date) values ('" + farmID + "','" + DateTime.Now.Date.ToString("yyyy-MM-dd") + "')";
                execQuery(insertdate);
            }

            return date;
        }



        private double GetMeanEt(string month, string stateID)
        {
            bool ConnFound = Connection();
            double mean = 0;

            try
            {
                if (ConnFound)
                {
                    string qry = "select Mean_ET from test.evaporation_ratemaster where Month='" + month + "' and StateId='" + stateID + "'";
                    MySqlCommand command = new MySqlCommand(qry, conn);
                    MySqlDataReader rd = command.ExecuteReader();
                    if (rd.HasRows)
                    {
                        while (rd.Read())
                        {
                            mean = Convert.ToDouble(rd["Mean_ET"].ToString());
                        }
                    }
                }
                conn.Close();
            }
            catch (Exception ex)
            {

            }
            finally { conn.Close(); }
            return mean;
        }

        void SMSSchedulerMain_GGRC()
        {
            execQuery("delete from mfi.ggrcvillagesms where LogDate < '" + DateTime.Now.AddDays(-14).ToString("yyyy-MM-dd") + "'");
            execQuery("delete from mfi.sms_lastsend where date(LogDate) < '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");

            DataTable DTVillages = new DataTable();
            DTVillages = getGGRCVillages();
            string Client = "ggrc";

            DataTable DTSMSTypes = getSMSMaster(Client);


            for (int i = 0; i < DTVillages.Rows.Count; i++)
            {
                string SowingDate = DTVillages.Rows[i]["SowingDateValue"].ToString();
                string VillageID = DTVillages.Rows[i]["ID"].ToString();
                string Village = DTVillages.Rows[i]["Village"].ToString();
                string District = DTVillages.Rows[i]["District"].ToString();
                string Block = DTVillages.Rows[i]["Taluka"].ToString();
                Console.WriteLine("Starting for " + Village + "(" + i + " / " + DTVillages.Rows.Count + ") at " + DateTime.Now);
                double Latitude = DTVillages.Rows[i]["Latitude"].ToString().doubleTP();
                double Longitude = DTVillages.Rows[i]["Longitude"].ToString().doubleTP();
                if (!(District == "PORBANDAR"))
                    continue;

                for (int j = 0; j < DTSMSTypes.Rows.Count; j++)
                {

                    string MessageType = DTSMSTypes.Rows[j]["MessageType"].ToString();
                    if (!(MessageType == "Custom_ZY4BBB5RF9JHWPJV"))
                        continue;
                    string MessageVillageID = DTSMSTypes.Rows[j]["VillageID"].ToString();
                    string MessageDistrict = DTSMSTypes.Rows[j]["DistrictID"].ToString();
                    string MessageBlock = DTSMSTypes.Rows[j]["BlockID"].ToString();
                    if (!(MessageType.ToLower().Contains("custom")))
                    {
                        if (Latitude == 0)
                            continue;
                    }

                    DateTime DTSowingDate = new DateTime();
                    if (SowingDate != "")
                    {
                        string[] ArrSowingDate = SowingDate.Split('-');
                        DTSowingDate = new DateTime(Convert.ToInt32(ArrSowingDate[0]), Convert.ToInt32(ArrSowingDate[1]), Convert.ToInt32(ArrSowingDate[2]));
                    }

                    if (MessageType.ToLower().Contains("custom"))
                    {
                        bool flgMessageSend = false;
                        if (VillageID == MessageVillageID || (MessageVillageID == "0" && MessageBlock == Block) || (MessageVillageID == "0" && MessageBlock == "0" && MessageDistrict == District) || (MessageVillageID == "0" && MessageBlock == "0" && MessageDistrict == "0"))
                            flgMessageSend = true;

                        if (!flgMessageSend)
                            continue;
                    }
                    //else
                    //  continue;
                    string SendingType = DTSMSTypes.Rows[j]["SendingType"].ToString();
                    int SendingFrequency = 0;
                    int.TryParse(DTSMSTypes.Rows[j]["SendingFrequency"].ToString(), out SendingFrequency);
                    DateTime SendingDate = new DateTime();
                    DateTime.TryParse(DTSMSTypes.Rows[j]["SendingDate"].ToString(), out SendingDate);
                    DateTime dtLastSentDate = new DateTime();
                    string CustomMessage = DTSMSTypes.Rows[j]["Message"].ToString();
                    string FloatingDays = DTSMSTypes.Rows[j]["FloatingDays"].ToString();
                    string Status = "";
                    string SMS = "";
                    string SMS2 = "";
                    int floatdays = 0;
                    if (FloatingDays != "")
                    {
                        int.TryParse(FloatingDays, out floatdays);
                    }

                    if (chkVillageProcessed(VillageID, MessageType))
                        continue;

                    DataTable DTLastMessage = getVillageLastMessage(VillageID, MessageType);

                    if (DTLastMessage.Rows.Count > 0)
                    {
                        DateTime.TryParse(DTLastMessage.Rows[0]["ScheduleDate"].ToString(), out dtLastSentDate);
                        Status = DTLastMessage.Rows[0]["Status"].ToString();
                    }

                    bool flgDoProcess = false;

                    if (SendingType == "Day")
                        flgDoProcess = (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency)));
                    else if (SendingType == "Floating")
                        flgDoProcess = (DateTime.Now.Date == DTSowingDate.AddDays(floatdays) && Status == "");
                    else
                        flgDoProcess = (DateTime.Now >= SendingDate && SendingDate > DateTime.Now.AddDays(-15) && Status == "");

                    if (!flgDoProcess)
                        continue;
                    string ss = "";
                    if (MessageType == "forecast")
                    {
                        SMS = getGGRCForecastSMS(Latitude, Longitude, Village, Client, "", out SMS2, "",out ss);
                    }
                    if (MessageType == "PHLow")
                    {
                        string Ph = "";
                        Ph = getGGRCSoilAnalysis(Village, Latitude, Longitude, "PH");
                        if (!(Ph.doubleTP() < 5.5))
                            continue;
                        SMS = CustomMessage;

                    }
                    if (MessageType == "PHHigh")
                    {
                        string Ph = "";
                        Ph = getGGRCSoilAnalysis(Village, Latitude, Longitude, "PH");
                        if (!(Ph.doubleTP() > 8.0))
                            continue;
                        SMS = CustomMessage;
                    }
                    if (MessageType == "SoilTexture")
                    {
                        string st = "";
                        st = getGGRCSoilAnalysis(Village, Latitude, Longitude, "SoilTexture");
                        if (!(st.ToLower().Contains("sandy")))
                            continue;
                        SMS = CustomMessage;
                    }
                    if (MessageType == "NDVI")
                    {
                        DataTable DTNDVI = new DataTable();
                        DTNDVI = getGGRCNDVI(VillageID, DTSowingDate.AddDays(90), DTSowingDate.AddDays(120));
                        string NDVIValue = DTNDVI.Rows[0][0].ToString().Trim();
                        if (NDVIValue == "")
                            continue;
                        double NDVI = NDVIValue.doubleTP();
                        if (!(NDVI < 0.5))
                            continue;
                        SMS = CustomMessage;
                    }
                    else if (MessageType == "weather")
                    {
                        SMS = getGGRCWeatherDataSMS(Latitude, Longitude, Village, Client, "");
                    }
                    else if (MessageType == "disease")
                    {
                        List<string> lstSMS = getGGRCDiseaseSMS(Latitude, Longitude, Village, Client);
                        if (lstSMS.Count > 0)
                        {
                            SMS = lstSMS[0];
                            SMS2 = lstSMS[1];
                        }
                    }
                    else if (MessageType == "planthealth")
                    {
                        SMS = getGGRCMoistureSMS(VillageID, Village, Client);
                    }
                    else if (MessageType == "raindeficit")
                    {
                        if (Client == "ggrc")
                            SMS = "????? ????? ???????????? ?????? ?????? ??????? ????? ????? ?? ?? ???? ??????? ??? (???? ???- ???? ?? ??? ?? ?? ?? ????? ????) ?? ??? ???? ?????????? ??.";
                        else
                            SMS = "Repeated hoeing is advisable to conserve the soil moisture under scanty rainfall conditions.";
                    }
                    else if (MessageType == "leafreddening")
                    {
                        if (Client == "ggrc")
                            SMS = "??? ???? ??? ?????? ??? ????? ???? ?.?% ?????????? ?????? ?????? ??? ?? ???? ??? ??? ????.";
                        else
                            SMS = "Application of 0.2 % Magnesium sulphate at square formation and boll formation to reduce leaf reddening.";
                    }
                    else if (MessageType == "flower1" || MessageType == "flower2" || MessageType == "flower3")
                    {
                        if (Client == "ggrc")
                            SMS = " ?????? ??? ????, ??? ?????? ????????? ?????? ???????? ???????? 2 ????????? / 100 ???? ??????  ??????? ?? ?????? ??????? ?????.";
                        else
                            SMS = "For yield maximization, foliar application of Potassium Nitrate @ 2kg/100 lit of water at flower initiation stage would be benificial.";
                    }
                    else if (MessageType == "weedemergence")
                    {
                        if (Client == "ggrc")
                            SMS = "????? ???????: ???? ??????? ????????? ???? ???? ??? ??? ??????? ???? ????.";
                        else
                            SMS = "Weed Management: Intercultural operations should be carried out to control emerging weeds.";
                    }
                    else if (MessageType == "flowertoball")
                    {
                        if (Client == "ggrc")
                            SMS = "??? ??? ??? ??? ??? ????? : NAA (???????????) ?? ?????? : ???? ??? ??? ?????? ??? ???????? ????? ????? ??? ??? ?????? ???? ??? ?? 15 ???? ???? ???????  3.5 ???? NAA ?? ?????? ????.";
                        else
                            SMS = "Flower/ Bud dropping: Spray Of NAA(Planofix): Spray NAA @ 3.5 ml per 15 litres of water on the crop to retain more number of squares and flowers and to increase the yield.";
                    }
                    else if (MessageType.ToLower().Contains("custom"))
                        SMS = CustomMessage;



                    if (SendingType == "Date" && DateTime.Now >= SendingDate && Status == "")
                    {
                        if (SMS != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "')");
                        if (SMS2 != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + SendingDate.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "')");
                        if (SMS != "" && SMS2 != "")
                            Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    }
                    else if (SendingType == "Day" && (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= dtLastSentDate.Date.AddDays(SendingFrequency))))
                    {
                        execQuery("update mfi.ggrcvillagesms set Status = 'Expired' where VillageID = " + VillageID + " and MessageType = '" + MessageType + "'");


                        if (SMS != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate, Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "')");
                        if (SMS2 != "")
                            execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "')");

                        if (SMS != "" && SMS2 != "")
                            Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    }
                    else if (SendingType == "Floating" && (dtLastSentDate.Year == 1 || (DateTime.Now.Date >= DTSowingDate.AddDays(floatdays))))
                    {
                        //execQuery("update mfi.ggrcvillagesms set Status = 'Expired' where VillageID = " + VillageID + " and MessageType = '" + MessageType + "'");


                        //if (SMS != "")
                        //    execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate, Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS + "', '" + MessageType + "')");
                        //if (SMS2 != "")
                        //    execQuery("insert into mfi.ggrcvillagesms (VillageID, ScheduleDate, LogDate,Status, Message, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', 'Pending', '" + SMS2 + "', '" + MessageType + "')");

                        //if (SMS != "" && SMS2 != "")
                        //    Console.WriteLine(Village + " " + SendingType + " " + MessageType + " added (" + j + "/" + DTSMSTypes.Rows.Count + ") at " + DateTime.Now);
                    }
                    // execQuery("insert into mfi.sms_lastsend (VillageID, LogDate, MessageType) values ('" + VillageID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + MessageType + "')");

                }
            }
        }








        public bool execQuery(string Query)
        {
            bool ConnFound = Connection();
            string sql = "";
            try
            {
                if (ConnFound)
                {

                    sql = Query;

                    MySqlCommand Cmd = new MySqlCommand(sql, conn);
                    if (Cmd.ExecuteNonQuery() > 0)
                    {
                        conn.Close();
                        return true;
                    }
                    else
                    {
                        conn.Close();
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
            catch (Exception e)
            {
                conn.Close();
                return false;
            }

        }

        public void PepsicoWork()
        {

        }

        public List<string> getGGRCDiseaseSMS(double Latitude, double Longitude, string Village, string Client)
        {
            List<string> objResult = new List<string>();
            try
            {
                WebClient wc = new WebClient();

                //string apiAddrForecast = "http://54.174.231.79:82/wdrest.svc/MergeWZDailyForecast_v2/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/English/json/wrinternal";;
                string apiAddrForecast = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "/District/json/internal";
                string apiAddrWeather = "http://54.174.231.79:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-2).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no"; ;

                //  string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(4).ToString("yyyy-MM-dd") + "/District/json/internal";
                string strForecast = wc.DownloadString(apiAddrForecast);
                string strWeather = wc.DownloadString(apiAddrWeather);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");
                DataTable DTForecastFUll = new DataTable();
                DataTable DTWeatherFull = new DataTable();
                DataTable DTForecast = new DataTable();
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                string objWeather = JsonConvert.DeserializeObject<string>(strWeather);
                if (objFOrecast != "no data" && objWeather != "no data")
                {
                    DTForecastFUll = JsonConvert.DeserializeObject<DataTable>(objFOrecast);
                    DTWeatherFull = JsonConvert.DeserializeObject<DataTable>(objWeather);

                    for (int k = 0; k < DTWeatherFull.Rows.Count; k++)
                    {
                        DataRow ForecastRow = DTForecastFUll.NewRow();
                        DTForecastFUll.Rows.Add(ForecastRow);
                        int count = DTForecastFUll.Rows.Count;
                        string datetime = Convert.ToDateTime(DTWeatherFull.Rows[k]["DateTime"]).ToString("dd-MMM-yyyy");
                        string maxhumidity = DTWeatherFull.Rows[k]["HumMor"].ToString();
                        string minhumidity = DTWeatherFull.Rows[k]["HumEve"].ToString();
                        string maxTemp = DTWeatherFull.Rows[k]["MaxTemp"].ToString();
                        DTForecastFUll.Rows[count - 1]["DateTime"] = datetime;
                        DTForecastFUll.Rows[count - 1]["MaxHumidity"] = maxhumidity;
                        DTForecastFUll.Rows[count - 1]["MinHumidity"] = minhumidity;
                        DTForecastFUll.Rows[count - 1]["MaxTemp"] = maxTemp;

                    }

                    DTForecastFUll.DefaultView.Sort = "DateTime";
                    DTForecastFUll = DTForecastFUll.DefaultView.ToTable();




                    //DTForecastFUll.Merge(DTWeatherFull);
                    bool flgCondition = true;
                    int counter = 0;
                    for (int i = 0; i < DTForecastFUll.Rows.Count; i++)
                    {
                        double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                        double HumidityMor = DTForecastFUll.Rows[i]["MaxHumidity"].ToString().doubleTP();
                        double HumidityEve = DTForecastFUll.Rows[i]["MinHumidity"].ToString().doubleTP();

                        if (MaxTemp < 33 || HumidityMor > 70 || HumidityEve < 40)
                        {
                            flgCondition = false;
                            counter = 0;
                        }
                        else
                        {
                            counter++;
                            if (counter >= 4)
                                flgCondition = true;
                        }
                    }
                    string PreventiveMessage = "";
                    if (Client == "cottonadvisory18")
                        PreventiveMessage = "श्रीमान्, आपके क्षेत्र में पिंक बालवर्म के आक्रमण के अनुकूल मौसम की स्थिति बन रही है कृपया अपने खेत  का निरीक्षण करें और उचित कार्यवाही करें |";
                    else if (Client == "ggrc")
                        PreventiveMessage = "?. ?????? ????? ????? ??????? ????????? ???? ???? ???????? ??????? ???? (??????? ?????) ????? ??.  ?? ??? ??????? ??????." +
                               "?.  ?????? ?????? ?????? ??????? ???? ???? ???? ????? ????? ??? 8 ????? ???? ?????  ?? ??????????  ?????? ????";
                    else
                        PreventiveMessage = "1. To install gossyplure pheromone baited traps @ 20 nos./ha to arrest the population development of PINK BOLLWORM. " +
                        "2. At economic threshold of 8 moths per trap per night for three consecutive nights, an insecticidal spray in the field is desired.";
                    string CurativeMessage = "";
                    if (Client == "cottonadvisory18")
                        CurativeMessage = "";
                    else if (Client == "ggrc")
                        CurativeMessage = "?????? ????? ????????? ????  ??? ??? ??????????? ??% EC ? ???? / ??. ???? ?????????? ??% EC ? ????/??. ???? ??????????? ??% WP ? ?????/ ???? ???? ??????? ????? ?????? ???? ??? ????? ??.";
                    else if (Client == "GIZ")
                        CurativeMessage = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರೇ,ಹೇನಿನಂತಹ ಕೀಟಗಳ ಬೆಳವಣಿಗೆಗೆ ಅನುಕೂಲಕರವಾದ ವಾತಾವರಣವು ನಿರ್ಮಾಣವಾಗುತ್ತಿದ್ದರೆ,ದಯವಿಟ್ಟು ನಿಮ್ಮ ಜಮೀನನ್ನು ಕೀಟದ ಹೊರೆಯ ಸಂಭಂದಿತವಾಗಿ ಗಮನಿಸಿ ಮತ್ತು ಕೀಟನಾಶಕವನ್ನು ಸಿಂಪಡಿಸಿ ಮತ್ತು ನಮ್ಮ ಕೃಷಿ ತಜ್ಞರನ್ನು ಸಂಪರ್ಕಿಸಿ.";
                    else
                        CurativeMessage = "To control  PINK BOLLWORM foliar application of Profenophos 50%EC @ 3 ml /lit or or Triazophos 40% EC @3ml/lit or Thiodicarb 75%WP @ 4 gms/lit of water is recommended.";

                    if (flgCondition)
                    {
                        objResult.Add(PreventiveMessage);
                        objResult.Add(CurativeMessage);
                    }
                }
                return objResult;
            }
            catch (Exception ex)
            {
                return objResult;
            }
        }
        public bool chkLateBlightCondition(double Latitude, double Longitude, string Village, string Client)
        {
            bool flgCondition = false;
            List<string> objResult = new List<string>();
            try
            {
                WebClient wc = new WebClient();
                string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.AddDays(1).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(10).ToString("yyyy-MM-dd") + "/District/json/internal";
                string strForecast = wc.DownloadString(apiAddr);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");
                DataTable DTForecastFUll = new DataTable();
                DataTable DTForecast = new DataTable();
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                if (objFOrecast != "no data")
                {
                    DTForecastFUll = JsonConvert.DeserializeObject<DataTable>(objFOrecast);

                    int DiseaseCtr = 0;
                    for (int i = 0; i < DTForecastFUll.Rows.Count; i++)
                    {
                        double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                        double MinTemp = DTForecastFUll.Rows[i]["MinTemp"].ToString().doubleTP();
                        double HumidityMor = DTForecastFUll.Rows[i]["MaxHumidity"].ToString().doubleTP();
                        double HumidityEve = DTForecastFUll.Rows[i]["MinHumidity"].ToString().doubleTP();
                        double AvgHumidity = (HumidityMor + HumidityEve) / 2;
                        if (MaxTemp >= 10 && MaxTemp <= 25 && MinTemp <= 10 && AvgHumidity >= 90)
                        {
                            DiseaseCtr++;
                            if (DiseaseCtr == 3)
                                flgCondition = true;
                        }
                        else
                            DiseaseCtr = 0;
                    }

                }
                return flgCondition;
            }
            catch (Exception ex)
            {
                return flgCondition;
            }
        }

        public List<DieaseDates> getGGRCDiseasePinkBollwormSMS(double Latitude, double Longitude, string Village, string Client, DataTable DT)
        {
            //List<string> objResult = new List<string>();
            List<DieaseDates> lstDieaseDates = new List<DieaseDates>();
            DataTable DTForecastFUll = new DataTable();
            DTForecastFUll.Merge(DT);

            try
            {
                WebClient wc = new WebClient();



                string apiAddrWeather = "http://54.174.231.79:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no"; ;

                //  string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(4).ToString("yyyy-MM-dd") + "/District/json/internal";

                string strWeather = wc.DownloadString(apiAddrWeather);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");

                DataTable DTWeatherFull = new DataTable();
                DataTable DTForecast = new DataTable();

                string objWeather = JsonConvert.DeserializeObject<string>(strWeather);
                if (objWeather != "no data")
                {

                    DTWeatherFull = JsonConvert.DeserializeObject<DataTable>(objWeather);

                    for (int k = 0; k < DTWeatherFull.Rows.Count; k++)
                    {
                        DataRow ForecastRow = DTForecastFUll.NewRow();
                        DTForecastFUll.Rows.Add(ForecastRow);
                        int count = DTForecastFUll.Rows.Count;
                        string datetime = Convert.ToDateTime(DTWeatherFull.Rows[k]["DateTime"]).ToString("dd-MMM-yyyy");
                        DateTime newdatetime = new DateTime();
                        DateTime.TryParse(DTWeatherFull.Rows[k]["DateTime"].ToString(), out newdatetime);
                        string maxhumidity = DTWeatherFull.Rows[k]["HumMor"].ToString();
                        string minhumidity = DTWeatherFull.Rows[k]["HumEve"].ToString();
                        string maxTemp = DTWeatherFull.Rows[k]["MaxTemp"].ToString();
                        DTForecastFUll.Rows[count - 1]["DateTime"] = datetime;
                        DTForecastFUll.Rows[count - 1]["NewDateTime"] = newdatetime;
                        DTForecastFUll.Rows[count - 1]["HumMor"] = maxhumidity;
                        DTForecastFUll.Rows[count - 1]["HumEve"] = minhumidity;
                        DTForecastFUll.Rows[count - 1]["MaxTemp"] = maxTemp;

                    }

                    DTForecastFUll.DefaultView.Sort = "NewDateTime";
                    DTForecastFUll = DTForecastFUll.DefaultView.ToTable();





                    int g = 0;
                    int counter = 0;
                    int newcounter = 0;
                    int entryparam = 0;
                    for (int i = g; i < DTForecastFUll.Rows.Count; i++)
                    {
                        newcounter++;
                        double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                        double HumidityMor = DTForecastFUll.Rows[i]["HumMor"].ToString().doubleTP();
                        double HumidityEve = DTForecastFUll.Rows[i]["HumEve"].ToString().doubleTP();

                        if (MaxTemp < 33 || HumidityMor > 70 || HumidityEve < 40)
                        {

                            counter = 0;
                        }
                        else
                        {
                            counter++;
                            if (counter >= 4)
                            {
                                entryparam++;
                                int End = i;
                                int start = i - 3;


                                for (int j = start; j <= End; j++)
                                {
                                    DieaseDates objDD = new DieaseDates();
                                    DateTime Dates = new DateTime();
                                    string rh = DTForecastFUll.Rows[j]["RH"].ToString();
                                    string MTemp = DTForecastFUll.Rows[j]["MaxTemp"].ToString();
                                    string MinTemperature = DTForecastFUll.Rows[j]["MinTemp"].ToString();
                                    string HumidMor = DTForecastFUll.Rows[j]["HumMor"].ToString();
                                    string HumidEve = DTForecastFUll.Rows[j]["HumEve"].ToString();
                                    DateTime.TryParse(DTForecastFUll.Rows[j]["DateTime"].ToString(), out Dates);

                                    objDD.MaxTemperature = MTemp;
                                    objDD.MaxHumid = HumidMor;
                                    objDD.MinHumid = HumidEve;
                                    objDD.Date = Dates;
                                    if (lstDieaseDates.FindAll(a => a.Date.Date == Dates.Date).Count() == 0)
                                        lstDieaseDates.Add(objDD);
                                }

                            }

                        }

                        if (newcounter == 4)
                        {
                            g = g + 1;
                            i = g - 1;
                            newcounter = 0;
                            counter = 0;
                        }
                    }
                    string PreventiveMessage = "";
                    if (Client == "cottonadvisory18")
                        PreventiveMessage = "श्रीमान्, आपके क्षेत्र में पिंक बालवर्म के आक्रमण के अनुकूल मौसम की स्थिति बन रही है कृपया अपने खेत  का निरीक्षण करें और उचित कार्यवाही करें |";
                    else if (Client == "bayer" || Client == "ggrc")
                        PreventiveMessage = "Dear Sir, due to weather conditions there may be attack of PINK BALL WORM. Please check your farm and take necessary action.";
                    if (entryparam == 1)
                    {
                        DieaseDates objDD = new DieaseDates();
                        objDD.SMSPinkBollWorm = PreventiveMessage;

                        lstDieaseDates.Add(objDD);
                        //objResult.Add(PreventiveMessage);
                        //objResult.Add(CurativeMessage);
                    }
                }
                return lstDieaseDates;
            }
            catch (Exception ex)
            {
                return lstDieaseDates;
            }
        }







        public List<DieaseDates> getWhiteFlyDiseaseSMS(double Latitude, double Longitude, string Village, string Client, DataTable DT)
        {
            //List<string> objResult = new List<string>();
            List<DieaseDates> lstDieaseDates = new List<DieaseDates>();
            DataTable DTForecastFUll = new DataTable();
            DTForecastFUll.Merge(DT);
            DTForecastFUll.Columns.Add("RH");
            for (int f = 0; f < DTForecastFUll.Rows.Count; f++)
            {
                string maxhumidity = DTForecastFUll.Rows[f]["HumMor"].ToString();
                string minhumidity = DTForecastFUll.Rows[f]["HumEve"].ToString();
                double rh = (maxhumidity.doubleTP() + minhumidity.doubleTP()) / 2;
                DTForecastFUll.Rows[f]["RH"] = rh.ToString();
            }

            try
            {
                WebClient wc = new WebClient();


                string apiAddrWeather = "http://54.174.231.79:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no"; ;

                //  string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(4).ToString("yyyy-MM-dd") + "/District/json/internal";

                string strWeather = wc.DownloadString(apiAddrWeather);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");

                DataTable DTWeatherFull = new DataTable();
                DataTable DTForecast = new DataTable();

                string objWeather = JsonConvert.DeserializeObject<string>(strWeather);
                if (objWeather != "no data")
                {

                    DTWeatherFull = JsonConvert.DeserializeObject<DataTable>(objWeather);
                    for (int k = 0; k < DTWeatherFull.Rows.Count; k++)
                    {
                        DataRow ForecastRow = DTForecastFUll.NewRow();
                        DTForecastFUll.Rows.Add(ForecastRow);
                        int count = DTForecastFUll.Rows.Count;
                        string datetime = Convert.ToDateTime(DTWeatherFull.Rows[k]["DateTime"]).ToString("dd-MMM-yyyy");
                        DateTime newdatetime = new DateTime();
                        DateTime.TryParse(DTWeatherFull.Rows[k]["DateTime"].ToString(), out newdatetime);
                        string maxhumidity = DTWeatherFull.Rows[k]["HumMor"].ToString();
                        string minhumidity = DTWeatherFull.Rows[k]["HumEve"].ToString();
                        string maxTemp = DTWeatherFull.Rows[k]["MaxTemp"].ToString();
                        string minTemp = DTWeatherFull.Rows[k]["MinTemp"].ToString();
                        DTForecastFUll.Rows[count - 1]["DateTime"] = datetime;
                        DTForecastFUll.Rows[count - 1]["NewDateTime"] = newdatetime;
                        DTForecastFUll.Rows[count - 1]["HumMor"] = maxhumidity;
                        DTForecastFUll.Rows[count - 1]["HumEve"] = minhumidity;
                        DTForecastFUll.Rows[count - 1]["MaxTemp"] = maxTemp;
                        DTForecastFUll.Rows[count - 1]["MinTemp"] = minTemp;
                        DTForecastFUll.Rows[count - 1]["RH"] = ((maxhumidity.doubleTP() + minhumidity.doubleTP()) / 2).ToString();

                    }

                    DTForecastFUll.DefaultView.Sort = "NewDateTime";
                    DTForecastFUll = DTForecastFUll.DefaultView.ToTable();

                    int g = 0;
                    int counter = 0;
                    int newcounter = 0;
                    int entryparam = 0;
                    for (int i = g; i < DTForecastFUll.Rows.Count; i++)
                    {
                        newcounter = newcounter + 1;
                        double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                        double HumidityMor = DTForecastFUll.Rows[i]["HumMor"].ToString().doubleTP();
                        double HumidityEve = DTForecastFUll.Rows[i]["HumEve"].ToString().doubleTP();
                        double MinTemp = DTForecastFUll.Rows[i]["MinTemp"].ToString().doubleTP();
                        double Rh = DTForecastFUll.Rows[i]["RH"].ToString().doubleTP();

                        if (MaxTemp > 36 || MinTemp < 26 || Rh < 75)
                        {
                            counter = 0;

                        }
                        else
                        {
                            counter++;
                            if (counter >= 4)
                            {

                                int End = i;
                                int start = i - 3;

                                entryparam++;
                                for (int j = start; j <= End; j++)
                                {
                                    DieaseDates objDD = new DieaseDates();
                                    DateTime Dates = new DateTime();
                                    string rh = DTForecastFUll.Rows[j]["RH"].ToString();
                                    string MTemp = DTForecastFUll.Rows[j]["MaxTemp"].ToString();
                                    string MinTemperature = DTForecastFUll.Rows[j]["MinTemp"].ToString();
                                    string HumidMor = DTForecastFUll.Rows[j]["HumMor"].ToString();
                                    string HumidEve = DTForecastFUll.Rows[j]["HumEve"].ToString();
                                    DateTime.TryParse(DTForecastFUll.Rows[j]["DateTime"].ToString(), out Dates);

                                    objDD.MaxTemperature = MTemp;
                                    objDD.Date = Dates;
                                    objDD.MinTemperature = MinTemperature;
                                    objDD.Rh = rh;
                                    if (lstDieaseDates.FindAll(a => a.Date.Date == Dates.Date).Count() == 0)
                                        lstDieaseDates.Add(objDD);
                                }


                            }

                        }
                        if (newcounter == 4)
                        {
                            g = g + 1;
                            i = g - 1;
                            newcounter = 0;
                            counter = 0;
                        }
                    }
                    string PreventiveMessage = "";
                    if (Client == "cottonadvisory18")
                        PreventiveMessage = "श्रीमान्, आपके क्षेत्र में व्हाटफ्लाई अनुकूल मौसम की स्थिति बन रही है कृपया अपने खेत  का निरीक्षण करें यदि कीट की उपस्थिति  देखे तो कीटनाशकों का छिड़काव करे या हमारे विशेषज्ञ से संपर्क करें।";
                    else if (Client == "bayer" || Client == "ggrc")
                        PreventiveMessage = "Dear Sir, whityfly conducive weather conditions are forming in your filed, please inspect your field if pest load is observed then spray insecticide or contact our expert.";
                    if (entryparam == 1)
                    {
                        DieaseDates objDD = new DieaseDates();
                        objDD.SMSWhiteFly = PreventiveMessage;
                        lstDieaseDates.Add(objDD);
                    }
                }
                return lstDieaseDates;
            }
            catch (Exception ex)
            {
                return lstDieaseDates;
            }
        }




        public List<DieaseDates> getBlightDiseaseSMS(double Latitude, double Longitude, string Village, string Client, DataTable DT)
        {

            List<DieaseDates> lstDieaseDates = new List<DieaseDates>();
            DataTable DTForecastFUll = new DataTable();
            DTForecastFUll.Merge(DT);
            DTForecastFUll.Columns.Add("RH");
            DTForecastFUll.Columns.Add("AvgTemp");
            for (int j = 0; j < DTForecastFUll.Rows.Count; j++)
            {
                string maxhumidity = DTForecastFUll.Rows[j]["HumMor"].ToString();
                string minhumidity = DTForecastFUll.Rows[j]["HumEve"].ToString();
                string maxTemp = DTForecastFUll.Rows[j]["MaxTemp"].ToString();
                string minTemp = DTForecastFUll.Rows[j]["MinTemp"].ToString();
                DTForecastFUll.Rows[j]["RH"] = ((maxhumidity.doubleTP() + minhumidity.doubleTP()) / 2).ToString();
                DTForecastFUll.Rows[j]["AvgTemp"] = ((maxTemp.doubleTP() + minTemp.doubleTP()) / 2).ToString();
            }

            try
            {
                WebClient wc = new WebClient();
                string apiAddrWeather = "http://54.174.231.79:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no";

                string strWeather = wc.DownloadString(apiAddrWeather);

                DataTable DTWeatherFull = new DataTable();

                string objWeather = JsonConvert.DeserializeObject<string>(strWeather);
                if (objWeather != "no data")
                {

                    DTWeatherFull = JsonConvert.DeserializeObject<DataTable>(objWeather);
                    for (int k = 0; k < DTWeatherFull.Rows.Count; k++)
                    {
                        DataRow ForecastRow = DTForecastFUll.NewRow();
                        DateTime newdatetime = new DateTime();
                        DateTime.TryParse(DTWeatherFull.Rows[k]["DateTime"].ToString(), out newdatetime);
                        string maxhumidity = DTWeatherFull.Rows[k]["HumMor"].ToString();
                        string minhumidity = DTWeatherFull.Rows[k]["HumEve"].ToString();
                        string maxTemp = DTWeatherFull.Rows[k]["MaxTemp"].ToString();
                        string minTemp = DTWeatherFull.Rows[k]["MinTemp"].ToString();
                        string Rain = DTWeatherFull.Rows[k]["Rain"].ToString();
                        ForecastRow["DateTime"] = DTWeatherFull.Rows[k]["DateTime"];
                        ForecastRow["NewDateTime"] = newdatetime;
                        ForecastRow["HumMor"] = maxhumidity;
                        ForecastRow["HumEve"] = minhumidity;
                        ForecastRow["MaxTemp"] = maxTemp;
                        ForecastRow["MinTemp"] = minTemp;
                        ForecastRow["AvgTemp"] = ((maxTemp.doubleTP() + minTemp.doubleTP()) / 2).ToString();
                        ForecastRow["RH"] = ((maxhumidity.doubleTP() + minhumidity.doubleTP()) / 2).ToString();
                        ForecastRow["Rain"] = Rain;
                        DTForecastFUll.Rows.Add(ForecastRow);

                    }

                    DTForecastFUll.DefaultView.Sort = "NewDateTime";
                    DTForecastFUll = DTForecastFUll.DefaultView.ToTable();

                    int g = 0;
                    int counter = 0;
                    int newcounter = 0;
                    int entryparam = 0;
                    for (int i = g; i < DTForecastFUll.Rows.Count; i++)
                    {
                        newcounter = newcounter + 1;
                        double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                        double HumidityMor = DTForecastFUll.Rows[i]["HumMor"].ToString().doubleTP();
                        double HumidityEve = DTForecastFUll.Rows[i]["HumEve"].ToString().doubleTP();
                        double Rain = DTForecastFUll.Rows[i]["Rain"].ToString().doubleTP();
                        double AvgTemp = DTForecastFUll.Rows[i]["AvgTemp"].ToString().doubleTP();

                        if (AvgTemp > 35 || AvgTemp < 25 || Rain < 0.6)
                        {
                            counter = 0;

                        }
                        else
                        {
                            counter++;
                            if (counter >= 4)
                            {

                                int End = i;
                                int start = i - 3;

                                entryparam++;
                                for (int j = start; j <= End; j++)
                                {
                                    DieaseDates objDD = new DieaseDates();
                                    DateTime Dates = new DateTime();
                                    string rh = DTForecastFUll.Rows[j]["RH"].ToString();
                                    string MTemp = DTForecastFUll.Rows[j]["MaxTemp"].ToString();
                                    string MinTemperature = DTForecastFUll.Rows[j]["MinTemp"].ToString();
                                    string HumidMor = DTForecastFUll.Rows[j]["HumMor"].ToString();
                                    string HumidEve = DTForecastFUll.Rows[j]["HumEve"].ToString();
                                    DateTime.TryParse(DTForecastFUll.Rows[j]["DateTime"].ToString(), out Dates);
                                    string avgtemp = DTForecastFUll.Rows[j]["AvgTemp"].ToString();
                                    string rain = DTForecastFUll.Rows[j]["Rain"].ToString();
                                    objDD.MaxTemperature = MTemp;
                                    objDD.MinTemperature = MinTemperature;
                                    objDD.MaxHumid = HumidMor;
                                    objDD.MinHumid = HumidEve;
                                    objDD.Rh = rh;
                                    objDD.Rain = rain;
                                    objDD.AverageTemp = avgtemp;
                                    objDD.Date = Dates;

                                    if (lstDieaseDates.FindAll(a => a.Date.Date == Dates.Date).Count() == 0)
                                        lstDieaseDates.Add(objDD);
                                }


                            }

                        }
                        if (newcounter == 4)
                        {
                            g = g + 1;
                            i = g - 1;
                            newcounter = 0;
                            counter = 0;
                        }
                    }
                    string PreventiveMessage = "";
                    if (Client.ToLower() == "giz")
                        PreventiveMessage = "ಪ್ರಿಯ ರೈತ ಮಿತ್ರರೇ,ಹೇನಿನಂತಹ ಕೀಟಗಳ ಬೆಳವಣಿಗೆಗೆ ಅನುಕೂಲಕರವಾದ ವಾತಾವರಣವು ನಿರ್ಮಾಣವಾಗುತ್ತಿದ್ದರೆ,ದಯವಿಟ್ಟು ನಿಮ್ಮ ಜಮೀನನ್ನು ಕೀಟದ ಹೊರೆಯ ಸಂಭಂದಿತವಾಗಿ ಗಮನಿಸಿ ಮತ್ತು ಕೀಟನಾಶಕವನ್ನು ಸಿಂಪಡಿಸಿ ಮತ್ತು ನಮ್ಮ ಕೃಷಿ ತಜ್ಞರನ್ನು ಸಂಪರ್ಕಿಸಿ.";
                    if (entryparam == 1)
                    {
                        DieaseDates objDD = new DieaseDates();
                        objDD.SMSBlight = PreventiveMessage;

                        lstDieaseDates.Add(objDD);
                    }
                }
                return lstDieaseDates;
            }
            catch (Exception ex)
            {
                return lstDieaseDates;
            }
        }


        public DataTable getforecastdata(double Latitude, double Longitude, string Village, string Client)
        {
            DataTable DT = new DataTable();
            try
            {
                WebClient wc = new WebClient();
                string apiAddrForecast = "http://54.174.231.79:82/wdrest.svc/WZDailyForecast_v2/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/English/json/mfi";

                string strForecast = wc.DownloadString(apiAddrForecast);
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                if (objFOrecast != "no data")
                {
                    DT = JsonConvert.DeserializeObject<DataTable>(objFOrecast);
                    DT.Columns.Add("NewDateTime", typeof(DateTime));
                    for (int h = 0; h < DT.Rows.Count; h++)
                    {
                        DateTime datetime = new DateTime();
                        DateTime.TryParse(DT.Rows[h]["DateTime"].ToString(), out datetime);
                        DT.Rows[h]["NewDateTime"] = datetime;
                    }

                }
            }
            catch (Exception ex)
            {
                return DT;
            }
            return DT;
        }








        public string getGGRCWeatherDataSMS(double Latitude, double Longitude, string Village, string Client, string prefweatherlanguage)
        {
            string result = "";
            try
            {
                WebClient wc = new WebClient();
                // string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/EstActualData/" + Latitude + "/" + Longitude + "/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.ToString("yyyy-MM-dd") + "/wrlinternaldata";
                // string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/EstActualData/" + Latitude + "/" + Longitude + "/" + DateTime.Now.AddDays(-7).ToString("yyyy-MM-dd") + "/" + DateTime.Now.ToString("yyyy-MM-dd") + "/wrlinternaldata";
                string apiAddr = "http://54.174.231.79:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no";

                string strForecast = wc.DownloadString(apiAddr);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");
                DataTable DTForecastFUll = new DataTable();
                DataTable DTForecast = new DataTable();
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                if (objFOrecast != "no data")
                {
                    DTForecastFUll = JsonConvert.DeserializeObject<DataTable>(objFOrecast);
                    double Temp_Max = 0;
                    double Temp_Min = 1000;
                    double Humidity_Max = 0;
                    double Humidity_Min = 1000;
                    double TotRain = 0;
                    for (int i = 0; i < DTForecastFUll.Rows.Count; i++)
                    {
                        if (DTForecastFUll.Rows[i]["MaxTemp"].ToString() != "")
                        {
                            double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                            if (MaxTemp > Temp_Max)
                                Temp_Max = MaxTemp;
                            if (MaxTemp < Temp_Min)
                                Temp_Min = MaxTemp;

                        }

                        if (DTForecastFUll.Rows[i]["Humidity"].ToString() != "")
                        {
                            double Humidity = DTForecastFUll.Rows[i]["Humidity"].ToString().doubleTP();
                            if (Humidity > Humidity_Max)
                                Humidity_Max = Humidity;
                            if (Humidity < Humidity_Min)
                                Humidity_Min = Humidity;

                        }

                        TotRain = TotRain + DTForecastFUll.Rows[i]["Rain"].ToString().doubleTP();

                    }
                    string MyMessage = "";
                    //if (Client == "ggrc")
                    //    MyMessage = "?????? (????????)- ?????? ? ??????? ?????? " + Temp_Min + " - " +
                    //              Temp_Max + " ??., ?????? (????? ??? ) " + Humidity_Min + " - " + Humidity_Max +
                    //              "%, ?????- " + TotRain + " ??.??.";



                    //if (Client == "test")
                    //    MyMessage = "?????? (????????)- ?????? ? ??????? ?????? " + Temp_Min + " - " +
                    //              Temp_Max + " ??., ?????? (????? ??? ) " + Humidity_Min + " - " + Humidity_Max +
                    //              "%, ?????- " + TotRain + " ??.??.";

                    if (prefweatherlanguage.ToLower() == "hindi" && Client == "cottonadvisory18")
                        MyMessage = Village + " में पिछले 3 दिनों का मौसम: तापमान " + Temp_Min + " - " +
                                Temp_Max + " C, वर्षा " + TotRain + " मिमी, आद्रता " + Humidity_Min + " - " + Humidity_Max;

                    else if (prefweatherlanguage.ToLower() == "gujrati")
                        MyMessage = " છેલ્લા ૩ દિવસ નું હવામાન- તાપમાન " + Temp_Min + " - " + Temp_Max + " ડીગ્રી સે. ," +
                            "વરસાદ - " + TotRain + "  મીમી,  હવા નો  ભેજ " + Humidity_Min + " - " + Humidity_Max + " %";

                    else if (prefweatherlanguage.ToLower() == "english")
                        MyMessage = Village + " Weather[G]- Last 3 days Temp " + Temp_Min + " - " +
                                 Temp_Max + " C, Rain - " + TotRain + " mm Humidity " + Humidity_Min + " - " + Humidity_Max;

                    else if (prefweatherlanguage.ToLower() == "bengali")
                        MyMessage = "" + Village + "  আবহাওয়া [G]- বিগত 3 দিনের তাপমাত্রা " + Temp_Min + " - " + Temp_Max + " C, আর্দ্রতা " + Humidity_Min + "% - " + Humidity_Max + "%, বৃষ্টিপাত - " + TotRain + " MM।";




                    result = MyMessage;

                }
                return result;
            }
            catch (Exception ex)
            {
                return result;
            }
        }




        public double GetCropRainData(double Latitude, double Longitude, string Village, string Client)
        {
            double result = 0;
            try
            {
                WebClient wc = new WebClient();
                string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/EstActualData/" + Latitude + "/" + Longitude + "/" + DateTime.Now.AddDays(-7).ToString("yyyy-MM-dd") + "/" + DateTime.Now.ToString("yyyy-MM-dd") + "/wrlinternaldata";
                // string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/EstActualData/" + Latitude + "/" + Longitude + "/" + DateTime.Now.AddDays(-7).ToString("yyyy-MM-dd") + "/" + DateTime.Now.ToString("yyyy-MM-dd") + "/wrlinternaldata";
                string strForecast = wc.DownloadString(apiAddr);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");
                DataTable DTForecastFUll = new DataTable();
                DataTable DTForecast = new DataTable();
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                if (objFOrecast != "no data")
                {
                    DTForecastFUll = JsonConvert.DeserializeObject<DataTable>(objFOrecast);
                    double Temp_Max = 0;
                    double Temp_Min = 1000;
                    double Humidity_Max = 0;
                    double Humidity_Min = 1000;
                    double TotRain = 0;
                    for (int i = 0; i < DTForecastFUll.Rows.Count; i++)
                    {
                        if (DTForecastFUll.Rows[i]["MaxTemp"].ToString() != "")
                        {
                            double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                            if (MaxTemp > Temp_Max)
                                Temp_Max = MaxTemp;
                            if (MaxTemp < Temp_Min)
                                Temp_Min = MaxTemp;

                        }

                        if (DTForecastFUll.Rows[i]["Humidity"].ToString() != "")
                        {
                            double Humidity = DTForecastFUll.Rows[i]["Humidity"].ToString().doubleTP();
                            if (Humidity > Humidity_Max)
                                Humidity_Max = Humidity;
                            if (Humidity < Humidity_Min)
                                Humidity_Min = Humidity;

                        }

                        TotRain = TotRain + DTForecastFUll.Rows[i]["Rain"].ToString().doubleTP();

                    }


                    if (Client == "wrmsdabwali")
                        result = TotRain;


                }
                return result;
            }
            catch (Exception ex)
            {
                return result;
            }
        }


        public double getGGRCCropForecastSMS(double Latitude, double Longitude, string Village, string Client, int days)
        {
            double result = 0;
            try
            {
                WebClient wc = new WebClient();
                string apiAddr = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(days).ToString("yyyy-MM-dd") + "/District/json/internal";
                string strForecast = wc.DownloadString(apiAddr);
                // strForecast = strForecast.Replace("\\\\\\\"","\"");
                DataTable DTForecastFUll = new DataTable();
                DataTable DTForecast = new DataTable();
                string objFOrecast = JsonConvert.DeserializeObject<string>(strForecast);
                if (objFOrecast != "no data")
                {
                    DTForecastFUll = JsonConvert.DeserializeObject<DataTable>(objFOrecast);
                    double MaxTemp_Max = 0;
                    double MaxTemp_Min = 1000;
                    double MinTemp_Max = 0;
                    double MinTemp_Min = 1000;
                    double Humidity_Max = 0;
                    double Humidity_Min = 1000;
                    double TotRain = 0;
                    for (int i = 0; i < DTForecastFUll.Rows.Count; i++)
                    {
                        if (DTForecastFUll.Rows[i]["MaxTemp"].ToString() != "")
                        {
                            double MaxTemp = DTForecastFUll.Rows[i]["MaxTemp"].ToString().doubleTP();
                            if (MaxTemp > MaxTemp_Max)
                                MaxTemp_Max = MaxTemp;
                            if (MaxTemp < MaxTemp_Min)
                                MaxTemp_Min = MaxTemp;

                        }

                        if (DTForecastFUll.Rows[i]["MinTemp"].ToString() != "")
                        {
                            double MinTemp = DTForecastFUll.Rows[i]["MinTemp"].ToString().doubleTP();
                            if (MinTemp > MinTemp_Max)
                                MinTemp_Max = MinTemp;
                            if (MinTemp < MinTemp_Min)
                                MinTemp_Min = MinTemp;

                        }

                        if (DTForecastFUll.Rows[i]["Humidity"].ToString() != "")
                        {
                            double Humidity = DTForecastFUll.Rows[i]["Humidity"].ToString().doubleTP();
                            if (Humidity > Humidity_Max)
                                Humidity_Max = Humidity;
                            if (Humidity < Humidity_Min)
                                Humidity_Min = Humidity;

                        }

                        TotRain = TotRain + DTForecastFUll.Rows[i]["Rain"].ToString().doubleTP();
                        if (Client == "wrmsdabwali")
                            result = TotRain;
                    }


                }
                return result;
            }
            catch (Exception ex)
            {
                return result;
            }
        }

    }





    public class ChartDataCl
    {
        public string Label { get; set; }
        public double XValue { get; set; }
        public DateTime XValueDT { get; set; }
        public double Value { get; set; }
        public ChartDataCl()
        {
            Label = "";
            XValue = 0;
            Value = 0;
            XValueDT = new DateTime();
        }
    }
    public class DistricCoordinator
    {
        public string Block { get; set; }
        public string PhoneNo { get; set; }

    }

    public class WrmsAdmin
    {
        public string Msg { get; set; }
        public string PhoneNo { get; set; }
        public DateTime ddat { get; set; }
    }
    public class ChartDataSeries
    {
        public List<ChartDataCl> lstChartDataCl { get; set; }
        public string SeriesName { get; set; }
        public double Rank { get; set; }

        public ChartDataSeries()
        {
            lstChartDataCl = new List<ChartDataCl>();
            SeriesName = "";
            Rank = 0;
        }
        public ChartDataSeries(List<ChartDataCl> lstChartDataCl, string SeriesName, double Rank)
        {
            this.lstChartDataCl = lstChartDataCl;
            this.SeriesName = SeriesName;
            this.Rank = Rank;
        }
        public ChartDataSeries(List<ChartDataCl> lstChartDataCl, string SeriesName)
        {
            this.lstChartDataCl = lstChartDataCl;
            this.SeriesName = SeriesName;
        }
    }



    public class StringString
    {
        public string Str1 { get; set; }
        public string Str2 { get; set; }

        public StringString()
        {
            Str1 = "";
            Str2 = "";
        }

        public StringString(string Str1, string Str2)
        {
            this.Str1 = Str1;
            this.Str2 = Str2;
        }
    }

    public class StringStringStringString
    {
        public string Str1 { get; set; }
        public string Str2 { get; set; }
        public string Str3 { get; set; }
        public string Str4 { get; set; }

        public StringStringStringString()
        {
            Str1 = "";
            Str2 = "";
            Str3 = "";
            Str4 = "";
        }

        public StringStringStringString(string Str1, string Str2, string Str3)
        {
            this.Str1 = Str1;
            this.Str2 = Str2;
            this.Str3 = Str3;
            this.Str4 = Str4;
        }


    }





    public class StringStringString
    {
        public string Str1 { get; set; }
        public string Str2 { get; set; }
        public string Str3 { get; set; }

        public StringStringString()
        {
            Str1 = "";
            Str2 = "";
            Str3 = "";
        }

        public StringStringString(string Str1, string Str2, string Str3)
        {
            this.Str1 = Str1;
            this.Str2 = Str2;
            this.Str3 = Str3;
        }


    }
    public class DataTableAndColDesc
    {
        public DataTable DT { get; set; }
        public DataTable DT4 { get; set; }
        public DataTable DT5 { get; set; }
        public DataTable DT6 { get; set; }
        public DataTable DT7 { get; set; }
        public DataTable DT8 { get; set; }
        public List<string> lstColNames { get; set; }
        public List<StringString> lstFilter { get; set; }
        public DataTable DTLegends { get; set; }
        public string ChartData { get; set; }
        public string SoilInfo { get; set; }
        public string ForecastInfo { get; set; }
        public DataTable DTChartDesc { get; set; }
        public List<string> lstFilter2 { get; set; }
        public List<StringStringString> lstSSS { get; set; }
        public List<ChartDataSeries> lstChartDataSeries { get; set; }
        public List<ChartDataSeries> lstChartDataSeries2 { get; set; }
        public List<ChartDataSeries> lstChartDataSeries3 { get; set; }
        public List<ChartDataSeries> lstChartDataSeries4 { get; set; }

        public int ID { get; set; }
        public DataTableAndColDesc()
        {
            ID = 0;
            DT = new DataTable();
            lstColNames = new List<string>();
            lstFilter = new List<StringString>();
            DTLegends = new DataTable();
            DTChartDesc = new DataTable();
            ChartData = "";
            lstFilter2 = new List<string>();
            lstChartDataSeries = new List<ChartDataSeries>();
            DT4 = new DataTable();
            DT5 = new DataTable();
            DT6 = new DataTable();
            DT7 = new DataTable();
            DT8 = new DataTable();
        }
    }



    public class DataTableAndColDescForecastData
    {
        public DataTable DTForecast { get; set; }
        public string RID { get; set; }
        public DataTableAndColDescForecastData()
        {
            RID = "";
            DTForecast = new DataTable();
        }
    }
    public class DieaseDates
    {
        public string MinTemperature { get; set; }
        public string MaxTemperature { get; set; }
        public string MaxHumid { get; set; }
        public string MinHumid { get; set; }
        public DateTime Date { get; set; }
        public string DiseaseName { get; set; }
        public string Rh { get; set; }
        public string Rain { get; set; }
        public string AverageTemp { get; set; }
        public string SMSPinkBollWorm { get; set; }
        public string SMSWhiteFly { get; set; }
        public string SMSBlight { get; set; }
    }


    public class ForecastData
    {
        public string DateTime { get; set; }
        public double MaxTemp { get; set; }
        public double MinTemp { get; set; }
        public double MaxHumidity { get; set; }
        public double MinHumidity { get; set; }
        public DateTime NewDateTime { get; set; }
        public string DateDate { get; set; }
        public double RH { get; set; }
    }
    public class forecastlan
    {
        public string stateID { get; set; }
        public string forcastlan { get; set; }
    }

    public class Agronimist
    {
        public string Id { get; set; }
        public string PhoneNo { get; set; }
        public List<string> lstIds { get; set; }
    }

    public class DiseaseNames
    {
        public string ID { get; set; }
        public string DiseaseName { get; set; }
    }

    public static class ExtensionMethods
    {

        public static double doubleTP(this string OrigVal)
        {

            double dblVal = 0;
            double.TryParse(OrigVal, out dblVal);
            return dblVal;
        }
        public static int intTP(this string OrigVal)
        {
            int dblVal = 0;
            int.TryParse(OrigVal, out dblVal);
            return dblVal;
        }
        public static long longTP(this string OrigVal)
        {
            long dblVal = 0;
            long.TryParse(OrigVal, out dblVal);
            return dblVal;
        }
        public static DateTime dtTP(this string OrigVal)
        {
            DateTime dblVal = new DateTime();
            DateTime.TryParse(OrigVal, out dblVal);
            return dblVal;
        }
        public static DataTable ListToDataTable<T>(this IList<T> data, string tableName)
        {
            DataTable table = new DataTable(tableName);

            //special handling for value types and string
            if (typeof(T).IsValueType || typeof(T).Equals(typeof(string)))
            {

                DataColumn dc = new DataColumn("Value");
                table.Columns.Add(dc);
                foreach (T item in data)
                {
                    DataRow dr = table.NewRow();
                    dr[0] = item;
                    table.Rows.Add(dr);
                }
            }
            else
            {
                PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));
                foreach (PropertyDescriptor prop in properties)
                {
                    table.Columns.Add(prop.Name,
                    Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
                }
                foreach (T item in data)
                {
                    DataRow row = table.NewRow();
                    foreach (PropertyDescriptor prop in properties)
                    {
                        try
                        {
                            row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                        }
                        catch (Exception ex)
                        {
                            row[prop.Name] = DBNull.Value;
                        }
                    }
                    table.Rows.Add(row);
                }
            }
            return table;
        }
        public static T DeepClone<T>(T obj)
        {
            using (var ms = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(ms, obj);
                ms.Position = 0;

                return (T)formatter.Deserialize(ms);
            }
        }
        public static DataTable ToDataTable<T>(IList<T> data)
        {
            PropertyDescriptorCollection props =
                TypeDescriptor.GetProperties(typeof(T));
            DataTable table = new DataTable();
            for (int i = 0; i < props.Count; i++)
            {
                PropertyDescriptor prop = props[i];
                table.Columns.Add(prop.Name, prop.PropertyType);
            }
            object[] values = new object[props.Count];
            foreach (T item in data)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = props[i].GetValue(item);
                }
                table.Rows.Add(values);
            }
            return table;
        }
    }

}
