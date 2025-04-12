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
        static ArrayList arrActivityLog = new ArrayList();
        static int AppID = 22;
        public static System.Threading.Mutex mutex = new Mutex(true, "FarmSMScheduler2.exe");
        double totalwater = 25;
        public static bool flgTest = false;
        public static string Mode = "Other";
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
                if(Mode == "Jalna")
                {
                    AppID = 89;
                }
				ConfigManager.Initialize(); // uses ENVIRONMENT and SHARED_CONFIG_PATH

				Console.WriteLine("Environment: " + ConfigManager.AppEnvironment);
				Console.WriteLine("MySQL: " + ConfigManager.MySqlUrl);
				//  arrActivityLog.Add("Started at " + DateTime.Now);
				Console.WriteLine("POP Sender");
                Console.WriteLine("Start at ... " + DateTime.Now);
                arrActivityLog.Add("Started at " + DateTime.Now);
                if(flgTest)
                    Console.WriteLine("RUNNING IN TEST MODE");
                objProg.NewPOPMessageSender();
               
            }
            else
            {
                Console.WriteLine("Program is already running");
                Thread.Sleep(TimeSpan.FromSeconds(5));
            }
            Thread.Sleep(TimeSpan.FromSeconds(5));
        }
      

        void SMSScheduler_FarmerWise()
        {
            DataTable DTPOPMapping = getData("select * from mfi.pop_scheduler_stagemapping");
            DataTable DTPOPStages = new DataTable();
            DataTable DTPOPWork = new DataTable();
            DTPOPStages = getData("select * from mfi.pop_stages where CropID = 12");
            DTPOPWork = getData("select * from mfi.pop_work where StageID in (select ID from mfi.pop_stages where CropID = 12)");

            DataTable DTAllFarmers = new DataTable(); // getDabwaliFarms("100894", "");
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
            conn = new MySqlConnection(ConfigManager.MySqlUrl);
            conn.Open();

            string LogDate = ConDateTime(DateTime.Now);
            string sql = "";
            string sqlhead = "insert into appmanager.cm_log (AppID, Priority, Status, LogDate) values ";
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
            string Schema = "mfi";
            if (Mode == "Jalna")
                Schema = "appmanager";
            string sql = "update " + Schema + ".cm_apps set LastRanAt = '" + ConDateTime(DateTime.Now) + "' where ID = " + AppID;
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



        private int GetPopFlag(string UserId)
        {
            DataTable Data = getData("select NewPopFlag From mfi.clientcode where ClientID = " + UserId.intTP() + "");

            if (Data.Rows.Count > 0)
            {
                return Data.Rows[0]["NewPopFlag"].ToString().intTP();
            }

            return 0;
        }

        public DataTable getSMSMaster(string Client,string Defaultclient,string ClientID="" )
        {
            DataTable data = new DataTable();

            //Get CropID and POPId 
           

            return data;
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
                // string newapi = "http://3.88.31.90:82/wdrest.svc/WZDailyForecast_v2/2018-06-26/2018-06-27/25.4670/91.3662/English/json/wrinternal";
                
                if (prefforcastlanguage.ToLower() == "hindi")
                    apiAddr = "http://3.88.31.90:82/wdrest.svc/MergeWZDailyForecast_v3/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(3).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/Hindi/json/wrinternal";
                else
                    apiAddr = "http://3.88.31.90:82/wdrest.svc/MergeWZDailyForecast_v3/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(3).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/English/json/wrinternal";

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
                string apiAddr = "https://mfi.secu.farm/yfirest.svc/Soil/Info/" + Latitude + "/" + Longitude;
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

            DataTable DTSMSTypes = getSMSMaster("jalna","");


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
     
      
        public string popclientmastermap(string cid)
        {
            string result = "";
            DataTable DT =  getData("select * from mfi.pop_DistProject_Map where ClientID='" + cid + "' ");

            DataTable DTcrop = getData("select ClientCropId from mfi.clientcode where ClientID = '"+ cid + "'");
            string cropid = "";
            if(DTcrop.Rows.Count>0)
            {
                cropid = DTcrop.Rows[0]["ClientCropId"].ToString();

                
            }



            if (DT.Rows.Count > 0)
                result = "no";
            else if(cropid!="" && DT.Rows.Count==0)
            {
                //check Default entry
                DataTable defaultDT = new DataTable();
                defaultDT = getData("select * from mfi.pop_master where ClientID='0' and CropID='"+cropid+"'");
                if(defaultDT.Rows.Count>0)
                    result = "yes";
            }
                

           

            return result;
        }

        void NewPOPMessageSender()
        {
            DataTable DTLanguages = getData("select * from mfi.language_master");
            execQuery("delete from mfi.farm_sms_status_master where LogDate < '" + DateTime.Now.AddDays(-14).ToString("yyyy-MM-dd") + "'");
            // execQuery("delete from mfi.farm_sms_lstsend where date(LogDate) < '" + DateTime.Now.ToString("yyyy-MM-dd") + "' and (FarmID,MessageType) not in (SELECT FarmID,MessageType FROM mfi.farm_sms_status_master where MsgStatus='cancel' and date(DATE_ADD(ScheduleDate, INTERVAL 15 DAY))>date(Now())");
            execQuery("delete from mfi.farm_sms_lstsend where date(LogDate) < '" + DateTime.Now.ToString("yyyy-MM-dd") + "' and (FarmID,MessageType) " +
                " not in (SELECT FarmID,MessageType FROM mfi.farm_sms_status_master where MsgStatus='cancel')");
            execQuery("delete  from  mfi.farm_sms_status_master  where Date(StateWeatherExpiry) < '" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "' and ID>0 ");


            DataTable forecastdatatabledata = new DataTable();
            forecastdatatabledata.Columns.Add("FarmID");
            forecastdatatabledata.Columns.Add("Village");
            forecastdatatabledata.Columns.Add("Phonenumber");

            List<StringString> listdisnameID = new List<StringString>();

            DataTable DTWorkMaster = getData("select * from pop.work_master");
            var lstWorkMaster = (from rw in DTWorkMaster.AsEnumerable()
                                 select new StringString()
                                 {
                                     Str1 = Convert.ToString(rw["WorkID"]),
                                     Str2 = Convert.ToString(rw["WorkName"])
                                 }).ToList();
            string clientmsgqry = "select  pm.PolicyName, um.UserID,VisibleName,um.VisibleName as ClientName," +
                "stng.ServiceFinishDate, stng.NotificationOnly,pmap.PopID, c.Cluster, pm.PolicyMasterID " +
                " from policy.policymaster pm left join policy.policymastersettings stng on pm.policymasterid=stng.policymasterid" +
                " left join policy.planprojectmapping ppm on pm.policymasterid=ppm.planid " +
                "left join mfi.usermaster um on um.userid=projectid left join mfi.clientcode c on um.UserID=c.ClientID " +
                "left join pop.pop_DistPlan_Map pmap on pmap.PlanID = pm.PolicyMasterID " +
                "where stng.ServiceFinishDate>now() and usertypeid=5 and role='admin' " +
                "and um.UserID is not null group by ppm.projectid, pm.policymasterid order by um.UserID desc ";
            DataTable clientmsg = getData(clientmsgqry);
            for (int l = 0; l < clientmsg.Rows.Count; l++)
            {
                string visiblename = clientmsg.Rows[l]["ClientName"].ToString();
                string PolicyName = clientmsg.Rows[l]["PolicyName"].ToString();
                int PolicyMasterID = clientmsg.Rows[l]["PolicyMasterID"].ToString().intTP();
                string userid = clientmsg.Rows[l]["UserID"].ToString();
                string sevicenddate= clientmsg.Rows[l]["ServiceFinishDate"].ToString();
                string NotificationOnly = clientmsg.Rows[l]["NotificationOnly"].ToString();
                string PopID = clientmsg.Rows[l]["PopID"].ToString();
                int Cluster = clientmsg.Rows[l]["Cluster"].ToString().intTP();
                string ID = userid;


                if (flgTest)
                {
                    if (PolicyMasterID != 1440)
                        continue;
                }
                if (Mode == "Jalna" && Cluster != 2)
                    continue;
                else if (Mode != "Jalna" && Cluster == 2)
                    continue;
                if (string.IsNullOrEmpty(PopID))
                    continue;

                DateTime clientservicedate = new DateTime();
                DateTime.TryParse(sevicenddate,out clientservicedate);

              
              
                string Client = visiblename;
                Console.WriteLine("Policy is==>" + PolicyName + "==>client is==>" + Client);

                //if (ID == "150442")
                //    continue;


                updAppLastRun();



                List<StringStringStringString> LstRefID = new List<StringStringStringString>();
                List<DataTableAndColDescForecastData> LstsDataForecast = new List<DataTableAndColDescForecastData>();
                DataTable DTDabwalfarms = new DataTable();
                DTDabwalfarms = getDabwaliFarms(ID, Client, PolicyMasterID);
                
                //Check Clent entry in popclientmastermap
                string Defaultclient = popclientmastermap(ID);
                string tablename = "mfi.cotton_cropmessage_farms";

                List<WrmsAdmin> lstwrmsadmin = new List<WrmsAdmin>();

                List<string> lstSupport = new List<string>(); /*{ "9879010580", "8141897979", "9033635723", "9033765801", "7600616585", "9956128514" };*/
                List<Agronimist> lstAgro = new List<Agronimist>();

                DataTable preflan = GetPrefredlanguage(ID);

              

                DataTable dtAdmin = new DataTable();



                {

                    string status = "Pending";

                    if (Cluster == 2 || NotificationOnly != "0")
                        status = "NotSent";


                    for (int i = 0; i < DTDabwalfarms.Rows.Count; i++)
                    {
                       int LanguageID = 0;

                        //if (i <= 1100)
                        //    continue;
                        string District = "";
                        string hindivillage = "";
                        string VillageID = "";
                        string stateID = "";
                        string Village = "";
                        string farmername = "";
                        string databaserainalert = "";
                        string FarmID = DTDabwalfarms.Rows[i]["FarmID"].ToString();
                        if (flgTest)
                        {
                            if (FarmID != "1334557")
                                continue;
                        }
                        DataTable DTLastChecked = getData("select * from pop.sendlog where FarmID = " + FarmID);

                       

                        if(!flgTest && DTLastChecked.Rows.Count > 0)
                        {
                            DateTime dtLastChecked = DTLastChecked.Rows[0]["LastCheckedDate"].ToString().dtTP();
                            if ((DateTime.Now - dtLastChecked).TotalDays < 1)
                            {
                                continue;
                            }
                        }
                        string RefID = DTDabwalfarms.Rows[i]["RefId"].ToString();
                        farmername = DTDabwalfarms.Rows[i]["FarmerName"].ToString();
                        District = DTDabwalfarms.Rows[i]["District"].ToString();
                        string mobileno = DTDabwalfarms.Rows[i]["PhoneNumber"].ToString();
                        string altermobileno =  DTDabwalfarms.Rows[i]["AlterPhoneNo"].ToString().Trim();
                        string DistrictSVM = DTDabwalfarms.Rows[i]["District"].ToString();
                        string SubDistrictSVM = DTDabwalfarms.Rows[i]["Sub_District"].ToString();
                        string VillageId = DTDabwalfarms.Rows[i]["VillageId"].ToString();
                        string WRMS_StateID = DTDabwalfarms.Rows[i]["WRMS_StateID"].ToString();
                        string cropid = DTDabwalfarms.Rows[i]["cropid"].ToString();
                        string prefPoplan = preflan.Rows[0]["Pop_Preferredlan"].ToString();
                        string farmerlanguage = DTDabwalfarms.Rows[i]["Language"].ToString();
                        int PolicyID = DTDabwalfarms.Rows[i]["PolicyID"].ToString().intTP();
                        if (farmerlanguage != "")
                            prefPoplan = farmerlanguage;

                        try
                        {
                            var DRLanguage = DTLanguages.Select("Language = '" + prefPoplan + "'");
                            if (DRLanguage != null && DRLanguage.Count() > 0)
                                LanguageID = DRLanguage[0]["ID"].ToString().intTP();
                        }
                        catch (Exception)
                        {

                        }

                        databaserainalert = DTDabwalfarms.Rows[i]["RainAlertStatus"].ToString();
                        //if (FarmID != "683923")
                        //    continue;
                        string sqlhead_sentmessages = "insert into pop.sentmessages (FarmID, WorkID, LastCheckedDate) values ";
                        DataTable DTPOPSMS = new DataTable();
                        DataTable DTProblemSolution = new DataTable();
                        DataTable DTCropCondition = new DataTable();
                        string SMS = "";
                        string StageName = "";
                        string StageName_Regional = "";
                        try
                        {
                            WebClient wc = new WebClient();
                            wc.Encoding = Encoding.UTF8;

                            string AdvisoryData = wc.DownloadString(ConfigManager.ApiSecuFarmBaseUrl + "/Utility/GetCropAdvisory/" + FarmID + "/" + prefPoplan + "");


                            DashBordAdvisory objDA = JsonConvert.DeserializeObject<DashBordAdvisory>(AdvisoryData);
                            Console.WriteLine("==>client is==>" + Client + "Sending POP - " + i + "/" + DTDabwalfarms.Rows.Count);
                            sendPOPMessages(lstWorkMaster, PolicyID, Client, DTDabwalfarms, status, i, 
                                stateID,ref  hindivillage, out VillageID, out Village, FarmID, mobileno, sqlhead_sentmessages,  out DTPOPSMS, ref SMS,
                                ref StageName, ref StageName_Regional, objDA, ID.intTP(), LanguageID);
                            Console.WriteLine("==>client is==>" + Client + "Sending ProblemSolution - " + i + "/" + DTDabwalfarms.Rows.Count);
                            sendCC_PS_Messages("PS", Client, DTDabwalfarms,  status, i, stateID, ref hindivillage, out VillageID, out Village, FarmID, mobileno, sqlhead_sentmessages, out DTPOPSMS, ref SMS, ref StageName, ref StageName_Regional, objDA, ID.intTP(), LanguageID, PolicyID);
                            Console.WriteLine("==>client is==>" + Client + "Sending CropCondition - " + i + "/" + DTDabwalfarms.Rows.Count);
                            sendCC_PS_Messages("CC", Client, DTDabwalfarms,  status, i, stateID, ref hindivillage, out VillageID, out Village, FarmID, mobileno, sqlhead_sentmessages, out DTPOPSMS, ref SMS, ref StageName, ref StageName_Regional, objDA, ID.intTP(), LanguageID, PolicyID);

                            if (DTLastChecked.Rows.Count > 0)
                            {
                                execQuery("update pop.sendlog set LastCheckedDate = '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "' where FarmID = " + FarmID);
                                  }
                            else
                            {
                                execQuery("insert into pop.sendlog (FarmID, LastCheckedDate) values (" + FarmID + ", '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                            }



                        }
                        catch (Exception ex)
                        { }
                    }


                }
            }
        }
        private void sendCC_PS_Messages(string CC_PS, string Client, DataTable DTDabwalfarms,  string status, int i, string stateID, ref string hindivillage, out string VillageID, out string Village, string FarmID, string mobileno, string sqlhead_sentmessages, out DataTable DTPOPSMS, ref string SMS, ref string StageName, ref string StageName_Regional, DashBordAdvisory objDA, int ProjectID, int LanguageID, int PolicyID)
        {
            string IDCol = "";
            string TableName = "";
            Village = "";
            VillageID = "";
            string ccsid = "";
            if (CC_PS == "CC")
            {
                DTPOPSMS = objDA.dtCropCondition;
                IDCol = "CCPID";
                TableName = "sentcropcondition";
                ccsid = "CropCondition";
            }
            else
            {
                DTPOPSMS = objDA.dtProblemSolution;
                IDCol = "PSPId";
                TableName = "sentproblemsolution";
                ccsid = "ProblemSolution";
            }
            sqlhead_sentmessages = "insert into pop." + TableName + "(FarmID, WorkID, SendDate) values ";
            try
            {
                
                var sss = DTPOPSMS.AsEnumerable()
               .Select(r => r.Field<int>(IDCol));
                List<Int64> list = DTPOPSMS.AsEnumerable()
               .Select(r => r.Field<Int64>(IDCol))
               .ToList();
                var lstWorkIDs = DTPOPSMS.AsEnumerable().Select(a => a.Field<Int64>(IDCol)).ToList();
                if (lstWorkIDs.Count == 0)
                    return;
                string strWorkIDS = string.Join(",", lstWorkIDs);
                List<int> lstSentWorkIDs = new List<int>();
                DataTable DTSentMessages = getData("select * from pop." + TableName + " where FarmID = " + FarmID + " and WorkID in (" + strWorkIDS + ") and SendDate >= '" + DateTime.Now.AddDays(-11).ToString("yyyy-MM-dd") + "'");
                if (DTSentMessages.Rows.Count > 0)
                {

                    lstSentWorkIDs = DTSentMessages.AsEnumerable().Select(a => a.Field<int>("WorkID")).ToList();
                }

                for (int i_msg = 0; i_msg < DTPOPSMS.Rows.Count; i_msg++)
                {
                    if (i_msg == 0)
                    {
                        DataTable DTStage = getData("select * from pop.pop_stage where stageid = " + DTPOPSMS.Rows[i_msg]["StageID"].ToString());
                        StageName = DTStage.Rows[0]["stagename"].ToString();
                    }
                    int CurWorkID = DTPOPSMS.Rows[i_msg][IDCol].ToString().intTP();
                    if (lstSentWorkIDs.Contains(CurWorkID))
                    {
                        DTPOPSMS.Rows.RemoveAt(i_msg);
                        i_msg--;
                        continue;
                    }
                    //if (SMS != "")
                    //    SMS += Environment.NewLine + Environment.NewLine;

                    //SMS += DTPOPSMS.Rows[i_msg]["Work"].ToString();

                }



                if (DTPOPSMS.Rows.Count == 0)
                {
                    Console.WriteLine(CC_PS + " no rows found");
                    return;

                }

                Console.WriteLine(CC_PS + " " + DTPOPSMS.Rows.Count + " rows found");
                string CCSID = StageName;
                string SndFrom = Client + "_" + CC_PS + "_" + StageName;
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

                Console.WriteLine("Starting for Village" + Village);

                 //if (stateID == "" || stateID.ToLower() == "0")
                 //    return;


                DataTable DTMicronuterentvalue = new DataTable();
                int entry = 0;


             

                string Status = "";
                int Ctr = 0;
                for (int i_msg = 0; i_msg < DTPOPSMS.Rows.Count; i_msg++)
                { 
                    string Solution = "";
                    if (CC_PS == "PS")
                        Solution = DTPOPSMS.Rows[i_msg]["Solution"].ToString();
                    SMS = DTPOPSMS.Rows[i_msg]["Description"].ToString();
                    string Name = DTPOPSMS.Rows[i_msg]["Name"].ToString();
                    string Name_English = DTPOPSMS.Rows[i_msg]["Name_English"].ToString();
                    string ImageName = DTPOPSMS.Rows[i_msg]["imagefile"].ToString();
                    if(!ImageName.Contains("WeedImages"))
                        ImageName = "https://ndviimages.s3.ap-south-1.amazonaws.com/WeedImages/" + ImageName;
                    int CurWorkID = DTPOPSMS.Rows[i_msg][IDCol].ToString().intTP();
                    string CurWorkName = "";
                    string MessageType = ccsid + " - " + StageName;

                    if (Name == "" || SMS == "" || (CC_PS == "PS" && Solution == ""))
                        continue;

                    if (CC_PS == "PS")
                        SMS = Name + " - " + SMS + Environment.NewLine + Environment.NewLine + Solution;
                    else 
                        SMS = Name + " - " + SMS;

                    if (SMS != "")
                    {
                        Ctr++;
                        int Notificationflag = 0;
                        if (Ctr > 1)
                            Notificationflag = 6;

                        //execQuery("insert mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate, MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', 'Send', '" + SMS.Trim() + "', '" + MessageType + "', '" + MessageType + "')");

                        if (execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, Status, MsgMode, Channel," +
                            " ccSid ,OutDate,FarmID,Notificationflag, ImageName,keyid, ProjectID, LanguageID, SMSPolicyID) values ('" + Client + "Farmers_" + ccsid + "', " +
                            "'" + mobileno + "', '" + Client + "', '" + Client + " Subject', '" + SMS.Trim() + "', 'NotSent', " +
                            "'Unicode', 'Gateway2', '" + MessageType + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                            + "','" + FarmID + "', " + Notificationflag + ",'" + ImageName + "'," + CurWorkID + ", " + ProjectID + ", " + LanguageID + ", " + PolicyID + ")"))
                        {
                            execQuery(sqlhead_sentmessages + "(" + FarmID + "," + CurWorkID + ",'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                        }

                    }

                    if (SMS != "")
                        Console.WriteLine(Client + " added (" + i + "/" + DTDabwalfarms.Rows.Count + ") (" + i_msg + "/" + DTPOPSMS.Rows.Count + ") at " + DateTime.Now);

                }
            }
            catch(Exception ex)
            {

            }
        }

        private void sendPOPMessages(List<StringString> lstWorkMaster,int PolicyID, string Client, DataTable DTDabwalfarms,  string status, int i, string stateID, ref string hindivillage, out string VillageID,  out string Village, string FarmID, string mobileno, string sqlhead_sentmessages, out DataTable DTPOPSMS, ref string SMS, ref string StageName, ref string StageName_Regional, DashBordAdvisory objDA,int ProjectID, int LanguageID)
        {
            Village = "";
            VillageID = "";
            DTPOPSMS = objDA.NextStep.lstnextPopDT;
            if(DTPOPSMS.Rows.Count>0)
            {

            }
            var sss = DTPOPSMS.AsEnumerable()
           .Select(r => r.Field<int>("WorkMapID"));
            List<Int64> list = DTPOPSMS.AsEnumerable()
           .Select(r => r.Field<Int64>("WorkID"))
           .ToList();
            var lstWorkIDs = DTPOPSMS.AsEnumerable().Select(a => a.Field<Int64>("WorkMapID")).ToList();
            string strWorkIDS = string.Join(",", lstWorkIDs);
            List<int> lstSentWorkIDs = new List<int>();
            DataTable DTSentMessages = getData("select * from pop.sentmessages where FarmID = " + FarmID + " and WorkID in (" + strWorkIDS + ") and LastCheckedDate >= '" + DateTime.Now.AddDays(-11).ToString("yyyy-MM-dd") + "'");
            if (DTSentMessages.Rows.Count > 0)
            {
                lstSentWorkIDs = DTSentMessages.AsEnumerable().Select(a => a.Field<int>("WorkID")).ToList();
            }

            for (int i_msg = 0; i_msg < DTPOPSMS.Rows.Count; i_msg++)
            {
                if (i_msg == 0)
                {
                    DataTable DTStage = getData("select * from pop.pop_stage where stageid = " + DTPOPSMS.Rows[i_msg]["StageID"].ToString());
                    StageName = DTStage.Rows[0]["stagename"].ToString();
                    StageName_Regional = DTPOPSMS.Rows[i_msg]["StageName"].ToString();
                }
                int CurWorkID = DTPOPSMS.Rows[i_msg]["WorkMapID"].ToString().intTP();
                if (lstSentWorkIDs.Contains(CurWorkID))
                {
                    DTPOPSMS.Rows.RemoveAt(i_msg);
                    i_msg--;
                    continue;
                }
                //if (SMS != "")
                //    SMS += Environment.NewLine + Environment.NewLine;

                //SMS += DTPOPSMS.Rows[i_msg]["Work"].ToString();

            }



              if (DTPOPSMS.Rows.Count == 0)
                   return;

            string CCSID = StageName;
            string SndFrom = Client + "_POP_" + StageName;
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

            Console.WriteLine("Starting for Village" + Village);

             //if (stateID == "" || stateID.ToLower() == "0")
             //    return;


            DataTable DTMicronuterentvalue = new DataTable();
            int entry = 0;


         

            string Status = "";

            for (int i_msg = 0; i_msg < DTPOPSMS.Rows.Count; i_msg++)
            {
                SMS = DTPOPSMS.Rows[i_msg]["Recommendation"].ToString();
                int CurWorkID = DTPOPSMS.Rows[i_msg]["WorkID"].ToString().intTP();
                int CurWorkMapID = DTPOPSMS.Rows[i_msg]["WorkMapID"].ToString().intTP();
                string CurWorkName = lstWorkMaster.Find(a => a.Str1 == CurWorkID.ToString()).Str2;
                string MessageType = "POP - " + StageName + " - " + CurWorkName;

                if (SMS != "")
                {
                    //execQuery("insert mfi.farm_sms_status_master (FarmID, ScheduleDate, LogDate, MsgStatus, Message, MessageType,DummyMessageType) values ('" + FarmID + "', '" + DateTime.Now.ToString("yyyy-MM-dd") + "', '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "', 'Send', '" + SMS.Trim() + "', '" + MessageType + "', '" + MessageType + "')");

                    if (execQuery("insert into wrserver1.smsout(SndFrom, SndTo, MsgType, Subject, message, " +
                        "Status, MsgMode, Channel, ccSid ,OutDate,FarmID,keyid , ProjectID, LanguageID, SMSPolicyID) " +
                        " values " +
                        "('" + Client + "Farmers', '" + mobileno + "', '" + Client + "', '" + Client + " Subject', '" + SMS.Trim() + "', " +
                        "'" + status + "', 'Unicode', 'Gateway2', '" + MessageType + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + 
                        "','" + FarmID + "'," + CurWorkMapID + ", " + ProjectID + ", " + LanguageID + ", " + PolicyID + ")"))
                        execQuery(sqlhead_sentmessages + "(" + FarmID + "," + CurWorkMapID + ",'" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");

                }

                if (SMS != "")
                    Console.WriteLine(Client + " added (" + i + "/" + DTDabwalfarms.Rows.Count + ") (" + i_msg + "/" + DTPOPSMS.Rows.Count + ") at " + DateTime.Now);

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

                    if(messageType.Contains("pop")|| messageType.Contains("Pop") || messageType.Contains("POP"))
                    {
                        if (DateTime.Now.Date>= dtLogDate.Date)
                            return true;
                    }


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

        private DataTable getDabwaliFarms(string ID, string client, int PolicyMasterID)
        {


            DataTable DT = new DataTable();
            bool ConnFound = Connection();
            string sql = "";

            try
            {
                if (ConnFound)
                {
                    

                    //sql = "select info.AlterPhoneNo, fcrop.cropid,map.FarmID,gfs.RefId,vm.District,info.VillageID,vm.Village_Final as VillageName,vm.VillageName_Hindi,vm.District,vm.Sub_district,vm.WRMS_StateID," +
                    //    "info.state,info.FarmerName,info.PhoneNumber,(info.MaxLat+info.MinLat)/2 as latitude, (info.MaxLon+info.MinLon)/2 as " +
                    //    "longitude,Date(fcrop.CropFrom) as sowingdate,chkrn.RainAlertStatus from mfi.clientfarmmapping2 map left join wrserver1.yfi_farminfo info " +
                    //    "on map.FarmID=info.ID left	join wrserver1.yfi_farmcrop fcrop on fcrop.FarmID=map.FarmID left join test.sentinel_village_master vm " +
                    //    "on vm.Village_ID=info.VillageID left join wrwdata.mfi_gfs_farm as gfs on info.ID = gfs.Id left join mfi.rain_alert_checkstatus as chkrn on chkrn.FarmID=info.ID  where map.ClientID='" + ID + "' ";

                    sql = "select map.policyid, info.AlterPhoneNo, map.policycropid CropID,info.ID as FarmID,info.RefId,vm.District,info.VillageID,vm.Village_Final as VillageName,vm.VillageName_Hindi,vm.District,vm.Sub_district,vm.WRMS_StateID," +
                        "info.state,info.FarmerName,info.PhoneNumber,(info.MaxLat+info.MinLat)/2 as latitude, (info.MaxLon+info.MinLon)/2 as " +
                        "longitude,Date(map.CropStart) as sowingdate,chkrn.RainAlertStatus, lm.Language from policy.policy map left join wrserver1.yfi_farminfo info " +
                        "on map.FarmID=info.ID left join test.sentinel_village_master vm  " +
                        "on vm.Village_ID=info.VillageID " +
                        " left join mfi.rain_alert_checkstatus as chkrn on chkrn.FarmID=info.ID " +
                        " left join mfi.language_master lm on info.languageid = lm.ID left join policy.policyplans plan on plan.planid=map.planid " +
                        " left join policy.policymaster pm on pm.policymasterid=plan.policymasterid where " +
                        "map.MapProjectID=" + ID + " and pm.policymasterid= " + PolicyMasterID + " and VerificationStatus=1 and CurrentStatus=1  group by  info.ID";


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

            DataTable DTSMSTypes = getSMSMaster(Client,"");


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

                //string apiAddrForecast = "http://3.88.31.90:82/wdrest.svc/MergeWZDailyForecast_v2/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/English/json/wrinternal";;
                string apiAddrForecast = "https://weather-risk.com/wdrest.svc/Weather/WZDailyForecast/" + Latitude + "," + Longitude + "/New%20Delhi/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "/District/json/internal";
                string apiAddrWeather = "http://3.88.31.90:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-2).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no"; ;

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



                string apiAddrWeather = "http://3.88.31.90:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no"; ;

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


                string apiAddrWeather = "http://3.88.31.90:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no"; ;

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
                string apiAddrWeather = "http://3.88.31.90:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no";

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
                string apiAddrForecast = "http://3.88.31.90:82/wdrest.svc/WZDailyForecast_v2/" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(7).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/English/json/mfi";

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
                string apiAddr = "http://3.88.31.90:82/wdrest.svc/getMergeWeatherData/" + DateTime.Now.AddDays(-3).ToString("yyyy-MM-dd") + "/" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + "/" + Latitude + "/" + Longitude + "/00/0/%27%27/i,g,p/json/wrinternal/English/no";

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

                    if (prefweatherlanguage.ToLower() == "hindi")
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

    public class ExcelHelper
    {
        //Row limits older excel verion per sheet, the row limit for excel 2003 is 65536
        const int rowLimit = 5000000;

        private static string getWorkbookTemplate()
        {
            var sb = new StringBuilder(818);
            sb.AppendFormat(@"<?xml version=""1.0""?>{0}", Environment.NewLine);
            sb.AppendFormat(@"<?mso-application progid=""Excel.Sheet""?>{0}", Environment.NewLine);
            sb.AppendFormat(@"<Workbook xmlns=""urn:schemas-microsoft-com:office:spreadsheet""{0}", Environment.NewLine);
            sb.AppendFormat(@" xmlns:o=""urn:schemas-microsoft-com:office:office""{0}", Environment.NewLine);
            sb.AppendFormat(@" xmlns:x=""urn:schemas-microsoft-com:office:excel""{0}", Environment.NewLine);
            sb.AppendFormat(@" xmlns:ss=""urn:schemas-microsoft-com:office:spreadsheet""{0}", Environment.NewLine);
            sb.AppendFormat(@" xmlns:html=""http://www.w3.org/TR/REC-html40"">{0}", Environment.NewLine);
            sb.AppendFormat(@" <Styles>{0}", Environment.NewLine);
            sb.AppendFormat(@"  <Style ss:ID=""Default"" ss:Name=""Normal"">{0}", Environment.NewLine);
            sb.AppendFormat(@"   <Alignment ss:Vertical=""Bottom""/>{0}", Environment.NewLine);
            sb.AppendFormat(@"   <Borders/>{0}", Environment.NewLine);
            sb.AppendFormat(@"   <Font ss:FontName=""Calibri"" x:Family=""Swiss"" ss:Size=""11"" ss:Color=""#000000""/>{0}", Environment.NewLine);
            sb.AppendFormat(@"   <Interior/>{0}", Environment.NewLine);
            sb.AppendFormat(@"   <NumberFormat/>{0}", Environment.NewLine);
            sb.AppendFormat(@"   <Protection/>{0}", Environment.NewLine);
            sb.AppendFormat(@"  </Style>{0}", Environment.NewLine);
            sb.AppendFormat(@"  <Style ss:ID=""s62"">{0}", Environment.NewLine);
            sb.AppendFormat(@"   <Font ss:FontName=""Calibri"" x:Family=""Swiss"" ss:Size=""11"" ss:Color=""#000000""{0}", Environment.NewLine);
            sb.AppendFormat(@"    ss:Bold=""1""/>{0}", Environment.NewLine);
            sb.AppendFormat(@"  </Style>{0}", Environment.NewLine);
            sb.AppendFormat(@"  <Style ss:ID=""s63"">{0}", Environment.NewLine);
            sb.AppendFormat(@"   <NumberFormat ss:Format=""Short Date""/>{0}", Environment.NewLine);
            sb.AppendFormat(@"  </Style>{0}", Environment.NewLine);
            sb.AppendFormat(@" </Styles>{0}", Environment.NewLine);
            sb.Append(@"{0}\r\n</Workbook>");
            return sb.ToString();
        }

        private static string replaceXmlChar(string input)
        {
            input = input.Replace("&", "&amp");
            input = input.Replace("<", "&lt;");
            input = input.Replace(">", "&gt;");
            input = input.Replace("\"", "&quot;");
            input = input.Replace("'", "&apos;");
            return input;
        }

        private static string getCell(Type type, object cellData)
        {
            /*string CurVal = cellData.ToString();
            if(CurVal.Contains("*"))
            {
                CurVal=CurVal.Replace("*","");
                CurVal = "<font color='Red'>" + CurVal + "</font>";
                cellData = CurVal;
            }*/

            var data = (cellData is DBNull) ? "" : cellData;
            if (type.Name.Contains("Int") || type.Name.Contains("Double") || type.Name.Contains("Decimal")) return string.Format("<Cell><Data ss:Type=\"Number\">{0}</Data></Cell>", data);
            if (type.Name.Contains("Date") && data.ToString() != string.Empty)
            {
                return string.Format("<Cell ss:StyleID=\"s63\"><Data ss:Type=\"DateTime\">{0}</Data></Cell>", Convert.ToDateTime(data).ToString("yyyy-MM-dd"));
            }

            return string.Format("<Cell><Data ss:Type=\"String\">{0}</Data></Cell>", replaceXmlChar(data.ToString()));
        }
        private static string getWorksheets(DataSet source)
        {
            var sw = new StringWriter();

            if (source == null || source.Tables.Count == 0)
            {
                sw.Write("<Worksheet ss:Name=\"Sheet1\">\r\n<Table>\r\n<Row><Cell><Data ss:Type=\"String\"></Data></Cell></Row>\r\n</Table>\r\n</Worksheet>");
                return sw.ToString();
            }
            foreach (DataTable dt in source.Tables)
            {
                if (dt.Rows.Count == 0)
                    sw.Write("<Worksheet ss:Name=\"" + replaceXmlChar(dt.TableName) + "\">\r\n<Table>\r\n<Row><Cell  ss:StyleID=\"s62\"><Data ss:Type=\"String\"></Data></Cell></Row>\r\n</Table>\r\n</Worksheet>");
                else
                {
                    //write each row data                
                    var sheetCount = 0;
                    for (int i = 0; i < dt.Rows.Count; i++)
                    {
                        if ((i % rowLimit) == 0)
                        {
                            //add close tags for previous sheet of the same data table
                            if ((i / rowLimit) > sheetCount)
                            {
                                sw.Write("\r\n</Table>\r\n</Worksheet>");
                                sheetCount = (i / rowLimit);
                            }
                            sw.Write("\r\n<Worksheet ss:Name=\"" + replaceXmlChar(dt.TableName) +
                                     (((i / rowLimit) == 0) ? "" : Convert.ToString(i / rowLimit)) + "\">\r\n<Table>");
                            //write column name row
                            sw.Write("\r\n<Row>");
                            foreach (DataColumn dc in dt.Columns)
                                sw.Write(string.Format("<Cell ss:StyleID=\"s62\"><Data ss:Type=\"String\">{0}</Data></Cell>", replaceXmlChar(dc.ColumnName)));
                            sw.Write("</Row>");
                        }
                        sw.Write("\r\n<Row>");
                        foreach (DataColumn dc in dt.Columns)
                            sw.Write(getCell(dc.DataType, dt.Rows[i][dc.ColumnName]));
                        sw.Write("</Row>");
                    }
                    sw.Write("\r\n</Table>\r\n</Worksheet>");
                }
            }
            string data = sw.ToString();
            sw.Flush();
            return data;

        }
        public static string GetExcelXml(DataTable dtInput, string filename)
        {
            var excelTemplate = getWorkbookTemplate();
            var ds = new DataSet();
            ds.Tables.Add(dtInput.Copy());
            var worksheets = getWorksheets(ds);
            var excelXml = string.Format(excelTemplate, worksheets);
            return excelXml;
        }

        public static string GetExcelXml(DataSet dsInput, string filename)
        {
            var excelTemplate = getWorkbookTemplate();
            var worksheets = getWorksheets(dsInput);
            var excelXml = string.Format(excelTemplate, worksheets);
            return excelXml;
        }

        public static void ToExcel(DataSet dsInput, string filename)
        {
            StreamWriter wr = new StreamWriter(filename, true, Encoding.GetEncoding("UTF-16"));
            var excelXml = GetExcelXml(dsInput, filename);
            wr.Write(excelXml);
            wr.Close();


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

    public class stringstring
    {
        public string str1 { get; set; }
        public string str2 { get; set; }
        public string AlertType { get; set; }
        public stringstring()
        {
            str1 = "";
            str2 = "";
            AlertType = "";
        }
    }



    public class DashBordAdvisory
    {


        public NextStepFollow NextStep { get; set; }
        public CropStatus cropStatus { get; set; }
        public List<SMS> SMSLst { get; set; }
        public List<IntractiveSMS> IntractiveLst { get; set; }
        public IrrigationAdvisory IrrigationAdvisory { get; set; }
        public WeatherForecast weatherForecast { get; set; }
        public PestDiseaseAlert PestDiseaseAlert { get; set; }
        public PriceOutlook PriceOutlook { get; set; }
        public SoilTestInformation SoilTest { get; set; }
        public string FarmScoreMessage { get; set; }
        public double FarmScore { get; set; }
        public string Logo { get; set; }
        public DataTable dtCropCondition { get; set; }
        public DataTable dtProblemSolution { get; set; }

        public DashBordAdvisory()
        {
            IntractiveLst = new List<IntractiveSMS>();
            SMSLst = new List<SMS>();
            NextStep = new NextStepFollow();
            SoilTest = new SoilTestInformation();
            cropStatus = new CropStatus();
            IrrigationAdvisory = new IrrigationAdvisory();
            weatherForecast = new WeatherForecast();
            PestDiseaseAlert = new PestDiseaseAlert();
            PriceOutlook = new PriceOutlook();
            FarmScoreMessage = "";
            Logo = "";
            dtCropCondition = new DataTable();
            dtProblemSolution = new DataTable();

        }
    }


    public class SoilTestInformation
    {
        public bool Available { get; set; }
        public DataTable DataTable { get; set; }
        public SoilTestInformation()
        {
            DataTable = new DataTable();
            Available = false;
        }
    }

    public class PestDiseaseAlert
    {
        public bool Available { get; set; }
        public List<PestDiseaseData> lstPestDiseaseData { get; set; }

        public PestDiseaseAlert()
        {

            Available = false;
            lstPestDiseaseData = new List<PestDiseaseData>();

        }
    }

    public class PestDiseaseData
    {
        public string pestname { get; set; }
        public List<string> pestdescription { get; set; }
        public List<string> Imagelst { get; set; }
        public bool likelihood { get; set; }
        public List<string> lstmanagement { get; set; }
        public string NextStep { get; set; }
        public PestDiseaseData()
        {
            likelihood = false;
            pestdescription = new List<string>();
            Imagelst = new List<string>();
            lstmanagement = new List<string>();
            NextStep = "";
        }
    }

    public class IrrigationAdvisory
    {
        public List<string> lstirrigationadvisory { get; set; }
        public string CurrentDASfromto { get; set; }
        public string CurrentDatefrom { get; set; }
        public string CurrentDateto { get; set; }
        public DataTable irrgationadvisoryDT { get; set; }
        public string dynamicguidance { get; set; }
        public IrrigationAdvisory()
        {
            CurrentDASfromto = "";
            CurrentDatefrom = "";
            CurrentDateto = "";
            irrgationadvisoryDT = new DataTable();
            lstirrigationadvisory = new List<string>();
        }
    }

    public class PriceOutlook
    {

    }

    public class WeatherForecast
    {
        public bool Available { get; set; }
        public string forecastdata { get; set; }
        public List<string> lstAlertMessages { get; set; }
        public WeatherForecast()
        {
            lstAlertMessages = new List<string>();
            Available = false;
        }
    }

    public class NextStepFollow
    {
        public bool Available { get; set; }
        public List<string> lstnextPop { get; set; }
        public List<string> diseasemessagelst { get; set; }
        public List<string> weathermessagelst { get; set; }
        public DataTable lstnextPopDT { get; set; }
        public string CurrentDASfromto { get; set; }
        public string CurrentDatefrom { get; set; }
        public string CurrentDateto { get; set; }
        public List<string> lstExpertAdvisory { get; set; }
        public string AutoIrrigationMessage { get; set; }
        public string soiladvisorymsg { get; set; }
        public NDVIAdvisory nDVIAdvisory { get; set; }
        public NextStepFollow()
        {
            weathermessagelst = new List<string>();
            Available = false;
            soiladvisorymsg = "";
            CurrentDASfromto = "";
            CurrentDatefrom = "";
            CurrentDateto = "";
            lstnextPop = new List<string>();
            lstnextPopDT = new DataTable();
            lstExpertAdvisory = new List<string>();
            diseasemessagelst = new List<string>();
            nDVIAdvisory = new NDVIAdvisory();
        }
    }

    public class NDVIAdvisory
    {
        public DataTable Recomondation { get; set; }
        public string message { get; set; }
        public NDVIAdvisory()
        {
            Recomondation = new DataTable();
            message = "";
        }
    }

    public class CropStatus
    {
        public Ndvi Ndvi { get; set; }
        public RainFall rain { get; set; }
        public SoilMoisture soilMoisture { get; set; }
        public List<Crop_DiseaseCondition> lstdisesecond { get; set; }
        public List<Crop_WeatherCondition> lstweathercond { get; set; }
        public List<string> lstdiseasealert { get; set; }
        public List<string> lstweatheralert { get; set; }
        public bool Available { get; set; }
        public string status { get; set; }
        public string statusThumb { get; set; }
        public CropStatus()
        {
            lstdiseasealert = new List<string>();
            lstweatheralert = new List<string>();
            Available = false;
            status = "Normal";
            statusThumb = "up";
            Ndvi = new Ndvi();
            rain = new RainFall();
            soilMoisture = new SoilMoisture();
            lstdiseasealert = new List<string>();
            lstdisesecond = new List<Crop_DiseaseCondition>();
            lstweathercond = new List<Crop_WeatherCondition>();

        }
    }


    public class Crop_DiseaseCondition
    {
        public string diseaseName { get; set; }
        public string MaxTemp { get; set; }
        public string MinTemp { get; set; }
        public string AvgTemp { get; set; }
        public string hummor { get; set; }
        public string humeve { get; set; }
        public string rh { get; set; }
        public string Diurnal { get; set; }
        public string Conducivedays { get; set; }
        public string Diseaseconducivedates { get; set; }
        public Crop_DiseaseCondition()
        {
            diseaseName = "";
            MaxTemp = "";
            MinTemp = "";
            AvgTemp = "";
            hummor = "";
            humeve = "";
            rh = "";
            Diurnal = "";
            Conducivedays = "";
            Diseaseconducivedates = "";
        }
    }

    public class Crop_WeatherCondition
    {
        public string WeatherEventName { get; set; }
        public string MaxTemp { get; set; }
        public string MinTemp { get; set; }
        public string AvgTemp { get; set; }
        public string hummor { get; set; }
        public string humeve { get; set; }
        public string rh { get; set; }
        public string Rain { get; set; }
        public string TotalRain { get; set; }
        public string WindSpeed { get; set; }
        public string Conducivedays { get; set; }
        public string Weatherconducivedates { get; set; }
        public Crop_WeatherCondition()
        {
            WeatherEventName = "";
            MaxTemp = "";
            MinTemp = "";
            AvgTemp = "";
            hummor = "";
            humeve = "";
            rh = "";
            Rain = "";
            TotalRain = "";
            WindSpeed = "";
            Conducivedays = "";
            Weatherconducivedates = "";
        }
    }


    public class Ndvi
    {
        public string Status { get; set; }
        public string NdviValue { get; set; }
        public string NdviBenchMark { get; set; }
        public Ndvi()
        {
            Status = "";
            NdviBenchMark = "";
            NdviValue = "";

        }

    }



    public class RainFall
    {
        public string Status { get; set; }
        public string Value { get; set; }
        public string Benchmark { get; set; }
        public RainFall()
        {
            Status = "";
            Value = "";
            Benchmark = "";
        }

    }
    public class SoilMoisture
    {
        public string Status { get; set; }
        public string SoilValue { get; set; }
        public string SoilBenchMark { get; set; }
        public SoilMoisture()
        {
            Status = "";
            SoilValue = "";
            SoilBenchMark = "";

        }

    }


    public class SMS
    {
        public string Message { get; set; }
        public string CCSID { get; set; }

        public DateTime OutDate { get; set; }

        public DateTime FeddbackDate { get; set; }

        public string MessageType { get; set; }

        public string Feedback { get; set; }

        public int Id { get; set; }

        public bool foundflag { get; set; }

        public SMS()
        {
            Feedback = "";
            foundflag = false;
        }

    }
    public class IntractiveSMS
    {
        public string Message { get; set; }

        public DateTime OutDate { get; set; }

        public DateTime FeddbackDate { get; set; }

        public string MessageType { get; set; }

        public string Feedback { get; set; }

        public int Id { get; set; }

        public bool foundflag { get; set; }

        public IntractiveSMS()
        {
            Feedback = "";
            foundflag = false;
        }

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
      
      
    }

    

}
