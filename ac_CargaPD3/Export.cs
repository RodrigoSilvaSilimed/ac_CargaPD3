using System;
using System.Collections.Generic;
using System.Text;

using FireSharp.Config;
using FireSharp.Interfaces;
using FireSharp.Response;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using ac_CargaPD3.Model;

namespace ac_CargaPD3
{
    public class Export
    {
        private FirebaseConfig config; //Configuração de conexão a com base do App AppInventro
        private FirebaseConfig configX; //Configuração de conexão a com base do App Xamarin
        private SqlConnection con;

        public Export()
        {

            con = new SqlConnection("Data Source=SAN;Initial Catalog=GESTOR;User ID=GESTOR;Password=g1e2s3t4o5r6");

            config = new FirebaseConfig
            {
                //Credenciais App Iventor
                AuthSecret = "txcKeG10ZDvcF3zJRNw1szaeICtSMHav4jNccqWZ",
                BasePath = "https://invetorypd3.firebaseio.com/"
                                
            };

            configX = new FirebaseConfig
            {
                
                //Credencial App Xamarin
                AuthSecret = "xh6Yuo0zW28QZ2pIsS0MYj3EntRcsb0nzeCwKJ6M",
                BasePath = "https://inventorypd3.firebaseio.com/"
            };

        }
        public async Task<int> IniciarCarga()
        {
            int TotalInseridos;
            IFirebaseClient client = new FireSharp.FirebaseClient(config);
            IFirebaseClient clientX = new FireSharp.FirebaseClient(configX);

            GravaLog("Iniciando", "Carga Iniciada!");

            ApagaTabelaTemporaria();

            Apagarconstraint();


            CriarTabelaTemporaria();

            GravaLog("Executando", "Tabela Temp Recriada!");

            //Obtendo Base AppInventor
            FirebaseResponse res2 = client.Get("/Appv09Bucket/");
            string myJson2 = res2.Body;
            var jsonObj2 = JsonConvert.DeserializeObject<JObject>(myJson2).First.First;

            //Obtendo Base App Xamarin
            FirebaseResponse res1 = clientX.Get("/Appv09Bucket/");
            string myJson1 = res1.Body;
            var jsonObj1 = JsonConvert.DeserializeObject<JObject>(myJson1).First.First;

            GravaLog("Executando", "Json Lido!");

            //DataTable App Inventor
            var DTFireBaseImportado = ImportarFireBaseNovaEstrutura(jsonObj2);

            //DataTable App Xamarin
            var DTFireBaseImportado1 = ImportarFireBaseNovaEstrutura(jsonObj1);

            GravaLog("Executando", "Json Convertido! Total de Registros base AppInventor: " + DTFireBaseImportado.Rows.Count.ToString()+". Total base Xamarin: "+ DTFireBaseImportado1.Rows.Count.ToString());
            
            TotalInseridos = ExportarLeituraSQL(DTFireBaseImportado) + ExportarLeituraSQL(DTFireBaseImportado1);

            GravaLog("Executando", "Json Inserido! Total de Registros: " + TotalInseridos.ToString());

            GravaLog("Fim", "Carga Finalizada!");

            //DadosSilimed();
            //Processar proc = new Processar();
            //proc.RetiraAmostaMedidorMedgel();
            //proc.ExcluirNaoNumerico(); //Ao invés de exlcuir, preciso gerar relatório para tratamento
            //proc.ExcluirMenorQueSete(); //Ao invés de exlcuir, preciso gerar relatório para tratamento

            //Ajustes();
            //ApagarDuplicados();
            return 1;

        }


        private DataTable ImportarEstruturaAntiga(IEnumerable<JToken> jsonObjConc1, List<Model_Leitura_Importada> list2)
        {
            int i = 0;
            DataTable dt = new DataTable();
            dt.Columns.Add("Id");
            dt.Columns.Add("Barcode");
            dt.Columns.Add("Cliente");
            dt.Columns.Add("Data");
            dt.Columns.Add("Endereco");
            dt.Columns.Add("Latitude");
            dt.Columns.Add("Longitude");
            dt.Columns.Add("Timestamp");
            dt.Columns.Add("Usuario");


            foreach (var itemDynamic in jsonObjConc1)
            {
                list2.Add(JsonConvert.DeserializeObject<Model_Leitura_Importada>(((JProperty)itemDynamic).Value.ToString()));

                if (list2[i].Barcode != null)
                {

                    DataRow row = dt.NewRow();
                    if (list2[i].Cliente == null)
                    {
                        row["Id"] = "";
                    }
                    else
                    {
                        row["Id"] = Retira(list2[i].Cliente.Replace('"', ' ').ToString().Trim() + '-' + list2[i].Data.Replace('"', ' ').ToString().Trim() + '-' + list2[i].Barcode.Replace('"', ' ').ToString().Trim());
                    }
                    if (list2[i].Barcode != null)
                    {
                        row["Barcode"] = Retira(list2[i].Barcode.Replace('"', ' ').ToString().Trim());
                    }
                    else
                    {
                        row["Barcode"] = "";
                    }
                    if (list2[i].Cliente != null)
                    {
                        row["Cliente"] = Retira(list2[i].Cliente.Replace('"', ' ').ToString().Trim());
                    }
                    else
                    {
                        row["Cliente"] = "";
                    }
                    if (list2[i].Data != null)
                    {
                        row["Data"] = Retira(list2[i].Data.Replace('"', ' ').ToString().Trim());
                    }
                    else
                    {
                        row["Data"] = "";
                    }

                    if (list2[i].Latitude != null)
                    {
                        row["Latitude"] = list2[i].Latitude.Replace('"', ' ').ToString().Trim();
                    }
                    else
                    {
                        row["Latitude"] = "";
                    }

                    if (list2[i].Longitude != null)
                    {
                        row["Longitude"] = list2[i].Longitude.Replace('"', ' ').ToString().Trim();
                    }
                    else
                    {
                        row["Longitude"] = "";
                    }
                    if (list2[i].Timestamp != null)
                    {
                        row["Timestamp"] = list2[i].Timestamp.Replace('"', ' ').ToString().Trim();
                    }
                    else
                    {
                        row["Timestamp"] = "";
                    }
                    if (list2[i].Usuario != null)
                    {
                        row["Usuario"] = list2[i].Usuario.Replace('"', ' ').ToString().Trim();
                    }
                    else
                    {
                        row["Usuario"] = null;
                    }

                    dt.Rows.Add(row);



                }
                i++;
            }
            return dt;
        }


        private DataTable ImportarFireBaseNovaEstrutura(IEnumerable<JToken> jsonObjConc)
        {
            int i = 0;
            var list = new List<Model_Leitura_Importada>();
            DataTable dt = new DataTable();
            dt.Columns.Add("Id");
            dt.Columns.Add("Barcode");
            dt.Columns.Add("Cliente");
            dt.Columns.Add("Data");
            dt.Columns.Add("Latitude");
            dt.Columns.Add("Longitude");
            dt.Columns.Add("Timestamp");
            dt.Columns.Add("Usuario");

            foreach (var itemDynamicAnoInventario in jsonObjConc)
            {

                var jsonObjN1 = itemDynamicAnoInventario.First;

                foreach (var itemDynamic in jsonObjN1)
                {

                    var jsonObjN2 = itemDynamic.First; //avanço para o nível dos SNs

                    foreach (var itemDynamicN2 in jsonObjN2)
                    {
                        list.Add(JsonConvert.DeserializeObject<Model_Leitura_Importada>(((JProperty)itemDynamicN2).Value.ToString()));

                        if ((list[i].Barcode != null))
                        {
                            DataRow row = dt.NewRow();
                            if (list[i].Cliente == null)
                            {
                                row["Id"] = "";
                            }
                            else
                            {
                                //row["Id"] = Retira(list[i].Cliente.Replace('"', ' ').ToString().Trim() + '-' + list[i].Data.Replace('"', ' ').ToString().Trim() + '-' + list[i].Barcode.Replace('"', ' ').ToString().Trim());
                                row["Id"] = "";
                            }
                            if (list[i].Barcode != null)
                            {
                                row["Barcode"] = Retira(list[i].Barcode.Replace('"', ' ').ToString().Trim());
                            }
                            else
                            {
                                row["Barcode"] = "";
                            }
                            if (list[i].Cliente != null)
                            {
                                row["Cliente"] = Retira(list[i].Cliente.Replace('"', ' ').ToString().Trim());
                            }
                            else
                            {
                                row["Cliente"] = "";
                            }
                            if (list[i].Data != null)
                            {
                                row["Data"] = Retira(list[i].Data.Replace('"', ' ').ToString().Trim());
                            }
                            else
                            {
                                row["Data"] = "";
                            }

                            if (list[i].Latitude != null)
                            {
                                row["Latitude"] = list[i].Latitude.Replace('"', ' ').ToString().Trim();
                            }
                            else
                            {
                                row["Latitude"] = "";
                            }

                            if (list[i].Longitude != null)
                            {
                                row["Longitude"] = list[i].Longitude.Replace('"', ' ').ToString().Trim();
                            }
                            else
                            {
                                row["Longitude"] = "";
                            }
                            if (list[i].Timestamp != null)
                            {
                                row["Timestamp"] = list[i].Timestamp.Replace('"', ' ').ToString().Trim();
                            }
                            else
                            {
                                row["Timestamp"] = "";
                            }
                            if (list[i].Usuario != null)
                            {
                                row["Usuario"] = list[i].Usuario.Replace('"', ' ').ToString().Trim();
                            }
                            else
                            {
                                row["Usuario"] = "";
                            }
                            dt.Rows.Add(row);

                        }
                        i++;
                    }

                }

            }
            return dt;
        }

        private int ExportarLeituraSQL(DataTable dt)
        {
            string cQuery;
            int i;

            con.Open();

            for (i = 0; i < (dt.Rows.Count) - 1; i++)
            {

                // pc.Verificar(dataGridView1.Rows[i].Cells[0].Value.ToString());
                string inventario = (DateTime.Now.ToString("yyyyMM"));

                if (((dt.Rows[i]["Data"].ToString())== inventario))
                {
                    cQuery = "INSERT INTO FireBase_Inventory_temp";
                    cQuery += " (ID,Barcode,Cliente,Data,Latitude,Longitude,Timestamp,Usuario)";
                    cQuery += " VALUES('" + dt.Rows[i]["Id"] + "',";
                    cQuery += " '" + dt.Rows[i]["Barcode"] + "',";
                    cQuery += " '" + dt.Rows[i]["Cliente"] + "',";
                    cQuery += " '" + dt.Rows[i]["Data"] + "',";
                    cQuery += " '" + dt.Rows[i]["Latitude"].ToString().Replace(",", ".") + "',";
                    cQuery += " '" + dt.Rows[i]["Longitude"].ToString().Replace(",",".") + "',";
                    cQuery += " '" + dt.Rows[i]["Timestamp"] + "',";
                    cQuery += " '" + dt.Rows[i]["Usuario"] + "')";

                    SqlCommand cmd = new SqlCommand(cQuery, con);

                    cmd.ExecuteNonQuery();
                }


            }
            dt.Dispose();
            con.Close();
            i++;
            return i;
        }

        private string ExportarLeituraFirebase(string caminho, Model_Leitura_Importada leitura)
        {
            //caminho pode ser por exemplo "/Appv99Bucket/Inventario/"


            IFirebaseClient client = new FireSharp.FirebaseClient(config);

            var response = client.Set(caminho + leitura.Data + "/" + leitura.Cliente + "/" + leitura.Barcode + "/", leitura);
            return JsonConvert.SerializeObject(response).ToString();

        }


        private void Apagarconstraint()
        {
            string cQuery;
            con.Open();


            cQuery = "IF(EXISTS(SELECT COLUMN_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ";
            cQuery += "where CONSTRAINT_name = 'FireBase_Inventory_temp1')) ";
            cQuery += "Begin ";
            cQuery += "ALTER TABLE dbo.FireBase_Inventory ";
            cQuery += "DROP CONSTRAINT FireBase_Inventory_temp1 ";
            cQuery += "end ";

            SqlCommand cmd7 = new SqlCommand(cQuery, con);
            cmd7.ExecuteNonQuery();
            cmd7.Dispose();
            con.Close();
        }


        private void CriarTabelaTemporaria()
        {
            string cQuery;

            con.Open();

            cQuery = "CREATE TABLE[dbo].[FireBase_Inventory_temp]( ";
            cQuery += "[IDENT] [int] IDENTITY(1,1) NOT NULL, ";
            cQuery += "[ID] [varchar] (100)  NOT NULL, ";
            cQuery += "[Barcode] [varchar] (20) NOT NULL, ";
            cQuery += "[Cliente] [varchar] (60) NOT NULL, ";
            cQuery += "[Data] [varchar] (10) NULL, ";
            cQuery += "[Endereco] [varchar] (150) NULL, ";
            cQuery += "[Latitude] FLOAT NOT NULL, ";
            cQuery += "[Longitude] FLOAT NOT NULL, ";
            cQuery += "[Timestamp] varchar(60) NOT NULL, ";
            cQuery += "[Usuario] VARCHAR(70) NOT NULL, ";
            cQuery += "[PRODUTO] VARCHAR(15)  NULL, ";
            cQuery += "[NF] VARCHAR(9)  NULL, ";
            cQuery += "[SERIE] VARCHAR(3)  NULL, ";
            cQuery += "[TES] VARCHAR(3) NULL, ";
            cQuery += "[NATUREZA] VARCHAR(2) NULL, ";
            cQuery += "[DESCRICAO_NATUREZA] VARCHAR(40) NULL, ";
            cQuery += "[LOTE] VARCHAR(10)  NULL, ";
            cQuery += "[Cliente_NF] VARCHAR(6)  NULL, ";
            cQuery += "[NOME_Cliente] VARCHAR(60)  NULL, ";
            cQuery += "[REPRESENTANTE] VARCHAR(6) NULL, ";
            cQuery += "[NOME_REPRESENTANTE] VARCHAR(40) NULL, ";
            cQuery += "CONSTRAINT[FireBase_Inventory_temp1] PRIMARY KEY CLUSTERED ";
            cQuery += "( ";
            cQuery += "[IDENT] ASC ";
            cQuery += ")WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 97) ON[PRIMARY] ";
            cQuery += ") ON[PRIMARY] ";

            SqlCommand cmd5 = new SqlCommand(cQuery, con);
            cmd5.ExecuteNonQuery();
            cmd5.Dispose();
            con.Close();
        }

        private void ApagaTabelaTemporaria()
        {
            string cQuery;

            con.Open();
            cQuery = "IF(EXISTS(SELECT * ";
            cQuery += "FROM INFORMATION_SCHEMA.TABLES ";
            cQuery += "WHERE TABLE_SCHEMA = 'dbo' ";
            cQuery += "AND  TABLE_NAME = 'FireBase_Inventory_temp')) ";
            cQuery += " BEGIN ";
            cQuery += "DROP TABLE FireBase_Inventory_temp ";
            cQuery += "END ";

            SqlCommand cmd6 = new SqlCommand(cQuery, con);
            cmd6.ExecuteNonQuery();
            cmd6.Dispose();
            con.Close();
        }

        private void Ajustes()
        {
            string cQuery;

            cQuery = "UPDATE FireBase_Inventory_temp ";
            cQuery += "SET Barcode = Right(Barcode,7), ";
            cQuery += "Latitude =  round(Latitude,2), ";
            cQuery += "Longitude = round(Longitude,2)";

            SqlCommand cmd7 = new SqlCommand(cQuery, con);
            cmd7.ExecuteNonQuery();
            cmd7.Dispose();
        }

        private void ApagarDuplicados()
        {
            string cQuery;
            //con.Open();
            cQuery = "SELECT Left(Timestamp,6) Timestamp,IDENT, ID, Barcode ";
            cQuery += "FROM[dbo].[FireBase_Inventory_temp] ";
            cQuery += "where Barcode in( ";
            cQuery += "SELECT  Barcode ";
            cQuery += "from[FireBase_Inventory_temp] ";
            // cQuery += " where UPPER(ID) like '%SILISUL%'";
            cQuery += "group by Barcode ";

            cQuery += "having count(*) > 1) ";
            cQuery += "order by Left(Timestamp,6),Barcode,IDENT, ID DESC";

            SqlCommand cmd = new SqlCommand(cQuery, con);

            SqlDataReader dr2 = cmd.ExecuteReader();

            if (dr2.Read())
            {
                cmd.Dispose();
                /* Processar pc = new Processar();

                 while (dr2.Read())
                 {
                     pc.ExecutarDelete(dr2["Timestamp"].ToString(), dr2["IDENT"].ToString(), dr2["ID"].ToString(), dr2["Barcode"].ToString());
                 }
                 */
            }

            dr2.Dispose();

        }

        private void GravaLog(string status, string detalhe)
        {
            string cQuery;
            con.Open();
            cQuery = "INSERT INTO [dbo].[FireBase_Log] ([Status],[DateTime],[Detail]) VALUES ";
            cQuery += "('" + status.Trim();
            cQuery += "', getdate()";
            cQuery += ", '" + detalhe.Trim() + "')";
            SqlCommand cmd7 = new SqlCommand(cQuery, con);
            cmd7.ExecuteNonQuery();
            cmd7.Dispose();
            con.Close();
        }


        private static string Retira(string cTexto)
        {

            StringBuilder sbReturn = new StringBuilder();
            var arrayText = cTexto.Normalize(NormalizationForm.FormD).ToCharArray();
            foreach (char letter in arrayText)
            {
                if (CharUnicodeInfo.GetUnicodeCategory(letter) != UnicodeCategory.NonSpacingMark)
                    if (CharUnicodeInfo.GetUnicodeCategory(letter) != UnicodeCategory.OtherPunctuation)
                    {
                        sbReturn.Append(letter);
                    }
            }
            return sbReturn.ToString();
        }


    }
}
