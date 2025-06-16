using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using RSFacWeb.Entities.MKTPUBLIC;
using RSFacWeb.Models.Databases;
using RSFacWeb.ModelsView;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System;
using RSFacWeb.Interfaces;
using System.Data;
using System.Linq;
using Amazon;
using RSFacWeb.Entities;
using RSFacLocal.ModelsView;
using RSFacWeb.Utils;
using System.Diagnostics;
using RSFacLocal.Entities;
using RSFacLocal.Entities.CDRO;
using RSFacWeb.ModelsView.XML.ModelsView;
using RSFacWeb.ModelsView.FELOG;
using System.Net;
using System.Text;
using RSFacLocal.ModelsView.CPE;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using RSFacLocal.XML.Entities.Declarador;
using RSFacWeb.ModelsView.FERESC;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using RS_FacturadorWeb.EntityJWT;
using RSFacWeb.ModelsView.PDPENC;
using RSFacWeb.ModelsView.FEERROR;

namespace RSFacWeb.Models
{
    public class HlDAL : IHlDAL
    {
        private readonly MySQLDatabase _MySqlDatabase;
        private readonly IConfiguration _configuracion;
        private readonly string _baseUrl;

        public HlDAL(MySQLDatabase MySqlDatabase, IConfiguration configuracion)
        {
            _MySqlDatabase = MySqlDatabase;
            _configuracion = configuracion;
            _baseUrl = configuracion["s3-domain"];
        }



        #region Consola Documentos
        public async Task<REIMPRESION_CABECERA_PAGINACION> FacturaCabListar( REIMPRESION_CABECERA_REQUEST REIMPRESION_CABECERA_REQUEST)
        {

            var TablaCab = REIMPRESION_CABECERA_REQUEST.FLAG_FAC.Equals("FACC") ? "FT" + REIMPRESION_CABECERA_REQUEST.CIA + "FACC" : "FT" + REIMPRESION_CABECERA_REQUEST.CIA + "ACUC";
            var FTFORV = "FT" + REIMPRESION_CABECERA_REQUEST.CIA + "FORV";
            var FTVEND = "FT" + REIMPRESION_CABECERA_REQUEST.CIA + "VEND";

            var condicion = "";

            var REIMPRESION_CABECERA_PAGINACION = new REIMPRESION_CABECERA_PAGINACION();
            var LIST_REIMPRESION_CABECERA = new List<REIMPRESION_CABECERA>();

            var count = "COUNT(F5_CCODAGE) AS TOTALES ";

            var select = "F5_CLEYENDA, F5_CESTADO, F5_CCODAGE, F5_CTD, F5_CNUMSER, F5_CNUMDOC, F5_CCODCLI,  F5_CFCONTIG , F5_CDESTIN," +
                         "F5_CNOMBRE, F5_CDIRECC, F5_CCODMON, F5_NIMPORT, DATE_FORMAT(F5_DFECDOC, '%d/%m/%Y') AS F5_DFECDOC, " +
                         "F5_CDOCEST, F5_CSUNEST, DATE_FORMAT(F5_DFECCDR, '%d/%m/%Y') AS F5_DFECCDR,F5_DFECCRE, F5_CPROEST, FV_CCODIGO, FV_CDESCRI, VE_CCODIGO, VE_CNOMBRE, F5_CTIPFAC, F5_CRFNSER, F5_CRFNDOC, F5_CRFTD, F5_CTNC  ";

            string filtroFecha = "F5_DFECDOC >= STR_TO_DATE('" + REIMPRESION_CABECERA_REQUEST.PUFECPRO + "','%d/%m/%Y') AND F5_DFECDOC <= STR_TO_DATE('" + REIMPRESION_CABECERA_REQUEST.PUFECPRO2 + "','%d/%m/%Y') ";

            var from = "FROM " + TablaCab + " LEFT OUTER JOIN " + FTFORV + " ON F5_CFORVEN = FV_CCODIGO " +
                       "LEFT OUTER JOIN " + FTVEND + " ON F5_CVENDE = VE_CCODIGO " +
                       "WHERE F5_CCODAGE = '" + REIMPRESION_CABECERA_REQUEST.PUCODAGE + "' AND " + filtroFecha;

            var orderBy = "ORDER BY F5_CCODAGE,F5_CTD,F5_CNUMSER,F5_CNUMDOC ";

            var skipreg = (REIMPRESION_CABECERA_REQUEST.Total_paginas * REIMPRESION_CABECERA_REQUEST.Pagina_actual) - REIMPRESION_CABECERA_REQUEST.Total_paginas;

            var paginacion = "Limit " + skipreg + "," + REIMPRESION_CABECERA_REQUEST.Total_paginas;

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CTD))
            {
                condicion += "AND F5_CTD = '" + REIMPRESION_CABECERA_REQUEST.CTD + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CNUMSER))
            {
                condicion += "AND F5_CNUMSER LIKE '" + REIMPRESION_CABECERA_REQUEST.CNUMSER + "%' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CNUMDOC))
            {
                condicion += "AND F5_CNUMDOC LIKE '" + REIMPRESION_CABECERA_REQUEST.CNUMDOC + "%' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CESTADO))
            {
                condicion += "AND F5_CESTADO = '" + REIMPRESION_CABECERA_REQUEST.CESTADO + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CESTADODOC))
            {
                condicion += "AND F5_CDOCEST = '" + REIMPRESION_CABECERA_REQUEST.CESTADODOC + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CESTADOSUN))
            {
                condicion += "AND F5_CSUNEST = '" + REIMPRESION_CABECERA_REQUEST.CESTADOSUN + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CESTADOPUB))
            {
                condicion += "AND F5_CPROEST = '" + REIMPRESION_CABECERA_REQUEST.CESTADOPUB + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CCODCLI))
            {
                condicion += "AND F5_CCODCLI LIKE '" + REIMPRESION_CABECERA_REQUEST.CCODCLI + "%' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.CNOMBRE))
            {
                condicion += "AND F5_CNOMBRE LIKE '%" + REIMPRESION_CABECERA_REQUEST.CNOMBRE + "%' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.PUCUSERCONSOLA))
            {
                condicion += "AND F5_CUSUCRE  = '" + REIMPRESION_CABECERA_REQUEST.PUCUSERCONSOLA + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_CABECERA_REQUEST.PUCAJA))
            {
                condicion += "AND F5_CNROCAJ  = '" + REIMPRESION_CABECERA_REQUEST.PUCAJA + "' ";
            }

            var query = select + from + condicion + orderBy + paginacion;

            var totalquery = count + from + condicion;

            try
            {
                var Total_resultados = 0;

                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(REIMPRESION_CABECERA_REQUEST.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT " + query + "; " +
                                           "SELECT " + totalquery + ";";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                skipreg = skipreg + 1;

                                REIMPRESION_CABECERA rEIMPRESION_CABECERA = new REIMPRESION_CABECERA();

                                rEIMPRESION_CABECERA.ROWNUMBER = skipreg;
                                rEIMPRESION_CABECERA.F5_CLEYENDA = rd["F5_CLEYENDA"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CESTADO = rd["F5_CESTADO"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CCODAGE = rd["F5_CCODAGE"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CTD = rd["F5_CTD"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CNUMSER = rd["F5_CNUMSER"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CNUMDOC = rd["F5_CNUMDOC"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CCODCLI = rd["F5_CCODCLI"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CNOMBRE = rd["F5_CNOMBRE"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CCODMON = rd["F5_CCODMON"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_NIMPORT = Convert.ToDecimal(rd["F5_NIMPORT"]);
                                rEIMPRESION_CABECERA.F5_DFECDOC = rd["F5_DFECDOC"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_DFECCRE = rd["F5_DFECCRE"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CDOCEST = rd["F5_CDOCEST"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CPROEST = rd["F5_CPROEST"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CSUNEST = rd["F5_CSUNEST"].ToString().Trim();
                                rEIMPRESION_CABECERA.FV_CCODIGO = rd["FV_CCODIGO"].ToString().Trim();
                                rEIMPRESION_CABECERA.FV_CDESCRI = rd["FV_CDESCRI"].ToString().Trim();
                                rEIMPRESION_CABECERA.VE_CCODIGO = rd["VE_CCODIGO"].ToString().Trim();
                                rEIMPRESION_CABECERA.VE_CNOMBRE = rd["VE_CNOMBRE"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CDIRECC = rd["F5_CDIRECC"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CTIPFAC = rd["F5_CTIPFAC"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CRFNSER = rd["F5_CRFNSER"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CRFNDOC = rd["F5_CRFNDOC"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CRFTD = rd["F5_CRFTD"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_DFECCDR = ConvertHelper.ToNonNullString(rd["F5_DFECCDR"]);
                                rEIMPRESION_CABECERA.F5_CFCONTIG = ConvertHelper.ToNonNullString(rd["F5_CFCONTIG"]);
                                rEIMPRESION_CABECERA.F5_CTNC = rd["F5_CTNC"].ToString().Trim();
                                rEIMPRESION_CABECERA.F5_CDESTIN = ConvertHelper.ToNonNullString(rd["F5_CDESTIN"]);


                                LIST_REIMPRESION_CABECERA.Add(rEIMPRESION_CABECERA);
                            }

                            await rd.NextResultAsync();

                            while (await rd.ReadAsync())
                            {
                                Total_resultados = Convert.ToInt32(rd["TOTALES"]);
                            }

                            rd.Close();
                        }
                    }
                    cn.Close();

                    REIMPRESION_CABECERA_PAGINACION.LIST_REIMPRESION_CABECERA = LIST_REIMPRESION_CABECERA;
                    REIMPRESION_CABECERA_PAGINACION.Total_resultados = Total_resultados;
                    REIMPRESION_CABECERA_PAGINACION.Total_paginas = REIMPRESION_CABECERA_REQUEST.Total_paginas;
                    REIMPRESION_CABECERA_PAGINACION.Pagina_actual = REIMPRESION_CABECERA_REQUEST.Pagina_actual;
                }
            }
            catch (Exception e)
            {
                var st = new StackTrace(e, true);
                //var conexString = _SqlDatabase.GetConnection(PUCBDCONN).ConnectionString.Split(';');
                //await _logDAL.LogCrear(PUCBDCONN, PUCODEMP, "ERROR", REIMPRESION_CABECERA_REQUEST.PUCODAGE, REIMPRESION_CABECERA_REQUEST.CTD, REIMPRESION_CABECERA_REQUEST.CNUMSER, REIMPRESION_CABECERA_REQUEST.CNUMDOC, "", PUCUSER, e.Message + " - " + query, st.GetFrame(0).GetFileLineNumber().ToString(), "", "SW-LOCAL", "FacturaCabListar", "get", "api/v1/faclocal/facturacion/FacturaCabListar", "", conexString[0] + " - " + conexString[2]);
            }
            return REIMPRESION_CABECERA_PAGINACION;
        }

        public async Task<List<FTFACD>> FacturaDetListarSimple(REIMPRESION_DETALLE_REQUEST request)
        {
            var Tabla = request.CTABLA.Equals("FACC") ? "FT" + request.CIA + "FACD" : "FT" + request.CIA + "ACUD";
            var TablaCabecera = request.CTABLA.Equals("FACC") ? "FT" + request.CIA + "FACC" : "FT" + request.CIA + "ACUC";

            var Lista = new List<FTFACD>();

            var Query = "SELECT " +
                         "F6_CITEM,F6_CCODIGO,F6_CDESCRI,F6_CUNIDAD,F6_NCANTID,F6_NPRECIO,F6_NICBPUS, F6_NPORDES, " +
                         "F6_NDESCTO,F6_CTR,F6_NIMPMN,F6_NIMPUS,F6_NIGV,F6_NISC,F6_NICBPMN,F6_CTF, F5_CCODMON " +
                         "FROM " + TablaCabecera + " INNER JOIN " + Tabla + " ON F6_CCODAGE = F5_CCODAGE AND F6_CTD = F5_CTD AND F6_CNUMSER = F5_CNUMSER AND F6_CNUMDOC = F5_CNUMDOC WHERE " +
                         "F5_CCODAGE = '" + request.CODAGE + "' AND " +
                         "F5_CTD = '" + request.CTD + "' AND " +
                         "F5_CNUMSER = '" + request.CNUMSER + "' AND " +
                         "F5_CNUMDOC = '" + request.CNUMDOC + "' ORDER BY F6_CITEM;";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = Query;
                    await cn.OpenAsync();
                    using (var rd = await cmd.ExecuteReaderAsync())
                    {
                        while (await rd.ReadAsync())
                        {
                            Lista.Add(new FTFACD
                            {
                                F6_CITEM = rd["F6_CITEM"].ToString().Trim(),
                                F6_CCODIGO = rd["F6_CCODIGO"].ToString().Trim(),
                                F6_CDESCRI = rd["F6_CDESCRI"].ToString().Trim(),
                                F6_CUNIDAD = rd["F6_CUNIDAD"].ToString().Trim(),
                                F6_NCANTID = Convert.ToDecimal(rd["F6_NCANTID"]),
                                F6_NPRECIO = Convert.ToDecimal(rd["F6_NPRECIO"]),
                                F6_NDESCTO = Convert.ToDecimal(rd["F6_NDESCTO"]),
                                F6_CTR = rd["F6_CTR"].ToString().Trim(),
                                F6_NIMPMN = Convert.ToDecimal(rd["F6_NIMPMN"]),
                                F6_NIGV = Convert.ToDecimal(rd["F6_NIGV"]),
                                F6_NISC = Convert.ToDecimal(rd["F6_NISC"]),
                                F6_NICBPMN = Convert.ToDecimal(rd["F6_NICBPMN"]),
                                F6_CTF = rd["F6_CTF"].ToString().Trim(),
                                F6_NICBPUS = Convert.ToDecimal(rd["F6_NICBPUS"]),
                                F5_CCODMON = ConvertHelper.ToNonNullString(rd["F5_CCODMON"]),
                                F6_NPORDES = Convert.ToDecimal(rd["F6_NPORDES"])
                            });
                        }
                        rd.Close();
                    }
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("FacturaDetListarSimple : " + e.Message);
            }
            return Lista;
        }

        #region Obtener conf. SUPROV
        public async Task<SUPROV> ObtenerConfiguracion(string CCID, string CIA, string TipoDoc, string Proveedor)
        {
            SUPROV configuracion = null;

            string Query = "SELECT PR_CURL,PR_CWSUSER, PR_CWSPASS, PR_CCODPROV, PR_CPROVMOD " +
                           "FROM SUPROV " +
                           "WHERE " +
                          $"PR_CCIA = '{CIA}' AND " +
                          $"PR_CCODPROV = '{Proveedor}' AND " +
                          $"PR_CTD LIKE '%{TipoDoc}%' AND " +
                           "PR_CPROVMOD = 'P' LIMIT 1";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = Query;
                            await cn.OpenAsync();
                            using var rd = await cmd.ExecuteReaderAsync();
                            while (await rd.ReadAsync())
                            {
                                configuracion = new SUPROV()
                                {
                                    PR_CURL = rd["PR_CURL"].ToString(),
                                    PR_CWSUSER = rd["PR_CWSUSER"].ToString(),
                                    PR_CWSPASS = rd["PR_CWSPASS"].ToString(),
                                    PR_CCODPROV = rd["PR_CCODPROV"].ToString(),
                                    PR_CPROVMOD = rd["PR_CPROVMOD"].ToString(),
                                };
                            }

                            await rd.CloseAsync();
                        }
                        await cn.CloseAsync();
                    }
                }
            }  catch (Exception e)
            {
                throw new ArgumentException(e.Message);
            }

            return configuracion;
        }
        #endregion

        #region Consulta estado de email
        public async Task<RsResponse<List<RespuestaEmailStatus>>> ConsultarEstadosEmail(string PUCBDCONN, string PUCODEMP, string UrlCpe, RequestStatusEmail request)
        {
            RsResponse<List<RespuestaEmailStatus>> RespuestaCpe = new RsResponse<List<RespuestaEmailStatus>>();

            try
            {
                string BodyJson = JsonConvert.SerializeObject(request);

                OpenSecurityWeb();

                HttpWebRequest webRequest;
                string requestParams = BodyJson;
                webRequest = (HttpWebRequest)WebRequest.Create($"{UrlCpe}/GetEmailStatus");
                webRequest.Method = "POST";
                webRequest.ContentType = "application/json";
                webRequest.ReadWriteTimeout = 60000;

                byte[] byteArray = Encoding.UTF8.GetBytes(requestParams);
                webRequest.ContentLength = byteArray.Length;

                using (Stream requestStream = webRequest.GetRequestStream())
                {
                    requestStream.Write(byteArray, 0, byteArray.Length);
                }

                using (WebResponse response = webRequest.GetResponse())
                {
                    using (Stream responseStream = response.GetResponseStream())
                    {
                        StreamReader rdr = new StreamReader(responseStream, Encoding.UTF8);

                        var respuesta = rdr.ReadToEnd();

                        JsonSerializerSettings settings = new JsonSerializerSettings();

                        RespuestaCpe = JsonConvert.DeserializeObject<RsResponse<List<RespuestaEmailStatus>>>(respuesta);
                    }
                }
            }
            catch (WebException ex)
            {

                if (ex.Status == WebExceptionStatus.ProtocolError)
                {
                    using Stream responseStream = ex.Response.GetResponseStream();
                    StreamReader rdr = new StreamReader(responseStream, Encoding.UTF8);

                    var respuesta = rdr.ReadToEnd();

                    RsException responseBadRequestCPE = JsonConvert.DeserializeObject<RsException>(respuesta);

                    throw new Exception(responseBadRequestCPE.Exception);
                }


                if (ex.Status == WebExceptionStatus.Timeout)
                {
                    throw new Exception("Error en Tiempo de Espera del Servicio. TimeOut");
                }
                else
                {
                    throw new Exception(ex.Status + " - " + ex.Message);
                }
            }

            return await Task.FromResult(RespuestaCpe);
        }
        #endregion

        #region Listar errores de FE-ERROR
        public async Task<ListaResponseFeerror> GetErrors( DOCUMENTO_REQUEST request)
        {
            try
            {
                ListaResponseFeerror lista = new ListaResponseFeerror();
                lista.Lista = new List<Entities.FEERROR.FEERROR>();

                StringBuilder query = new StringBuilder();
                List<MySqlParameter> parameters = new List<MySqlParameter>();
                query.Append($"SELECT ER_CCODAGE, ER_CTD, ER_CNUMSER, ER_CNUMDOC, ER_NITEM, ER_CCODERR, ER_CDESCRI, ER_DFEC FROM FE{request.CIA}ERROR " +
                    $"WHERE ER_CCODAGE = @ER_CCODAGE AND ER_CTD = @ER_CTD AND ER_CNUMSER = @ER_CNUMSER AND ER_CNUMDOC = @ER_CNUMDOC;" +
                    $"SELECT COUNT(*) FROM FE{request.CIA}ERROR WHERE ER_CCODAGE = @ER_CCODAGE AND ER_CTD = @ER_CTD AND ER_CNUMSER = @ER_CNUMSER AND ER_CNUMDOC = @ER_CNUMDOC;");

                parameters.Add(new MySqlParameter("ER_CCODAGE", request.CODAGE));
                parameters.Add(new MySqlParameter("ER_CTD", request.TIPDOC));
                parameters.Add(new MySqlParameter("ER_CNUMSER", request.NUMSER));
                parameters.Add(new MySqlParameter("ER_CNUMDOC", request.NUMDOC));


                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using var cn = new MySqlConnection(nuevaConexion); 

                    await cn.OpenAsync();

                using var cmd = cn.CreateCommand();

                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query.ToString();
                cmd.Parameters.AddRange(parameters.ToArray());

                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {

                    lista.Lista.Add(new Entities.FEERROR.FEERROR
                    {
                        ER_CCODAGE = ConvertHelper.ToNonNullString(reader["ER_CCODAGE"]),
                        ER_CTD = ConvertHelper.ToNonNullString(reader["ER_CTD"]),
                        ER_CNUMSER = ConvertHelper.ToNonNullString(reader["ER_CNUMSER"]),
                        ER_CNUMDOC = ConvertHelper.ToNonNullString(reader["ER_CNUMDOC"]),
                        ER_NITEM = ConvertHelper.ToNonNullString(reader["ER_NITEM"]),
                        ER_CCODERR = ConvertHelper.ToNonNullString(reader["ER_CCODERR"]),
                        ER_CDESCRI = ConvertHelper.ToNonNullString(reader["ER_CDESCRI"]),
                        ER_DFEC = ConvertHelper.ToNonNullString(reader["ER_DFEC"])
                    });

                }

                if (reader.NextResult())
                {
                    if (reader.Read())
                    {
                        lista.Total = Convert.ToInt32(reader[0]);
                    }
                }

                await cn.CloseAsync();

                return lista;

            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
        #endregion

        public async Task<List<CDRO>> FacturaCDR(REIMPRESION_DETALLE_REQUEST request)
        {
            var Tabla = "FE" + request.CIA + request.CTABLA;

            var Lista = new List<CDRO>();

            var Query = "SELECT " +
                         "CD_CCODERR,CD_CDESCRI,CD_DFEC " +
                         "FROM " + Tabla + " WHERE " +
                         "CD_CCODAGE = '" + request.CODAGE + "' AND " +
                         "CD_CTD = '" + request.CTD + "' AND " +
                         "CD_CNUMSER = '" + request.CNUMSER + "' AND " +
                         "CD_CNUMDOC = '" + request.CNUMDOC + "' ORDER BY CD_NITEM;";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = Query;
                    await cn.OpenAsync();
                    using (var rd = await cmd.ExecuteReaderAsync())
                    {
                        while (await rd.ReadAsync())
                        {
                            Lista.Add(new CDRO
                            {
                                CCODERR = rd["CD_CCODERR"].ToString().Trim(),
                                DFEC = rd.IsDBNull(rd.GetOrdinal("CD_DFEC")) ? string.Empty : Convert.ToDateTime(rd["CD_DFEC"]).ToString("dd/MM/yyyy"),
                                CDESCRI = rd["CD_CDESCRI"].ToString().Trim()
                            });
                        }
                        rd.Close();
                    }
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("FacturaCDR : " + e.Message);
            }
            return Lista;
        }
        public async Task<byte[]> GetZipXML(DOCUMENTO_REQUEST request)
        {
            byte[] ZipXML = null;

            string Tabla = $"FE{request.CIA}XML";
            string TipoXML = request.TIPOXML.Equals("CDR") ? "XM_BCDR" : "XM_BXML";

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $"SELECT {TipoXML} AS XML " +
                                          $"FROM {Tabla} WHERE " +
                                          $"XM_CCODAGE = '{request.CODAGE}' AND " +
                                          $"XM_CTD = '{request.TIPDOC}' AND " +
                                          $"XM_CNUMSER = '{request.NUMSER}' AND " +
                                          $"XM_CNUMDOC = '{request.NUMDOC}';";

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                ZipXML = (byte[])(rd["XML"] == DBNull.Value ? null : rd["XML"]);
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("GetZipXML:" + e.Message);
            }

            return ZipXML;
        }
        public async Task<List<FELOG>> ConsultarLogExcel(DOCUMENTO_REQUEST request)
        {
            List<FELOG> Lista = new List<FELOG>();

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = $"SELECT LG_CTYPE, LG_CEST, LG_CDESCR, LG_CDEBUG, LG_DFEC, LG_CUSU " +
                                      $"FROM FE{request.CIA}LOG WHERE " +
                                      $"LG_CCODAGE = '{request.CODAGE}' AND " +
                                      $"LG_CNUMSER = '{request.NUMSER}' AND " +
                                      $"LG_CNUMDOC = '{request.NUMDOC}' AND " +
                                      $"LG_CTD = '{request.TIPDOC}' " +
                                      $"ORDER BY LG_NLOG ASC;";
                    await cn.OpenAsync();
                    using var rd = await cmd.ExecuteReaderAsync();
                    while (await rd.ReadAsync())
                    {
                        Lista.Add(new FELOG
                        {
                            LG_CTYPE = rd["LG_CTYPE"].ToString(),
                            LG_CEST = rd["LG_CEST"].ToString(),
                            LG_CDESCR = rd["LG_CDESCR"].ToString(),
                            LG_CDEBUG = rd["LG_CDEBUG"].ToString(),
                            LG_DFEC = Convert.ToDateTime(rd["LG_DFEC"]).ToString("dd/MM/yyyy HH:mm:ss"),
                            LG_CUSU = rd["LG_CUSU"].ToString()
                        });
                    }

                    rd.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException(e.Message);
            }
            return Lista;
        }
        #endregion
        #region Consola Guias
        public async Task<LISTAR_GUIAS_RESPONSE> ListarGuias(LISTAR_GUIAS_REQUEST REQUEST)
        {
            var response = new LISTAR_GUIAS_RESPONSE()
            {
                LISTA = new List<CONSOLA_GUIA>(),
                Total_paginas = REQUEST.Total_paginas,
                Pagina_actual = REQUEST.Pagina_actual
            };

            var param0 = new List<MySqlParameter>();

            var sql0 = new StringBuilder();
            var sql1 = new StringBuilder();
            var sql2 = "";
            var tALMOVC = $"AL{REQUEST.CIA}MOVC";
            var tALGREC = $"AL{REQUEST.CIA}GREC";
            var tFTFORV = $"FT{REQUEST.CIA}FORV";
            var tFTVEND = $"FT{REQUEST.CIA}VEND";
            var tALTABM = $"AL{REQUEST.CIA}TABM";
            var tTRCFER = $"TR{REQUEST.CIA}CFER";
            var tTICKET = $"FE{REQUEST.CIA}TICKET";
            var skipreg = (REQUEST.Total_paginas * REQUEST.Pagina_actual) - REQUEST.Total_paginas;
            var fecIni = ConvertHelper.ToNullDateTimeWithFormat(REQUEST.FLT_CFECINI, "yyyy-MM-dd");
            var fecFin = ConvertHelper.ToNullDateTimeWithFormat(REQUEST.FLT_CFECFIN, "yyyy-MM-dd");

            try
            {
                sql0.Append("WHERE a.C5_CTD IN ('GS', 'PD')");

                if (!string.IsNullOrEmpty(REQUEST.FLT_CCODAGE))
                {
                    param0.Add(new MySqlParameter("@F5_CCODAGE", REQUEST.FLT_CCODAGE));
                    sql0.Append(" AND a.C5_CCODAGE = @F5_CCODAGE");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CTD))
                {
                    param0.Add(new MySqlParameter("@C5_CTD", REQUEST.FLT_CTD));
                    sql0.Append(" AND a.C5_CTD = @C5_CTD");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CNUMSER))
                {
                    param0.Add(new MySqlParameter("@C5_CNUMSER", REQUEST.FLT_CNUMSER));
                    //sql0.Append(" AND a.C5_CNUMDOC LIKE '' @C5_CNUMSER '%'");
                    sql0.Append(" AND LEFT(a.C5_CNUMDOC, 4) = @C5_CNUMSER");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CNUMDOC))
                {
                    param0.Add(new MySqlParameter("@C5_CNUMDOC", REQUEST.FLT_CNUMDOC));
                    //sql0.Append(" AND a.C5_CNUMDOC LIKE '%' @C5_CNUMDOC '%'");
                    sql0.Append(" AND SUBSTRING(a.C5_CNUMDOC, 5) LIKE CONCAT('%', @C5_CNUMDOC, '%')");

                }

                if (fecIni != null)
                {
                    sql0.Append($" AND a.C5_DFECDOC >= '{fecIni?.ToString("yyyy-MM-dd")} 00:00:00'");
                }

                if (fecFin != null)
                {
                    sql0.Append($" AND a.C5_DFECDOC <= '{fecFin?.ToString("yyyy-MM-dd")} 23:59:59'");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CCODCLI))
                {
                    param0.Add(new MySqlParameter("@C5_CCODCLI", REQUEST.FLT_CCODCLI));
                    sql0.Append(" AND a.C5_CCODCLI LIKE '%' @C5_CCODCLI '%'");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CNOMCLI))
                {
                    param0.Add(new MySqlParameter("@C5_CNOMCLI", REQUEST.FLT_CNOMCLI));
                    sql0.Append(" AND a.C5_CNOMCLI LIKE '%' @C5_CNOMCLI '%'");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CSITUA))
                {
                    param0.Add(new MySqlParameter("@C5_CSITUA", REQUEST.FLT_CSITUA));
                    sql0.Append(" AND a.C5_CSITUA = @C5_CSITUA");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CUSUCRE))
                {
                    param0.Add(new MySqlParameter("@C5_CUSUCRE", REQUEST.FLT_CUSUCRE));
                    sql0.Append(" AND a.C5_CUSUCRE = @C5_CUSUCRE");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CDOCEST))
                {
                    param0.Add(new MySqlParameter("@C5_CDOCEST", REQUEST.FLT_CDOCEST));
                    sql0.Append(" AND a.C5_CDOCEST = @C5_CDOCEST");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CSUNEST))
                {
                    param0.Add(new MySqlParameter("@C5_CSUNEST", REQUEST.FLT_CSUNEST));
                    sql0.Append(" AND a.C5_CSUNEST = @C5_CSUNEST");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CPROEST))
                {
                    param0.Add(new MySqlParameter("@C5_CPROEST", REQUEST.FLT_CPROEST));
                    sql0.Append(" AND a.C5_CPROEST = @C5_CPROEST");
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CALMA))
                {
                    sql0.Append(" AND a.C5_CALMA = @C5_CALMA");
                    param0.Add(new MySqlParameter("@C5_CALMA", REQUEST.FLT_CALMA));
                }

                if (!string.IsNullOrEmpty(REQUEST.FLT_CTIPGUI))
                {
                    param0.Add(new MySqlParameter("@C5_CTIPGUI", REQUEST.FLT_CTIPGUI));
                    sql0.Append(" AND a.C5_CTIPGUI = @C5_CTIPGUI");
                }

                //sql1.Append(" CREATE TEMPORARY TABLE TMP_ALMOVC")
                //    .Append(" SELECT a.C5_CALMA, a.C5_CCODAGE, a.C5_CTD, a.C5_CNUMDOC, a.C5_DFECDOC, a.C5_CCODCLI, a.C5_CCODFER, a.C5_CDOCIDE, ")
                //    .Append(" a.C5_CCODMON, a.C5_NIMPORT, a.C5_CDOCEST, a.C5_CSUNEST, a.C5_CNOMCLI,a.C5_CCODPRO,a.C5_CNOMPRO, a.C5_CSITUA, a.C5_CFITRANS, a.C5_CFIRS255, a.C5_CFIVEHVAC, a.C5_CFIENVVAC, a.C5_CFIVM1L,  ")
                //    .Append(" a.C5_CTIPGUI, a.C5_CFORVEN, a.C5_CVENDE, a.C5_CDIRECC, a.C5_CPROEST, a.C5_DFECCDR, a.C5_CRFNDOC, a.C5_CCODTRA, a.C5_CNOMTRA, a.C5_CDESTIN, a.C5_CDIRENV, a.C5_CGLOSA3 ")
                //    .Append($" FROM {tALMOVC} a")
                //    .Append($" {sql0}")
                //    .Append($" LIMIT {skipreg},{REQUEST.Total_paginas};")
                //    .Append(" SELECT a.C5_CALMA, a.C5_CCODAGE, a.C5_CTD, a.C5_CNUMDOC, a.C5_DFECDOC, a.C5_CCODCLI, a.C5_CCODFER, a.C5_CDOCIDE, a.C5_CFITRANS, a.C5_CFIRS255, a.C5_CFIVEHVAC, a.C5_CFIENVVAC, a.C5_CFIVM1L, ")
                //    .Append(" a.C5_CCODMON, a.C5_NIMPORT, a.C5_CDOCEST, a.C5_CSUNEST, a.C5_CNOMCLI,a.C5_CCODPRO,a.C5_CNOMPRO, a.C5_CSITUA, a.C5_CPROEST, a.C5_DFECCDR, a.C5_CRFNDOC, a.C5_CCODTRA, a.C5_CNOMTRA, a.C5_CDESTIN, a.C5_CDIRENV, a.C5_CGLOSA3, ")
                //    .Append(" a.C5_CTIPGUI, a.C5_CFORVEN, a.C5_CVENDE, a.C5_CDIRECC,")
                //    .Append(" f.FV_CDESCRI, v.VE_CNOMBRE, t.TM_CDESCRI, c.CR_CLICCON, c.CR_CNOMFER, c.CR_CNUMEID")
                //    .Append(" FROM TMP_ALMOVC a")
                //    .Append($" LEFT JOIN {tFTFORV} f ON f.FV_CCODIGO = a.C5_CFORVEN")
                //    .Append($" LEFT JOIN {tFTVEND} v ON v.VE_CCODIGO = a.C5_CVENDE ")
                //    .Append($" LEFT JOIN {tALTABM} t ON t.TM_CCODMOV = a.C5_CTIPGUI AND t.TM_CTIPMOV = 'S'")
                //    .Append($" LEFT JOIN {tTRCFER} c ON c.CR_CCODFER = a.C5_CCODFER;")
                //    .Append(" DROP TABLE TMP_ALMOVC;")
                //    ;

                //sql2 += $"SELECT COUNT(*) TOTALES FROM {tALMOVC} a {sql0};";
                sql1.Append(" CREATE TEMPORARY TABLE TMP_ALMOVC")
                    .Append(" SELECT a.C5_CALMA, a.C5_CCODAGE, a.C5_CTD, a.C5_CNUMDOC, a.C5_DFECDOC, a.C5_CCODCLI, a.C5_CCODFER, a.C5_CDOCIDE, ")
                    .Append(" a.C5_CCODMON, a.C5_NIMPORT, a.C5_CDOCEST, a.C5_CSUNEST, a.C5_CNOMCLI, a.C5_CCODPRO, a.C5_CNOMPRO, a.C5_CSITUA, a.C5_CFITRANS, a.C5_CFIRS255, a.C5_CFIVEHVAC, a.C5_CFIENVVAC, a.C5_CFIVM1L, ")
                    .Append(" a.C5_CTIPGUI, a.C5_CTIPMOV, a.C5_CFORVEN, a.C5_CVENDE, a.C5_CDIRECC, a.C5_CPROEST, a.C5_DFECCDR, a.C5_CRFNDOC, a.C5_CCODTRA, a.C5_CNOMTRA, a.C5_CDESTIN, a.C5_CDIRENV, a.C5_CGLOSA3 ")
                    .Append(" FROM ( ")
                    .Append($"     SELECT * FROM {tALMOVC} ")
                    .Append($"     UNION ALL ")
                    .Append($"     SELECT * FROM {tALGREC} ")
                    .Append(" ) a ")
                    .Append($" {sql0}")
                    .Append($" LIMIT {skipreg}, {REQUEST.Total_paginas};")

                    .Append(" SELECT a.C5_CALMA, a.C5_CCODAGE, a.C5_CTD, a.C5_CNUMDOC, a.C5_DFECDOC, a.C5_CCODCLI, a.C5_CCODFER, a.C5_CDOCIDE, a.C5_CFITRANS, a.C5_CFIRS255, a.C5_CFIVEHVAC, a.C5_CFIENVVAC, a.C5_CFIVM1L, ")
                    .Append(" a.C5_CCODMON, a.C5_NIMPORT, a.C5_CDOCEST, a.C5_CSUNEST, a.C5_CNOMCLI, a.C5_CCODPRO, a.C5_CNOMPRO, a.C5_CSITUA, a.C5_CPROEST, a.C5_DFECCDR, a.C5_CRFNDOC, a.C5_CCODTRA, a.C5_CNOMTRA, a.C5_CDESTIN, a.C5_CDIRENV, a.C5_CGLOSA3, ")
                    .Append(" a.C5_CTIPGUI, a.C5_CTIPMOV, a.C5_CFORVEN, a.C5_CVENDE, a.C5_CDIRECC, ")
                    .Append(" f.FV_CDESCRI, v.VE_CNOMBRE, t.TM_CDESCRI, c.CR_CLICCON, c.CR_CNOMFER, c.CR_CNUMEID ")
                    .Append(" FROM TMP_ALMOVC a ")
                    .Append($" LEFT JOIN {tFTFORV} f ON f.FV_CCODIGO = a.C5_CFORVEN ")
                    .Append($" LEFT JOIN {tFTVEND} v ON v.VE_CCODIGO = a.C5_CVENDE ")
                    .Append($" LEFT JOIN {tALTABM} t ON t.TM_CCODMOV = a.C5_CTIPGUI AND t.TM_CTIPMOV = a.C5_CTIPMOV ")
                    .Append($" LEFT JOIN {tTRCFER} c ON c.CR_CCODFER = a.C5_CCODFER;")
                    .Append(" DROP TABLE TMP_ALMOVC;");

                sql2 += $"SELECT COUNT(*) TOTALES FROM (SELECT * FROM {tALMOVC} UNION ALL SELECT * FROM {tALGREC}) a {sql0};";


                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(REQUEST.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                        using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sql1 + sql2;
                        cmd.Parameters.AddRange(param0.ToArray());

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                skipreg++;

                                var numDoc = ConvertHelper.ToNonNullString(rd["C5_CNUMDOC"]);

                                response.LISTA.Add(new CONSOLA_GUIA()
                                {
                                    ROWNUMBER = skipreg,
                                    C5_CCODAGE = ConvertHelper.ToNonNullString(rd["C5_CCODAGE"]),
                                    C5_CTD = ConvertHelper.ToNonNullString(rd["C5_CTD"]),
                                    C5_CNUMSER = numDoc.Length >= 4 ? numDoc.Substring(0, 4) : string.Empty,
                                    C5_CNUMDOC = numDoc.Length >= 4 ? numDoc.Substring(4) : string.Empty,
                                    C5_DFECDOC = ConvertHelper.ToNullDateTime(rd["C5_DFECDOC"])?.ToString("dd/MM/yyyy") ?? string.Empty,
                                    C5_CCODCLI = ConvertHelper.ToNonNullString(rd["C5_CCODCLI"]),
                                    C5_CCODPRO = ConvertHelper.ToNonNullString(rd["C5_CCODPRO"]),
                                    C5_CNOMPRO = ConvertHelper.ToNonNullString(rd["C5_CNOMPRO"]),
                                    C5_CCODMON = ConvertHelper.ToNonNullString(rd["C5_CCODMON"]),
                                    C5_NIMPORT = ConvertHelper.ToDecimal(rd["C5_NIMPORT"]),
                                    C5_CDOCEST = ConvertHelper.ToNonNullString(rd["C5_CDOCEST"]),
                                    C5_CSUNEST = ConvertHelper.ToNonNullString(rd["C5_CSUNEST"]),
                                    C5_CNOMCLI = ConvertHelper.ToNonNullString(rd["C5_CNOMCLI"]),
                                    C5_CSITUA = ConvertHelper.ToNonNullString(rd["C5_CSITUA"]),
                                    C5_CALMA = ConvertHelper.ToNonNullString(rd["C5_CALMA"]),
                                    C5_CTIPGUI = ConvertHelper.ToNonNullString(rd["C5_CTIPGUI"]),
                                    C5_CFORVEN = ConvertHelper.ToNonNullString(rd["C5_CFORVEN"]),
                                    C5_CVENDE = ConvertHelper.ToNonNullString(rd["C5_CVENDE"]),
                                    FV_CDESCRI = ConvertHelper.ToNonNullString(rd["FV_CDESCRI"]),
                                    VE_CNOMBRE = ConvertHelper.ToNonNullString(rd["VE_CNOMBRE"]),
                                    C5_CDIRECC = ConvertHelper.ToNonNullString(rd["C5_CDIRECC"]),
                                    C5_DFECCDR = ConvertHelper.ToNullDateTime(rd["C5_DFECCDR"])?.ToString("dd/MM/yyyy") ?? string.Empty,
                                    C5_CPROEST = ConvertHelper.ToNonNullString(rd["C5_CPROEST"]),
                                    C5_CRFNDOC = ConvertHelper.ToNonNullString(rd["C5_CRFNDOC"]),
                                    C5_CCODTRA = ConvertHelper.ToNonNullString(rd["C5_CCODTRA"]),
                                    C5_CNOMTRA = ConvertHelper.ToNonNullString(rd["C5_CNOMTRA"]),
                                    C5_CDESTIN = ConvertHelper.ToNonNullString(rd["C5_CDESTIN"]),
                                    C5_CDIRENV = ConvertHelper.ToNonNullString(rd["C5_CDIRENV"]),
                                    C5_CGLOSA3 = ConvertHelper.ToNonNullString(rd["C5_CGLOSA3"]),
                                    TM_CDESCRI = ConvertHelper.ToNonNullString(rd["TM_CDESCRI"]),
                                    C5_CDOCIDE = ConvertHelper.ToNonNullString(rd["C5_CDOCIDE"]),
                                    CR_CLICCON = ConvertHelper.ToNonNullString(rd["CR_CLICCON"]),
                                    CR_CNOMFER = ConvertHelper.ToNonNullString(rd["CR_CNOMFER"]),
                                    CR_CNUMEID = ConvertHelper.ToNonNullString(rd["CR_CNUMEID"]),
                                    C5_CFITRANS = ConvertHelper.ToNonNullString(rd["C5_CFITRANS"]),
                                    C5_CFIRS255 = ConvertHelper.ToNonNullString(rd["C5_CFIRS255"]),
                                    C5_CFIVEHVAC = ConvertHelper.ToNonNullString(rd["C5_CFIVEHVAC"]),
                                    C5_CFIENVVAC = ConvertHelper.ToNonNullString(rd["C5_CFIENVVAC"]),
                                    C5_CFIVM1L = ConvertHelper.ToNonNullString(rd["C5_CFIVM1L"]),
                                    C5_CTIPMOV = ConvertHelper.ToNonNullString(rd["C5_CTIPMOV"]),
                                });
                            }

                            await rd.NextResultAsync();

                            while (await rd.ReadAsync())
                            {
                                response.Total_resultados = Convert.ToInt32(rd["TOTALES"]);
                            }

                            await rd.CloseAsync();
                        }
                    }

                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                var st = new StackTrace(e, true);
                //var conexString = _SqlDatabase.GetConnection(PUCBDCONN).ConnectionString.Split(';');
                //await _logDAL.LogCrear(PUCBDCONN, PUCODEMP, "ERROR", REQUEST.FLT_CCODAGE, "GS", REQUEST.FLT_CNUMSER, REQUEST.FLT_CNUMDOC, "", PUCUSER, e.Message + " - " + sql1, st.GetFrame(0).GetFileLineNumber().ToString(), "", "SW-LOCAL", "ListarGuias", "get", "api/v1/faclocal/Guia/ListarGuias", "", conexString[0] + " - " + conexString[2]);
            }

            return response;
        }
        public async Task<List<CONSOLA_GUIA_DET>> ListarDetallesGuia(CONSOLA_GUIA REQUEST)
        {
            var lista = new List<CONSOLA_GUIA_DET>();
            var param0 = new List<MySqlParameter>();
            var sql0 = new StringBuilder();
            var tALMOVD = $"AL{REQUEST.CIA}MOVD";
            var tALGRED = $"AL{REQUEST.CIA}GRED";
            var tALARTI = $"AL{REQUEST.CIA}ARTI";
            var tALASER = $"AL{REQUEST.CIA}ASER";
            var tALSERI = $"AL{REQUEST.CIA}SERI";

            var sql1 = new StringBuilder(); //Query para filtrar de ASER
            //var param1 = new List<MySqlParameter>(); //Parametros para buscar en ALSER

            try
            {
                param0.Add(new MySqlParameter("C6_CALMA", REQUEST.C5_CALMA));
                param0.Add(new MySqlParameter("C6_CTD", REQUEST.C5_CTD));
                param0.Add(new MySqlParameter("C6_CNUMDOC", REQUEST.C5_CNUMSER + REQUEST.C5_CNUMDOC));

                sql0.Append(" SELECT a.C6_CCODIGO, a.C6_CITEM, a.C6_CDESCRI, a.C6_NCANTID, a.C6_NPRECIO, a.C6_NPORDES, a.C6_NIMPMN, a.C6_NIMPUS, ")
                    .Append(" a.C6_CTR, a.C6_NIGV, a.C6_NISC, a.C6_NDESCTO, a.C6_NICBPMN, a.C6_NICBPUS, t.AR_CTF, t.AR_CUNIDAD ")
                    .Append(" FROM ( ")
                    .Append($"     SELECT * FROM {tALMOVD} ")
                    .Append($"     UNION ALL ")
                    .Append($"     SELECT * FROM {tALGRED} ")
                    .Append(" ) a ")
                    .Append($" LEFT JOIN {tALARTI} t ON t.AR_CCODIGO = a.C6_CCODIGO ")
                    .Append(" WHERE 1 = 1 ")
                    .Append(" AND a.C6_CALMA = @C6_CALMA ")
                    .Append(" AND a.C6_CTD = @C6_CTD ")
                    .Append(" AND a.C6_CNUMDOC = @C6_CNUMDOC ");

                // ALSER
                sql1.Append($" SELECT C6_CITEM, C6_CCODIGO, C6_CSERIE, C6_NCANTID, SR_DFECVEN FROM {tALASER} LEFT JOIN {tALSERI} ON SR_CALMA = C6_CALMA AND SR_CCODIGO = C6_CCODIGO AND SR_CSERIE = C6_CSERIE WHERE 1 = 1 ")
                    .Append(" AND C6_CALMA = @C6_CALMA")
                    .Append(" AND C6_CTD = @C6_CTD")
                    .Append(" AND C6_CNUMDOC = @C6_CNUMDOC");

                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(REQUEST.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sql0.ToString();
                        cmd.Parameters.AddRange(param0.ToArray());

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                lista.Add(new CONSOLA_GUIA_DET()
                                {
                                    C6_CCODIGO = ConvertHelper.ToNonNullString(rd["C6_CCODIGO"]),
                                    C6_CDESCRI = ConvertHelper.ToNonNullString(rd["C6_CDESCRI"]),
                                    C6_NCANTID = ConvertHelper.ToDecimal(rd["C6_NCANTID"]),
                                    C6_NPRECIO = ConvertHelper.ToDecimal(rd["C6_NPRECIO"]),
                                    C6_NPORDES = ConvertHelper.ToDecimal(rd["C6_NPORDES"]),
                                    C6_NIMPMN = ConvertHelper.ToDecimal(rd["C6_NIMPMN"]),
                                    C6_NIMPUS = ConvertHelper.ToDecimal(rd["C6_NIMPUS"]),
                                    AR_CTF = ConvertHelper.ToNonNullString(rd["AR_CTF"]),
                                    AR_CUNIDAD = ConvertHelper.ToNonNullString(rd["AR_CUNIDAD"]),
                                    C6_CTR = ConvertHelper.ToNonNullString(rd["C6_CTR"]),
                                    C6_NIGV = ConvertHelper.ToDecimal(rd["C6_NIGV"]),
                                    C6_NISC = ConvertHelper.ToDecimal(rd["C6_NISC"]),
                                    C6_NDESCTO = ConvertHelper.ToDecimal(rd["C6_NDESCTO"]),
                                    C6_NICBPMN = ConvertHelper.ToDecimal(rd["C6_NICBPMN"]),
                                    C6_NICBPUS = ConvertHelper.ToDecimal(rd["C6_NICBPUS"]),
                                    C6_CITEM = ConvertHelper.ToNonNullString(rd["C6_CITEM"]),
                                });
                            }

                            await rd.CloseAsync();
                        }
                    }


                    //Consultamos si existe en ALSER
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sql1.ToString();
                        cmd.Parameters.AddRange(param0.ToArray());

                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                var item = ConvertHelper.ToNonNullString(rd["C6_CITEM"]);
                                var serieLote = new FT_SERIE_LOTE()
                                {
                                    FS_CITEMFACT = item,
                                    FS_CCODIGO = ConvertHelper.ToNonNullString(rd["C6_CCODIGO"]),
                                    FS_CSERIE = ConvertHelper.ToNonNullString(rd["C6_CSERIE"]),
                                    FS_NCANTLOTE = ConvertHelper.ToDecimal(rd["C6_NCANTID"]),
                                    FS_DFECVEN = rd["SR_DFECVEN"] != DBNull.Value ? (DateTime?)rd["SR_DFECVEN"] : null
                                };

                                var detalle = lista.FirstOrDefault(d => d.C6_CITEM == item);
                                if (detalle != null)
                                {
                                    detalle.SERIE_LOTE ??= new List<FT_SERIE_LOTE>();
                                    detalle.SERIE_LOTE.Add(serieLote);
                                }

                            }

                            await rd.CloseAsync();
                        }
                    }


                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                var st = new StackTrace(e, true);
                //var conexString = _SqlDatabase.GetConnection(PUCBDCONN).ConnectionString.Split(';');
                //await _logDAL.LogCrear(PUCBDCONN, PUCODEMP, "ERROR", REQUEST.C5_CCODAGE, REQUEST.C5_CTD, REQUEST.C5_CNUMSER, REQUEST.C5_CNUMDOC, "", PUCUSER, e.Message + " - " + sql0.ToString(), st.GetFrame(0).GetFileLineNumber().ToString(), "", "SW-LOCAL", "ListarGuias", "get", "api/v1/faclocal/Guia/ListarGuias", "", conexString[0] + " - " + conexString[2]);
            }

            return lista;
        }
        public async Task<List<ALALMA_PUBLICAS>> GetAlmacen(string Ruc, string Cia)
        {
            var ALALMA_PUBLICAS = new List<ALALMA_PUBLICAS>();
            var Tabla = "AL" + Cia + "ALMA";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    {
                        using (var cmd = cn.CreateCommand())
                        {
                            cmd.CommandType = CommandType.Text;
                            cmd.CommandText = @"SELECT A1_CALMA, A1_CDESCRI " +
                                               "FROM " + Tabla + " WHERE A1_CFWEB='S' ORDER BY A1_CALMA ASC; ";
                            await cn.OpenAsync();
                            using (var rd = await cmd.ExecuteReaderAsync())
                            {
                                while (await rd.ReadAsync())
                                {
                                    ALALMA_PUBLICAS.Add(new ALALMA_PUBLICAS()
                                    {
                                        PUALMA = rd["A1_CALMA"].ToString().Trim(),
                                        PUALMAD = rd["A1_CDESCRI"].ToString().Trim()
                                    });
                                }
                                rd.Close();
                            }
                        }
                        cn.Close();
                    }
                }
            }
            catch (Exception e)
            {
                var st = new StackTrace(e, true);
                //var conexString = _SqlDatabase.GetConnection(PUCBDCONN).ConnectionString.Split(';');
                //await _logDAL.LogCrear(PUCBDCONN, PUCODEMP, "ERROR", "", "", "", "", "", "", e.Message + " - " + Tabla, st.GetFrame(0).GetFileLineNumber().ToString(), "", "SW-LOCAL", "GetAlmacen", "get", "api/v1/faclocal/cia/GetAlmacen", "", conexString[0] + " - " + conexString[1]);
                throw e;
            }
            return ALALMA_PUBLICAS;
        }
        public async Task<List<ALTABM>> GuiaListaMovimiento(string Ruc, string Cia)
        {
            var ALTABM = new List<ALTABM>();
            var Tabla1 = "AL" + Cia + "TABM";
            var Tabla2 = "FE" + Cia + "SUCAT";

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT " +
                                           "A.TM_CTIPMOV,A.TM_CCODMOV,A.TM_CDESCRI,A.TM_CTDSUNT,A.TM_CTMSUNT,B.SU_CDESCRI, " +
                                           "A.TM_CFSTOCK,A.TM_CFMOVINT,A.TM_CFDOC,A.TM_CFALMA,A.TM_CVANEXO, A.TM_CFSOLI " +
                                          $"FROM {Tabla1} A INNER JOIN {Tabla2} B " +
                                           "ON A.TM_CTMSUNT = B.SU_CCLAVE " +
                                           "WHERE " +
                                           "B.SU_CCOD='20' AND " +
                                           "A.TM_CFWEB = 'S' AND " +
                                           //"A.TM_CTIPMOV='S' AND NOT (LEFT(A.TM_CCODMOV,1) BETWEEN 'A' AND 'Z') " +
                                           "NOT (LEFT(A.TM_CCODMOV,1) BETWEEN 'A' AND 'Z') " +
                                           "ORDER BY A.TM_CCODMOV ASC";

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                ALTABM.Add(new ALTABM()
                                {
                                    TM_CTIPMOV = rd["TM_CTIPMOV"].ToString().Trim(),
                                    TM_CCODMOV = rd["TM_CCODMOV"].ToString().Trim(),
                                    TM_CDESCRI = rd["TM_CDESCRI"].ToString().Trim(),
                                    TM_CTDSUNT = rd["TM_CTDSUNT"].ToString().Trim(),
                                    TM_CTMSUNT = rd["TM_CTMSUNT"].ToString().Trim(),
                                    TM_CMOTIVO = rd["SU_CDESCRI"].ToString().Trim(),
                                    TM_CFSTOCK = rd["TM_CFSTOCK"].ToString().Trim(),
                                    TM_CFMOVINT = rd["TM_CFMOVINT"].ToString().Trim(),
                                    TM_CFDOC = rd["TM_CFDOC"].ToString().Trim(),
                                    TM_CFALMA = rd["TM_CFALMA"].ToString().Trim(),
                                    TM_CVANEXO = rd["TM_CVANEXO"].ToString().Trim(),
                                    TM_CFSOLI = rd["TM_CFSOLI"].ToString().Trim()
                                });
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                var st = new StackTrace(e, true);
                //var conexString = _SqlDatabase.GetConnection(PUCBDCONN).ConnectionString.Split(';');
                //await _logDAL.LogCrear(PUCBDCONN, PUCODEMP, "ERROR", "", "", "", "", "", "", e.Message + " - " + Tabla1, st.GetFrame(0).GetFileLineNumber().ToString(), "", "SW-LOCAL", "GuiaListaMovimiento", "get", "api/v1/faclocal/guia", "", conexString[0] + " - " + conexString[1]);
                throw e;
            }
            return ALTABM;
        }
        #region Log Guia
        public async Task<List<FELOG>> ConsultarLogExcelGuia(DOCUMENTO_REQUEST request)
        {
            List<FELOG> Lista = new List<FELOG>();

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    //cmd.CommandText = $"SELECT LG_CTYPE, LG_CEST, LG_CDESCR, LG_CDEBUG, LG_DFEC, LG_CUSU, LG_CBZRESID " +
                    //                  $"FROM FE{PUCODEMP}LOG WHERE " +
                    //                  $"LG_CTD = '{request.TIPDOC}' AND " +
                    //                  $"LG_CCODAGE = '{request.CODAGE}' AND " +
                    //                  $"LG_CNUMSER = '{request.NUMSER}' AND " +
                    //                  $"LG_CNUMDOC = '{request.NUMDOC}' " +
                    //                  $"ORDER BY LG_NLOG ASC;";

                    cmd.CommandText = $@"
                                SELECT 
                                    L.LG_CCODAGE, L.LG_CTYPE, L.LG_CTD, L.LG_CNUMSER, L.LG_CNUMDOC, L.LG_NLOG,
                                    L.LG_CBZRESID, L.LG_CEST, L.LG_CPRMOD, L.LG_CERROR, L.LG_CIDCRON, 
                                    L.LG_CDESCR, L.LG_CDEBUG, L.LG_DFEC, L.LG_DFECFIN, L.LG_CUSU, 
                                    L.LG_CPROGR, L.LG_CIP, L.LG_CPSE, L.LG_CVERSION,
                                    A.AG_CDESCRI
                                FROM FE{request.CIA}LOG L
                                LEFT JOIN FT{request.CIA}AGEN A ON A.AG_CCODAGE = L.LG_CCODAGE
                                WHERE 
                                    L.LG_CCODAGE = @CODAGE AND
                                    L.LG_CNUMSER = @NUMSER AND
                                    L.LG_CNUMDOC = @NUMDOC AND
                                    L.LG_CTD = @TIPDOC
                                ORDER BY L.LG_NLOG DESC;";

                    // Consulta separada para ALCIAS
                    cmd.Parameters.AddWithValue("@CODAGE", request.CODAGE);
                    cmd.Parameters.AddWithValue("@NUMSER", request.NUMSER);
                    cmd.Parameters.AddWithValue("@NUMDOC", request.NUMDOC);
                    cmd.Parameters.AddWithValue("@TIPDOC", request.TIPDOC);



                    await cn.OpenAsync();
                    using var rd = await cmd.ExecuteReaderAsync();
                    while (await rd.ReadAsync())
                    {
                        //Lista.Add(new FELOG
                        //{
                        //    LG_CTYPE = rd["LG_CTYPE"].ToString(),
                        //    LG_CEST = rd["LG_CEST"].ToString(),
                        //    LG_CDESCR = rd["LG_CDESCR"].ToString(),
                        //    LG_CDEBUG = rd["LG_CDEBUG"].ToString(),
                        //    LG_DFEC = Convert.ToDateTime(rd["LG_DFEC"]).ToString("dd/MM/yyyy HH:mm:ss"),
                        //    LG_CUSU = rd["LG_CUSU"].ToString(),
                        //    LG_CBZRESID = rd["LG_CBZRESID"].ToString()
                        //});
                        Lista.Add(new FELOG
                        {
                            LG_NLOG = ConvertHelper.ToNonNullString(rd["LG_NLOG"]),
                            LG_CCODAGE = ConvertHelper.ToNonNullString(rd["LG_CCODAGE"]) + " " + ConvertHelper.ToNonNullString(rd["AG_CDESCRI"]),
                            LG_CTYPE = ConvertHelper.ToNonNullString(rd["LG_CTYPE"]),
                            LG_CTD = ConvertHelper.ToNonNullString(rd["LG_CTD"]),
                            LG_CNUMSER = ConvertHelper.ToNonNullString(rd["LG_CNUMSER"]),
                            LG_CNUMDOC = ConvertHelper.ToNonNullString(rd["LG_CNUMDOC"]),
                            LG_CBZRESID = ConvertHelper.ToNonNullString(rd["LG_CBZRESID"]),
                            LG_CEST = ConvertHelper.ToNonNullString(rd["LG_CEST"]),
                            LG_CPRMOD = ConvertHelper.ToNonNullString(rd["LG_CPRMOD"]),
                            LG_CERROR = ConvertHelper.ToNonNullString(rd["LG_CERROR"]),
                            LG_CIDCRON = ConvertHelper.ToNonNullString(rd["LG_CIDCRON"]),
                            LG_CDESCR = ConvertHelper.ToNonNullString(rd["LG_CDESCR"]),
                            LG_CDEBUG = ConvertHelper.ToNonNullString(rd["LG_CDEBUG"]),
                            LG_DFEC = rd["LG_DFEC"] != DBNull.Value ? Convert.ToDateTime(rd["LG_DFEC"]).ToString("dd/MM/yyyy HH:mm:ss") : null,
                            LG_DFECFIN = rd["LG_DFECFIN"] != DBNull.Value ? Convert.ToDateTime(rd["LG_DFECFIN"]).ToString("dd/MM/yyyy HH:mm:ss") : null,
                            LG_CUSU = ConvertHelper.ToNonNullString(rd["LG_CUSU"]),
                            LG_CPROGR = ConvertHelper.ToNonNullString(rd["LG_CPROGR"]),
                            LG_CIP = ConvertHelper.ToNonNullString(rd["LG_CIP"]),
                            LG_CPSE = ConvertHelper.ToNonNullString(rd["LG_CPSE"]),
                            LG_CVERSION = ConvertHelper.ToNonNullString(rd["LG_CVERSION"])
                        });
                    }

                    rd.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException(e.Message);
            }
            return Lista;
        }
        #endregion

        public async Task<List<FETICKET>> ObtenerTickets(string Cia, string Ruc, string Tipo, string Serie, string Numero, string Almacen)
        {
            string Tabla = "FE" + Cia + "TICKET";

            List<FETICKET> Lista = new List<FETICKET>();

            string Query = @"SELECT TK_CTICKET, TK_CSUNEST, TK_DSUNFEC, TK_CDOCEST, TK_DFECCRE FROM " + Tabla + " WHERE " +
                            "TK_CALMA = @Almacen AND " +
                            "TK_CTD = @Tipo AND " +
                            "TK_CNUMSER = @Serie AND " +
                            "TK_CNUMDOC = @Numero ORDER BY TK_DFECCRE ASC";
            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = Query;
                    cmd.Parameters.Add("@Almacen", MySqlDbType.VarChar).Value = Almacen;
                    cmd.Parameters.Add("@Tipo", MySqlDbType.VarChar).Value = Tipo;
                    cmd.Parameters.Add("@Serie", MySqlDbType.VarChar).Value = Serie;
                    cmd.Parameters.Add("@Numero", MySqlDbType.VarChar).Value = Numero;

                    await cn.OpenAsync();
                    using var rd = await cmd.ExecuteReaderAsync();
                    while (await rd.ReadAsync())
                    {
                        Lista.Add(new FETICKET
                        {
                            Ticket = rd["TK_CTICKET"].ToString(),
                            Estado = rd["TK_CSUNEST"].ToString(),
                            Codigo = rd["TK_CDOCEST"].ToString(),
                            Fecha = Convert.ToDateTime(rd["TK_DSUNFEC"]),
                            FechaCreacion = rd["TK_DFECCRE"].ToString(),
                        });
                    }
                    await rd.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("ObtenerTickets: " + e.Message);
            }

            return Lista;
        }
        #endregion
        #region Consola Resumenes
        public async Task<REIMPRESION_RESUMEN_REQUEST> GetListaResumenes(REIMPRESION_RESUMEN_REQUEST REIMPRESION_RESUMEN_REQUEST)
        {
            List<FERESC> Lista = new List<FERESC>();

            var REIMPRESION_RESUMEN_PAGINACION = new REIMPRESION_RESUMEN_REQUEST();

            string condicion = string.Empty;
            string filtroFecha = string.Empty;

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.PUFECPRO) && !string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.PUFECPRO2))
            {
                filtroFecha = " AND C.RE_DFECRES BETWEEN '" + REIMPRESION_RESUMEN_REQUEST.PUFECPRO + " 00:00:00.000000' AND '" + REIMPRESION_RESUMEN_REQUEST.PUFECPRO2 + " 23:59:59.999999'";
            }

            var skipreg = (REIMPRESION_RESUMEN_REQUEST.Total_paginas * REIMPRESION_RESUMEN_REQUEST.Pagina_actual) - REIMPRESION_RESUMEN_REQUEST.Total_paginas;

            var paginacion = " Limit " + skipreg + "," + REIMPRESION_RESUMEN_REQUEST.Total_paginas;

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CBZRESID))
            {
                condicion += "AND C.RE_CBZRESID LIKE '%" + REIMPRESION_RESUMEN_REQUEST.CBZRESID.Trim() + "%' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CTIPORES))
            {
                condicion += "AND C.RE_CTIPORES LIKE '" + REIMPRESION_RESUMEN_REQUEST.CTIPORES + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CFBAJA))
            {
                condicion += "AND C.RE_CFBAJA LIKE '" + REIMPRESION_RESUMEN_REQUEST.CFBAJA + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CDOCEST))
            {
                condicion += "AND C.RE_CDOCEST LIKE '" + REIMPRESION_RESUMEN_REQUEST.CDOCEST + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CSUNEST))
            {
                condicion += "AND C.RE_CSUNEST LIKE '" + REIMPRESION_RESUMEN_REQUEST.CSUNEST + "' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CTICKET)) // Número de TICKET
            {
                condicion += "AND C.RE_CTICKET LIKE '%" + REIMPRESION_RESUMEN_REQUEST.CTICKET + "%' ";
            }

            string filtroFeresd = string.Empty;

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CTD)) // Tipo de documento
            {
                filtroFeresd += "AND RE_CTD LIKE '%" + REIMPRESION_RESUMEN_REQUEST.CTD + "%' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CNUMSER)) // Serie
            {
                filtroFeresd += "AND RE_CNUMSER LIKE '%" + REIMPRESION_RESUMEN_REQUEST.CNUMSER + "%' ";
            }

            if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CNUMDOC)) // Número de documento
            {
                filtroFeresd += "AND RE_CNUMDOC LIKE '%" + REIMPRESION_RESUMEN_REQUEST.CNUMDOC + "%' ";
            }

            string Query = @" SELECT C.RE_NID, C.RE_CFBAJA, C.RE_CCIA, C.RE_DFECRES, C.RE_CFAPROB, C.RE_CTIPORES, C.RE_CBZRESID, C.RE_CTICKET, C.RE_DFECDOC, C.RE_CDOCEST, C.RE_CSUNEST, C.RE_CPROEST, D.RE_CCODAGE" +
                " FROM FERESC C INNER JOIN ( SELECT D1.* FROM FERESD D1 INNER JOIN ( SELECT RE_NIDC, MIN(RE_CNUMDOC) AS MinNumDoc FROM FERESD " +
                 "WHERE RE_CCIA = '" + REIMPRESION_RESUMEN_REQUEST.CIA + "' " + filtroFeresd + " GROUP BY RE_NIDC) D2 ON D1.RE_NIDC = D2.RE_NIDC AND D1.RE_CNUMDOC = D2.MinNumDoc) D ON C.RE_NID = D.RE_NIDC" +
                " WHERE C.RE_CCIA = '" + REIMPRESION_RESUMEN_REQUEST.CIA + "' AND D.RE_CCODAGE = '" + REIMPRESION_RESUMEN_REQUEST.PUCODAGE + "' ";

            string totalQuery = @" SELECT COUNT(C.RE_NID) AS TOTALES " +
                " FROM FERESC C INNER JOIN ( SELECT D1.* FROM FERESD D1 INNER JOIN ( SELECT RE_NIDC, MIN(RE_CNUMDOC) AS MinNumDoc FROM FERESD " +
                 "WHERE RE_CCIA = '" + REIMPRESION_RESUMEN_REQUEST.CIA + "' " + filtroFeresd + " GROUP BY RE_NIDC) D2 ON D1.RE_NIDC = D2.RE_NIDC AND D1.RE_CNUMDOC = D2.MinNumDoc) D ON C.RE_NID = D.RE_NIDC" +
                " WHERE C.RE_CCIA = '" + REIMPRESION_RESUMEN_REQUEST.CIA + "' AND D.RE_CCODAGE = '" + REIMPRESION_RESUMEN_REQUEST.PUCODAGE + "' ";


            try
            {
                var Total_resultados = 0;

                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(REIMPRESION_RESUMEN_REQUEST.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = Query + filtroFecha + condicion + paginacion + "; " + totalQuery + filtroFecha + condicion + ";";
                    await cn.OpenAsync();
                    using var rd = await cmd.ExecuteReaderAsync();
                    while (await rd.ReadAsync())
                    {
                        skipreg = skipreg + 1;

                        Lista.Add(new FERESC()
                        {
                            ROWNUMBER = skipreg,
                            RE_NID = Convert.ToInt32(rd["RE_NID"].ToString().Trim()),
                            RE_CCIA = rd["RE_CCIA"].ToString().Trim(),
                            RE_CTIPORES = rd["RE_CTIPORES"].ToString().Trim(),
                            RE_CBZRESID = rd["RE_CBZRESID"].ToString().Trim(),
                            RE_CTICKET = rd["RE_CTICKET"].ToString().Trim(),
                            RE_DFECRES = Convert.ToDateTime(rd["RE_DFECRES"].ToString().Trim()).ToString("dd/MM/yyyy"),
                            RE_DFECDOC = Convert.ToDateTime(rd["RE_DFECDOC"].ToString().Trim()).ToString("dd/MM/yyyy"),
                            RE_CDOCEST = rd["RE_CDOCEST"].ToString().Trim(),
                            RE_CSUNEST = rd["RE_CSUNEST"].ToString().Trim(),
                            RE_CPROEST = rd["RE_CPROEST"].ToString().Trim(),
                            RE_CFBAJA = rd["RE_CFBAJA"].ToString().Trim(),
                            RE_CFAPROB = rd["RE_CFAPROB"].ToString().Trim(),
                            PUCODAGE = rd["RE_CCODAGE"].ToString().Trim(),
                        });

                    }

                    await rd.NextResultAsync();

                    while (await rd.ReadAsync())
                    {
                        Total_resultados = Convert.ToInt32(rd["TOTALES"]);
                    }

                    await rd.CloseAsync();
                }

                REIMPRESION_RESUMEN_PAGINACION.ListaFeresc = Lista;
                REIMPRESION_RESUMEN_PAGINACION.Total_resultados = Total_resultados;
                REIMPRESION_RESUMEN_PAGINACION.Total_paginas = REIMPRESION_RESUMEN_REQUEST.Total_paginas;
                REIMPRESION_RESUMEN_PAGINACION.Pagina_actual = REIMPRESION_RESUMEN_REQUEST.Pagina_actual;

            }
            catch (Exception e)
            {
                throw new ArgumentException("GetLista: " + e.Message);
            }

            foreach (var item in Lista)
            {
                item.Detalle = await GetDetalleById(REIMPRESION_RESUMEN_REQUEST.CIA, REIMPRESION_RESUMEN_REQUEST.CCID, item);
            }

            return REIMPRESION_RESUMEN_PAGINACION;

        }
        public async Task<List<FERESD>> GetDetalleById(string CIA, string RUC, FERESC fERESC)
        {
            List<FERESD> Lista = new List<FERESD>();
            string tabla = "FERESD";
            string Query = "SELECT RE_NIDC,RE_CCIA, RE_CCODAGE, RE_CTD, RE_CNUMSER, RE_CNUMDOCD ,RE_CEST FROM " + tabla + " WHERE " +
                " RE_NIDC='" + fERESC.RE_NID + "';";
            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(RUC);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = Query;
                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();
                        while (await rd.ReadAsync())
                        {
                            Lista.Add(new FERESD()
                            {
                                RE_CCIA = rd["RE_CCIA"].ToString().Trim(),
                                RE_CCODAGE = rd["RE_CCODAGE"].ToString().Trim(),
                                RE_CTD = rd["RE_CTD"].ToString().Trim(),
                                RE_CNUMSER = rd["RE_CNUMSER"].ToString().Trim(),
                                RE_CNUMDOCD = rd["RE_CNUMDOCD"].ToString().Trim(),
                                RE_CEST = rd["RE_CEST"].ToString().Trim()
                            });
                        }
                        await rd.CloseAsync();
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("GetLista: " + e.Message);
            }

            return Lista;
        }

        public async Task<string> GetZipXMLResumenes(REIMPRESION_RESUMEN_REQUEST request)
        {
            string Base64XML = null;

            string Tabla = $"FERESC";
            string TipoXML = request.TIPOXML.Equals("CDR") ? "RE_BCDR" : "RE_BXML";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $"SELECT {TipoXML} AS XML " +
                                          $"FROM {Tabla} WHERE " +
                                          $"RE_CBZRESID = '{request.CBZRESID}' AND " +
                                          $"RE_CCIA = '{request.CIA}' AND RE_NID = {request.ID};";

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                Base64XML = rd["XML"] == DBNull.Value ? null : rd["XML"].ToString();
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("GetZipXML: " + e.Message);
            }

            return Base64XML;
        }
        public async Task<List<CDRO>> ResumenCDR(REIMPRESION_RESUMEN_REQUEST request)
        {
            var Tabla = "FE" + request.CIA + "CDRO";

            var Lista = new List<CDRO>();

            var Query = "SELECT " +
                         "CD_CTD, CD_CNUMSER, CD_CNUMDOC, CD_CCODERR, CD_CDESCRI, CD_DFEC  " +
                         "FROM " + Tabla + " WHERE " +
                         "CD_CBZRESID = '" + request.CBZRESID + "' ORDER BY CD_NITEM;";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = Query;
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                Lista.Add(new CDRO
                                {
                                    CCODERR = rd["CD_CCODERR"].ToString().Trim(),
                                    DFEC = rd.IsDBNull(rd.GetOrdinal("CD_DFEC")) ? string.Empty : Convert.ToDateTime(rd["CD_DFEC"]).ToString("dd/MM/yyyy"),
                                    CDESCRI = rd["CD_CDESCRI"].ToString().Trim(),
                                    CTD = rd["CD_CTD"].ToString().Trim(),
                                    CNUMSER = rd["CD_CNUMSER"].ToString().Trim(),
                                    CNUMDOC = rd["CD_CNUMDOC"].ToString().Trim(),
                                });
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("ResumenCDR : " + e.Message);
            }
            return Lista;

        }
        public async Task<List<FELOG>> ConsultarLogExcelResumen(DOCUMENTO_REQUEST request)
        {
            List<FELOG> Lista = new List<FELOG>();

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        //cmd.CommandText = $"SELECT LG_CTYPE, LG_CEST, LG_CDESCR, LG_DFEC, LG_CUSU, LG_CBZRESID " +
                        //                  $"FROM FE{PUCODEMP}LOG WHERE " +
                        //                  $"LG_CBZRESID = '{request.CBZRESID}' AND " +
                        //                  $"LG_CCODAGE = '{request.CODAGE}' " +
                        //                  $"ORDER BY LG_NLOG ASC;";

                        cmd.CommandText = $@"
                                SELECT 
                                    L.LG_CCODAGE, L.LG_CTYPE, L.LG_CTD, L.LG_CNUMSER, L.LG_CNUMDOC, L.LG_NLOG,
                                    L.LG_CBZRESID, L.LG_CEST, L.LG_CPRMOD, L.LG_CERROR, L.LG_CIDCRON, 
                                    L.LG_CDESCR, L.LG_CDEBUG, L.LG_DFEC, L.LG_DFECFIN, L.LG_CUSU, 
                                    L.LG_CPROGR, L.LG_CIP, L.LG_CPSE, L.LG_CVERSION,
                                    A.AG_CDESCRI
                                FROM FE{request.CIA}LOG L
                                LEFT JOIN FT{request.CIA}AGEN A ON A.AG_CCODAGE = L.LG_CCODAGE
                                WHERE 
                                    L.LG_CCODAGE = @CODAGE AND
                                    L.LG_CBZRESID = @CBZRESID
                                ORDER BY L.LG_NLOG DESC;";

                        // Consulta separada para ALCIAS
                        cmd.Parameters.AddWithValue("@CODAGE", request.CODAGE);
                        cmd.Parameters.AddWithValue("@CBZRESID", request.CBZRESID);



                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();
                        while (await rd.ReadAsync())
                        {
                            //Lista.Add(new FELOG
                            //{
                            //    LG_CTYPE = rd["LG_CTYPE"].ToString(),
                            //    LG_CEST = rd["LG_CEST"].ToString(),
                            //    LG_CDESCR = rd["LG_CDESCR"].ToString(),
                            //    LG_DFEC = Convert.ToDateTime(rd["LG_DFEC"]).ToString("dd/MM/yyyy HH:mm:ss"),
                            //    LG_CUSU = rd["LG_CUSU"].ToString(),
                            //    LG_CBZRESID = rd["LG_CBZRESID"].ToString()
                            //});
                            Lista.Add(new FELOG
                            {
                                LG_NLOG = ConvertHelper.ToNonNullString(rd["LG_NLOG"]),
                                LG_CCODAGE = ConvertHelper.ToNonNullString(rd["LG_CCODAGE"]) + " " + ConvertHelper.ToNonNullString(rd["AG_CDESCRI"]),
                                LG_CTYPE = ConvertHelper.ToNonNullString(rd["LG_CTYPE"]),
                                LG_CTD = ConvertHelper.ToNonNullString(rd["LG_CTD"]),
                                LG_CNUMSER = ConvertHelper.ToNonNullString(rd["LG_CNUMSER"]),
                                LG_CNUMDOC = ConvertHelper.ToNonNullString(rd["LG_CNUMDOC"]),
                                LG_CBZRESID = ConvertHelper.ToNonNullString(rd["LG_CBZRESID"]),
                                LG_CEST = ConvertHelper.ToNonNullString(rd["LG_CEST"]),
                                LG_CPRMOD = ConvertHelper.ToNonNullString(rd["LG_CPRMOD"]),
                                LG_CERROR = ConvertHelper.ToNonNullString(rd["LG_CERROR"]),
                                LG_CIDCRON = ConvertHelper.ToNonNullString(rd["LG_CIDCRON"]),
                                LG_CDESCR = ConvertHelper.ToNonNullString(rd["LG_CDESCR"]),
                                LG_CDEBUG = ConvertHelper.ToNonNullString(rd["LG_CDEBUG"]),
                                LG_DFEC = rd["LG_DFEC"] != DBNull.Value ? Convert.ToDateTime(rd["LG_DFEC"]).ToString("dd/MM/yyyy HH:mm:ss") : null,
                                LG_DFECFIN = rd["LG_DFECFIN"] != DBNull.Value ? Convert.ToDateTime(rd["LG_DFECFIN"]).ToString("dd/MM/yyyy HH:mm:ss") : null,
                                LG_CUSU = ConvertHelper.ToNonNullString(rd["LG_CUSU"]),
                                LG_CPROGR = ConvertHelper.ToNonNullString(rd["LG_CPROGR"]),
                                LG_CIP = ConvertHelper.ToNonNullString(rd["LG_CIP"]),
                                LG_CPSE = ConvertHelper.ToNonNullString(rd["LG_CPSE"]),
                                LG_CVERSION = ConvertHelper.ToNonNullString(rd["LG_CVERSION"])
                            });
                        }

                        rd.Close();

                    }
                    cn.Close();
                } 
            }
            catch (Exception e)
            {
                throw new ArgumentException(e.Message);
            }
            return Lista;
        }
        #endregion

        #region Consola de Partes
        public async Task<(List<ALMOVC>, int)> GetListaConsolaCab(RequestParteConsola request)
        {
            List<ALMOVC> Lista = new List<ALMOVC>();
            int Total = 0;

            string Condicion = string.Empty;

            if (!string.IsNullOrEmpty(request.NumDoc))
            {
                Condicion += "C5_CNUMDOC LIKE '%" + request.NumDoc + "%' AND ";
            }

            if (!string.IsNullOrEmpty(request.CodAlmacen))
            {
                Condicion += "C5_CALMA LIKE '%" + request.CodAlmacen + "%' AND ";
            }

            if (!string.IsNullOrEmpty(request.Estado))
            {
                Condicion += "C5_CSITUA LIKE '%" + request.Estado + "%' AND ";
            }

            if (!string.IsNullOrEmpty(request.TipoParte))
            {
                Condicion += "C5_CCODMOV LIKE '%" + request.TipoParte + "%' AND ";
            }

            if (!string.IsNullOrEmpty(request.TipoDoc))
            {
                Condicion += "C5_CTD LIKE '%" + request.TipoDoc + "%' AND ";
            }


            int skipreg = (request.TotalPagina * request.PaginaActual) - request.TotalPagina;

            string paginacion = $"LIMIT {skipreg},{request.TotalPagina}";

            //Query Detallado
            string QueryCab = $"SELECT C5_CALMA,C5_CTD,C5_CCODMOV,C5_CNUMDOC,C5_DFECDOC,C5_CCODCLI," +
                              $"C5_CNOMCLI,C5_CCODPRO,C5_CNOMPRO,C5_CSITUA " +
                              $"FROM AL{request.CIA}MOVC " +
                              $"WHERE C5_CTD IN ('PS','PE') AND {Condicion} " +
                              $"C5_DFECDOC >= STR_TO_DATE('{request.FechaIni}','%Y-%m-%d')  AND " +
                              $"C5_DFECDOC <= STR_TO_DATE('{request.FechaFin}','%Y-%m-%d') {paginacion}";

            //Query Totales
            string QueryTot = $"SELECT COUNT(C5_CNUMDOC) AS TOTAL " +
                              $"FROM AL{request.CIA}MOVC " +
                              $"WHERE C5_CTD IN ('PS','PE') AND {Condicion} " +
                              $"C5_DFECDOC >= STR_TO_DATE('{request.FechaIni}','%Y-%m-%d')  AND " +
                              $"C5_DFECDOC <= STR_TO_DATE('{request.FechaFin}','%Y-%m-%d')";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $"{QueryCab}; {QueryTot};";
                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();
                        while (await rd.ReadAsync())
                        {
                            Lista.Add(new ALMOVC()
                            {
                                C5_CALMA = rd["C5_CALMA"].ToString().Trim(),
                                C5_CTD = rd["C5_CTD"].ToString().Trim(),
                                C5_CCODMOV = rd["C5_CCODMOV"].ToString().Trim(),
                                C5_CNUMDOC = rd["C5_CNUMDOC"].ToString().Trim(),
                                C5_DFECDOC = Convert.ToDateTime(rd["C5_DFECDOC"]).ToString("dd/MM/yyyy"),
                                C5_CCODCLI = rd["C5_CCODCLI"].ToString().Trim(),
                                C5_CNOMCLI = rd["C5_CNOMCLI"].ToString().Trim(),
                                C5_CCODPRO = rd["C5_CCODPRO"].ToString().Trim(),
                                C5_CNOMPRO = rd["C5_CNOMPRO"].ToString().Trim(),
                                C5_CSITUA = rd["C5_CSITUA"].ToString().Trim()

                            });
                        }

                        await rd.NextResultAsync();

                        if (await rd.ReadAsync())
                        {
                            Total = Convert.ToInt32(rd["TOTAL"]);
                        }
                        rd.Close();
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException(e.Message);
            }

            return (Lista, Total);
        }
        public async Task<(ALMOVC, List<ALMOVD>)> GetImpresion(string Cia, string Ruc, ALMOVC ALMOVC)
        {
            ALMOVC Cabecera = new ALMOVC();
            List<ALMOVD> Detalle = new List<ALMOVD>();

            Cabecera = await GetCabeceraImpresion(Cia, Ruc, ALMOVC);

            Detalle = await GetDetalleImpresion(Cia, Ruc, ALMOVC);

            return (Cabecera, Detalle);
        }
        private async Task<ALMOVC> GetCabeceraImpresion(string Cia, string Ruc, ALMOVC Cabecera)
        {
            ALMOVC objecto = new ALMOVC();
            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $"SELECT A.C5_CNUMDOC,A.C5_CCODCLI,A.C5_CNOMCLI,A.C5_CCODPRO,A.C5_CNOMPRO,A.C5_CCODMOV,A.C5_DFECDOC," +
                                          $"A.C5_CALMA,B.A1_CDESCRI AS C5_CDALM,A.C5_CRFTDOC,A.C5_CRFNDOC,A.C5_CNUMORD,A.C5_CGLOSA1 " +
                                          $"FROM AL{Cia}MOVC A INNER JOIN AL{Cia}ALMA B ON A.C5_CALMA = B.A1_CALMA " +
                                          $"WHERE " +
                                          $"A.C5_CTD = '{Cabecera.C5_CTD}' AND " +
                                          $"A.C5_CALMA = '{Cabecera.C5_CALMA}' AND " +
                                          $"A.C5_CNUMDOC = '{Cabecera.C5_CNUMDOC}'";

                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();
                        while (await rd.ReadAsync())
                        {
                            objecto.C5_CNUMDOC = rd["C5_CNUMDOC"].ToString();
                            objecto.C5_CCODCLI = rd["C5_CCODCLI"].ToString();
                            objecto.C5_CNOMCLI = rd["C5_CNOMCLI"].ToString();
                            objecto.C5_CCODPRO = rd["C5_CCODPRO"].ToString();
                            objecto.C5_CNOMPRO = rd["C5_CNOMPRO"].ToString();
                            objecto.C5_DFECDOC = rd["C5_DFECDOC"].ToString();
                            objecto.C5_CALMA = rd["C5_CALMA"].ToString();
                            objecto.C5_CDALM = rd["C5_CDALM"].ToString();
                            objecto.C5_CRFTDOC = rd["C5_CRFTDOC"].ToString();
                            objecto.C5_CRFNDOC = rd["C5_CRFNDOC"].ToString();
                            objecto.C5_CNUMORD = rd["C5_CNUMORD"].ToString();
                            objecto.C5_CCODMOV = rd["C5_CCODMOV"].ToString();
                            objecto.C5_CGLOSA1 = rd["C5_CGLOSA1"].ToString();
                        }
                        rd.Close();
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException(e.Message);
            }
            return objecto;
        }

        private async Task<List<ALMOVD>> GetDetalleImpresion(string Cia, string Ruc, ALMOVC Cabecera)
        {
            var Lista = new List<ALMOVD>();
            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $"SELECT A.C6_CCODIGO, A.C6_CDESCRI, A.C6_NCANTID, A.C6_CITEM, C.AR_CUNIDAD AS C6_CUNIDAD FROM " +
                                          $"AL{Cia}MOVD A  " +
                                          $"INNER JOIN AL{Cia}ARTI C ON A.C6_CCODIGO = C.AR_CCODIGO " +
                                          $"WHERE " +
                                          $"A.C6_CTD = '{Cabecera.C5_CTD}' AND " +
                                          $"A.C6_CALMA = '{Cabecera.C5_CALMA}' AND " +
                                          $"A.C6_CNUMDOC = '{Cabecera.C5_CNUMDOC}'";

                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();
                        while (await rd.ReadAsync())
                        {
                            Lista.Add(new ALMOVD()
                            {
                                C6_CCODIGO = rd["C6_CCODIGO"].ToString(),
                                C6_CDESCRI = rd["C6_CDESCRI"].ToString(),
                                C6_NCANTID = Convert.ToDecimal(rd["C6_NCANTID"]),
                                C6_CUNIDAD = rd["C6_CUNIDAD"].ToString(),
                                C6_CITEM = rd["C6_CITEM"].ToString(),
                                //C6_CSERIE = string.IsNullOrEmpty(rd["C6_CSERIE"].ToString()) ? string.Empty : rd["C6_CSERIE"].ToString()
                            });
                        }
                        rd.Close();
                    }

                    //Consultamos si existe en ALSER
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $" SELECT C6_CITEM, C6_CCODIGO, C6_CSERIE, C6_NCANTID, SR_DFECVEN FROM AL{Cia}ASER LEFT JOIN AL{Cia}SERI ON SR_CALMA = C6_CALMA AND SR_CCODIGO = C6_CCODIGO AND SR_CSERIE = C6_CSERIE WHERE 1 = 1 " +
                                " AND C6_CALMA = @C6_CALMA" +
                                " AND C6_CTD = @C6_CTD" +
                                " AND C6_CNUMDOC = @C6_CNUMDOC";

                        cmd.Parameters.Add("@C6_CALMA", MySqlDbType.VarChar).Value = Cabecera.C5_CALMA;
                        cmd.Parameters.Add("@C6_CTD", MySqlDbType.VarChar).Value = Cabecera.C5_CTD;
                        cmd.Parameters.Add("@C6_CNUMDOC", MySqlDbType.VarChar).Value = Cabecera.C5_CNUMDOC;


                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                var item = ConvertHelper.ToNonNullString(rd["C6_CITEM"]);
                                var serieLote = new FT_SERIE_LOTE()
                                {
                                    FS_CITEMFACT = item,
                                    FS_CCODIGO = ConvertHelper.ToNonNullString(rd["C6_CCODIGO"]),
                                    FS_CSERIE = ConvertHelper.ToNonNullString(rd["C6_CSERIE"]),
                                    FS_NCANTLOTE = ConvertHelper.ToDecimal(rd["C6_NCANTID"]),
                                    FS_DFECVEN = rd["SR_DFECVEN"] != DBNull.Value ? (DateTime?)rd["SR_DFECVEN"] : null
                                };

                                var detalle = Lista.FirstOrDefault(d => d.C6_CITEM == item);
                                if (detalle != null)
                                {
                                    detalle.SERIE_LOTE ??= new List<FT_SERIE_LOTE>();
                                    detalle.SERIE_LOTE.Add(serieLote);
                                }

                            }

                            await rd.CloseAsync();
                        }
                    }

                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException(e.Message);
            }
            return Lista;
        }

        public async Task<List<ALTABM>> ParteListaMovimiento(string Ruc, string Cia)
        {
            var ALTABM = new List<ALTABM>();
            var Tabla1 = "AL" + Cia + "TABM";

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $@"SELECT 
                                                TM_CTIPMOV,
                                                TM_CCODMOV,
                                                TM_CDESCRI,
                                                TM_CTDSUNT,
                                                TM_CTMSUNT,
                                                TM_CFSTOCK,
                                                TM_CFMOVINT,
                                                TM_CFDOC,
                                                TM_CFALMA,
                                                TM_CVANEXO,
                                                TM_CFSOLI
                                            FROM {Tabla1} A
                                            WHERE (A.TM_CTDSUNT IS NULL OR TRIM(A.TM_CTDSUNT) = '') 
                                                AND (A.TM_CTMSUNT IS NULL OR TRIM(A.TM_CTMSUNT) = '')";

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                ALTABM.Add(new ALTABM()
                                {
                                    TM_CTIPMOV = rd["TM_CTIPMOV"].ToString().Trim(),
                                    TM_CCODMOV = rd["TM_CCODMOV"].ToString().Trim(),
                                    TM_CDESCRI = rd["TM_CDESCRI"].ToString().Trim(),
                                    TM_CTDSUNT = rd["TM_CTDSUNT"].ToString().Trim(),
                                    TM_CTMSUNT = rd["TM_CTMSUNT"].ToString().Trim(),
                                    TM_CFSTOCK = rd["TM_CFSTOCK"].ToString().Trim(),
                                    TM_CFMOVINT = rd["TM_CFMOVINT"].ToString().Trim(),
                                    TM_CFDOC = rd["TM_CFDOC"].ToString().Trim(),
                                    TM_CFALMA = rd["TM_CFALMA"].ToString().Trim(),
                                    TM_CVANEXO = rd["TM_CVANEXO"].ToString().Trim(),
                                    TM_CFSOLI = rd["TM_CFSOLI"].ToString().Trim()
                                });
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                var st = new StackTrace(e, true);
                //var conexString = _SqlDatabase.GetConnection(PUCBDCONN).ConnectionString.Split(';');
                //await _logDAL.LogCrear(PUCBDCONN, PUCODEMP, "ERROR", "", "", "", "", "", "", e.Message + " - " + Tabla1, st.GetFrame(0).GetFileLineNumber().ToString(), "", "SW-LOCAL", "ParteListaMovimientomiento", "get", "api/v1/faclocal/almacen", "", conexString[0] + " - " + conexString[1]);
                throw e;
            }
            return ALTABM;

        }
        #endregion

        #region Consola de Pedidos
        public async Task<List<FTVEND_LISTA>> GetListaVendedor(string Ruc, string Cia)
        {
            var FTVEND_LISTA = new List<FTVEND_LISTA>();
            var Tabla = "FT" + Cia + "VEND";

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $"SELECT VE_CCODIGO, VE_CNOMBRE, VE_CCOSUPV  FROM {Tabla} ORDER BY VE_CCODIGO;";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                FTVEND_LISTA.Add(new FTVEND_LISTA()
                                {
                                    PF_CCODIGO = rd["VE_CCODIGO"].ToString().Trim(),
                                    PF_CNOMBRE = rd["VE_CNOMBRE"].ToString().Trim(),
                                    PF_CCOSUPV = rd["VE_CCOSUPV"].ToString().Trim()
                                });
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException($"GetListaVendedor: {e.Message}");
            }

            return FTVEND_LISTA;
        }

        public async Task<List<FTAGEN>> GetListaShort(string Ruc, string Cia, string codUbigeo)
        {
            var parametros = new List<MySqlParameter>();
            var Lista = new List<FTAGEN>();
            string tabla = "FT" + Cia + "AGEN";
            string Query = $"SELECT AG_CCODAGE, AG_CDESCRI FROM {tabla}";

            if (!string.IsNullOrEmpty(codUbigeo))
            {
                parametros.Add(new MySqlParameter("AG_CUBIGEO", codUbigeo));
                Query += " WHERE AG_CUBIGEO = @AG_CUBIGEO";
            }

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = Query;
                        cmd.Parameters.AddRange(parametros.ToArray());
                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();
                        while (await rd.ReadAsync())
                        {
                            Lista.Add(new FTAGEN()
                            {
                                AG_CCODAGE = ConvertHelper.ToNonNullString(rd["AG_CCODAGE"]),
                                AG_CDESCRI = ConvertHelper.ToNonNullString(rd["AG_CDESCRI"]),
                            });
                        }
                        await rd.CloseAsync();
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("GetListaShort: " + e.Message);
            }

            return Lista;
        }
        public async Task<ResponseListaPedido> ListarPedidosConsola(RequestListaPedido request)
        {
            List<MySqlParameter> param0 = new List<MySqlParameter>();

            ResponseListaPedido response = new ResponseListaPedido();

            var sql0 = new StringBuilder();
            var sql1 = new StringBuilder();
            var sql2 = "";
            var sqlReferencia = new StringBuilder();
            var tPDPENC = $"PD{request.CIA}PENC";
            var FTFORV = $"FT{request.CIA}FORV";
            var FTVEND = $"FT{request.CIA}VEND";
            var FTAGEN = $"FT{request.CIA}AGEN";

            if (string.IsNullOrEmpty(request.FechaIni) || string.IsNullOrEmpty(request.FechaFin))
            {
                throw new Exception("Fecha de inicio y fecha final son requeridos.");
            }

            var skipreg = (request.Total_paginas * request.Pagina_actual) - request.Total_paginas;
            var fecIni = ConvertHelper.ToNullDateTimeWithFormat(request.FechaIni, "yyyy-MM-dd");
            var fecFin = ConvertHelper.ToNullDateTimeWithFormat(request.FechaFin, "yyyy-MM-dd");

            try
            {
                sql0.Append("WHERE F5_CTD ='PD'");
                sql0.Append($" AND F5_DFECDOC >= STR_TO_DATE('" + fecIni + "', '%d/%m/%Y')");
                sql0.Append($" AND F5_DFECDOC <= STR_TO_DATE('" + fecFin + "', '%d/%m/%Y')");

                if (!string.IsNullOrEmpty(request.Agencia))
                {
                    sql0.Append(" AND F5_CCODAGE = @Agencia ");
                    param0.Add(new MySqlParameter("Agencia", request.Agencia));
                }

                //if (!string.IsNullOrEmpty(request.Sectorista))
                //{
                //    sql0.Append(" AND F5_CCODAGE = '" + request.Agencia + "'");
                //}

                //if (!string.IsNullOrEmpty(request.Moneda))
                //{
                //    sql0.Append(" AND F5_CCODMON = '" + request.Moneda + "'");
                //}

                if (!string.IsNullOrEmpty(request.Vendedor))
                {
                    sql0.Append(" AND F5_CVENDE = @Vendedor ");
                    param0.Add(new MySqlParameter("Vendedor", request.Vendedor));
                }

                if (!string.IsNullOrEmpty(request.Estado))
                {
                    sql0.Append(" AND F5_CESTADO = @Estado ");
                    param0.Add(new MySqlParameter("Estado", request.Estado));
                }


                if (!string.IsNullOrEmpty(request.Numero))
                {
                    sql0.Append(" AND F5_CNUMPED LIKE @Numero ");
                    param0.Add(new MySqlParameter("Numero", $"%{request.Numero}%"));
                }


                if (!string.IsNullOrEmpty(request.CodCliente))
                {
                    sql0.Append(" AND F5_CCODCLI LIKE @CodCliente");
                    param0.Add(new MySqlParameter("CodCliente", $"%{request.CodCliente}%"));

                }

                if (!string.IsNullOrEmpty(request.RazonSocial))
                {
                    sql0.Append(" AND F5_CNOMBRE LIKE @RazonSocial");
                    param0.Add(new MySqlParameter("RazonSocial", $"%{request.RazonSocial}%"));

                }

                List<Pedido> Lista = new List<Pedido>();

                sqlReferencia.Append($" LEFT OUTER JOIN {FTFORV} ON F5_CFORVEN = FV_CCODIGO "); //Forma de venta
                sqlReferencia.Append($" LEFT OUTER JOIN {FTVEND} ON F5_CVENDE = VE_CCODIGO ");  //Vendedor
                sqlReferencia.Append($" LEFT OUTER JOIN {FTAGEN} ON F5_CCODAGE = AG_CCODAGE ");

                sql1.Append(" SELECT")
                    .Append(" F5_CCODAGE,F5_CNUMPED,F5_CTD,F5_DFECDOC,F5_CCODCLI,F5_CRUC, F5_CNOMBRE, F5_CDIRECC, F5_CESTADO,F5_NIMPORT,F5_CCODMON, F5_CVENDE, F5_CFORVEN, F5_DFECCRE, " +
                    " F5_CNUMSER, F5_CNUMDOC, VE_CNOMBRE, FV_CDESCRI, AG_CDESCRI ")
                    .Append($" FROM {tPDPENC} {sqlReferencia}")
                    .Append($" {sql0} ORDER BY F5_CCODAGE, F5_CNUMPED ASC")
                    .Append($" LIMIT {skipreg},{request.Total_paginas};")
                    ;

                sql2 += $"SELECT COUNT(*) TOTALES FROM {tPDPENC} {sql0};";

                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sql1 + sql2;
                        cmd.Parameters.AddRange(param0.ToArray());

                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();


                        while (await rd.ReadAsync())
                        {
                            var cab = new Pedido
                            {
                                Agencia = ConvertHelper.ToNonNullString(rd["F5_CCODAGE"]),
                                Numero = ConvertHelper.ToNonNullString(rd["F5_CNUMPED"]),
                                Tipo = ConvertHelper.ToNonNullString(rd["F5_CTD"]),
                                Fecha = Convert.ToDateTime(rd["F5_DFECDOC"]).ToString("dd/MM/yyyy"),
                                FechaCreacion = Convert.ToDateTime(rd["F5_DFECCRE"]).ToString(),
                                CodCliente = ConvertHelper.ToNonNullString(rd["F5_CRUC"]),
                                NomCliente = ConvertHelper.ToNonNullString(rd["F5_CNOMBRE"]),
                                Direccion = ConvertHelper.ToNonNullString(rd["F5_CDIRECC"]),
                                Estado = ConvertHelper.ToNonNullString(rd["F5_CESTADO"]),
                                Importe = Convert.ToDecimal(rd["F5_NIMPORT"]),
                                Moneda = ConvertHelper.ToNonNullString(rd["F5_CCODMON"]),
                                Vendedor = ConvertHelper.ToNonNullString(rd["F5_CVENDE"]),
                                FormaVenta = ConvertHelper.ToNonNullString(rd["F5_CFORVEN"]),
                                DesFormaVenta = ConvertHelper.ToNonNullString(rd["FV_CDESCRI"]),
                                DesVendedor = ConvertHelper.ToNonNullString(rd["VE_CNOMBRE"]),
                                DesAgencia = ConvertHelper.ToNonNullString(rd["AG_CDESCRI"]),
                                SerieFactura = ConvertHelper.ToNonNullString(rd["F5_CNUMSER"]),
                                NumeroFactura = ConvertHelper.ToNonNullString(rd["F5_CNUMDOC"]),
                            };

                            Lista.Add(cab);
                        }

                        response.Pedidos = Lista;

                        await rd.NextResultAsync();

                        while (await rd.ReadAsync())
                        {
                            response.Total = Convert.ToInt32(rd["TOTALES"]);
                        }

                        await rd.CloseAsync();
                    }

                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentNullException(e.Message);
            }

            return response;
        }
        public async Task<ResponseDetallePedido> ListarDetallePedido(RequestPedido request)
        {
            var param0 = new List<MySqlParameter>();

            ResponseDetallePedido response = new ResponseDetallePedido();

            var query = new StringBuilder();
            var tabla = $"PD{request.CIA}PEND";
            var tablaPENC = $"PD{request.CIA}PENC";

            try
            {

                List<DetallePedido> Lista = new List<DetallePedido>();

                query.Append(" SELECT F6_CITEM,F6_CCODIGO,F6_CDESCRI,F6_CUNIDAD,F6_NCANTID,F6_NPRECIO,F6_NICBPUS, F6_NPORDES,")
                    .Append(" F6_NDESCTO,F6_CTR,F6_NIMPMN,F6_NIMPUS,F6_NIGV,F6_NISC,F6_NICBPMN,F6_CTF, F5_CCODMON ")
                    .Append($" FROM {tabla} INNER JOIN {tablaPENC} ON F6_CCODAGE = F5_CCODAGE AND F6_CNUMPED = F5_CNUMPED")
                    .Append($" WHERE F5_CTD ='PD' AND F6_CCODAGE = @Agencia AND F6_CNUMPED = @Numero ORDER BY F6_CITEM ASC")
                    ;

                param0.Add(new MySqlParameter("Agencia", request.Agencia));
                param0.Add(new MySqlParameter("Numero", request.Numero));

                string nuevaConexion = await ObtenerCadenaConexion(request.CCID);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = query.ToString();
                        cmd.Parameters.AddRange(param0.ToArray());

                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();


                        while (await rd.ReadAsync())
                        {
                            var detalle = new DetallePedido
                            {
                                Item = ConvertHelper.ToNonNullString(rd["F6_CITEM"]),
                                CodArticulo = ConvertHelper.ToNonNullString(rd["F6_CCODIGO"]),
                                DesArticulo = ConvertHelper.ToNonNullString(rd["F6_CDESCRI"]),
                                Unidad = ConvertHelper.ToNonNullString(rd["F6_CUNIDAD"]),
                                Cantidad = Convert.ToDecimal(rd["F6_NCANTID"]),
                                Precio = Convert.ToDecimal(rd["F6_NPRECIO"]),
                                MontoDescuento = Convert.ToDecimal(rd["F6_NDESCTO"]),
                                ImpSoles = Convert.ToDecimal(rd["F6_NIMPMN"]),
                                ImpDolares = Convert.ToDecimal(rd["F6_NIMPUS"]),
                                Igv = Convert.ToDecimal(rd["F6_NIGV"]),
                                Isc = Convert.ToDecimal(rd["F6_NISC"]),
                                IcbpSoles = Convert.ToDecimal(rd["F6_NICBPMN"]),
                                IcbpDolares = Convert.ToDecimal(rd["F6_NICBPUS"]),
                                PorDescuento = Convert.ToDecimal(rd["F6_NPORDES"]),
                                Moneda = ConvertHelper.ToNonNullString(rd["F5_CCODMON"]),
                                TipoAfectacion = ConvertHelper.ToNonNullString(rd["F6_CTF"])
                            };

                            Lista.Add(detalle);
                        }

                        response.DetallePedidos = Lista;
                        await rd.CloseAsync();
                    }

                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentNullException(e.Message);
            }

            return response;
        }
        #endregion
        public async Task<List<FWCIAS>> ObtenerTotalCias(FWCIAS_REQUEST FWCIAS_REQUEST)
        {
            var FWCIAS = new List<FWCIAS>();
            string Condicion = "";

            if (!string.IsNullOrEmpty(FWCIAS_REQUEST.FWCCRUC))
            {
                Condicion = " WHERE CI_CRUC like '" + FWCIAS_REQUEST.FWCCRUC + "%' ORDER BY CI_CRAZON ASC";
            }

            if (!string.IsNullOrEmpty(FWCIAS_REQUEST.FWCCRAZON))
            {
                Condicion = " WHERE CI_CRAZON like '%" + FWCIAS_REQUEST.FWCCRAZON + "%' ORDER BY CI_CRAZON ASC";
            }

            if (!string.IsNullOrEmpty(FWCIAS_REQUEST.FWCCRUC) && !string.IsNullOrEmpty(FWCIAS_REQUEST.FWCCRAZON))
            {
                Condicion = " WHERE CI_CRUC like '" + FWCIAS_REQUEST.FWCCRUC + "%' ORDER BY CI_CRAZON ASC";
            }

            if (!string.IsNullOrEmpty(FWCIAS_REQUEST.FWCFACTIVO))
            {
                Condicion = " WHERE CI_CFACTIVO like '" + FWCIAS_REQUEST.FWCFACTIVO + "%' ORDER BY CI_CRAZON ASC";
            }

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT CI_CCIA, CI_CRUC, CI_CID, CI_CRAZON " +
                                           "FROM FWCIAS" + Condicion + ";";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWCIAS.Add(new FWCIAS()
                                {
                                    CI_CCIA = rd["CI_CCIA"].ToString().Trim(),
                                    CI_CRUC = rd["CI_CRUC"].ToString().Trim(),
                                    CI_CRAZON = rd["CI_CRAZON"].ToString().Trim(),
                                    CI_CID = rd["CI_CID"].ToString().Trim()
                                });
                            }
                            rd.Close();
                        }
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return FWCIAS;
        }
        #region HlAlertas
        public async Task<List<MKTPUBLIC>> ListarTipo(string tipo)
        {
            var mkts = new List<MKTPUBLIC>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();

                        // Consulta base para listar banners por tipo
                        var query = @"
                SELECT
                    MK.MK_NID, 
                    MK.MK_CNOMBRE, 
                    MK.MK_CTARGET, 
                    MK.MK_CFACTIVO, 
                    MK.MK_CIMGURL, 
                    MK.MK_CLINKURL, 
                    MK.MK_CUSUMOD, 
                    MK.MK_DFECMOD, 
                    MK.MK_NUBIC, 
                    MK.MK_CTITLE, 
                    MK.MK_CCONTENT, 
                    MK.MK_DFECDESDE, 
                    MK.MK_DFECHASTA, 
                    MK.MK_DFECCRE, 
                    MK.MK_CUUID,
                    CRE.US_CNOMBRE AS CREADOR_NOMBRE,
                    MODI.US_CNOMBRE AS MODIFICADOR_NOMBRE,
                    CRE.US_CCODAMB AS CREADOR_AMBIENTE,
                    MODI.US_CCODAMB AS MODIFICADOR_AMBIENTE
                FROM
                    MKTPUBLIC MK
                LEFT JOIN
                    FWUSERS CRE ON CRE.US_CIDUSU = MK.MK_CUSUCRE
                LEFT JOIN
                    FWUSERS MODI ON MODI.US_CIDUSU = MK.MK_CUSUMOD
                WHERE
                    MK.MK_CTIPO = @Tipo";

                        // Agregar orden por MK_NUBIC solo si el tipo es "BAN"
                        if (tipo == "BAN")
                        {
                            query += " ORDER BY MK.MK_NUBIC;";
                        }

                        // Asignar parámetro
                        cmd.CommandText = query;
                        cmd.Parameters.AddWithValue("@Tipo", tipo);

                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                var mkt = new MKTPUBLIC
                                {
                                    //MK_NID = reader.GetInt32("MK_NID"),
                                    //MK_CNOMBRE = reader.GetString("MK_CNOMBRE"),
                                    //MK_CTARGET = reader.GetString("MK_CTARGET"),
                                    //MK_CFACTIVO = reader.GetString("MK_CFACTIVO"),
                                    //MK_CIMGURL = reader.GetString("MK_CIMGURL"),
                                    //MK_CLINKURL = reader.GetString("MK_CLINKURL"),
                                    //MK_CUSUMOD = reader.GetString("MK_CUSUMOD"),
                                    //MK_DFECMOD = reader.GetDateTime("MK_DFECMOD"),
                                    //MK_DFECCRE = reader.GetDateTime("MK_DFECCRE"),
                                    //MK_CTITLE = reader.GetString("MK_CTITLE"),
                                    //MK_CCONTENT = string.Empty,
                                    //MK_CTDESCRIPTION = reader.GetString("MK_CCONTENT"),
                                    //MK_NUBIC = reader.GetInt32("MK_NUBIC"),
                                    //MK_CUUID = reader.GetString("MK_CUUID"),
                                    //CR_NOMBRE = reader.GetString("CREADOR_NOMBRE"),
                                    //MR_NOMBRE = reader.GetString("MODIFICADOR_NOMBRE"),
                                    //CR_AMB = reader.GetString("CREADOR_AMBIENTE"),
                                    //MR_AMB = reader.GetString("MODIFICADOR_AMBIENTE"),
                                    MK_NID = reader.IsDBNull(reader.GetOrdinal("MK_NID")) ? (int?)null ?? 0 : reader.GetInt32("MK_NID"),
                                    MK_CNOMBRE = reader.IsDBNull(reader.GetOrdinal("MK_CNOMBRE")) ? null : reader.GetString("MK_CNOMBRE"),
                                    MK_CTARGET = reader.IsDBNull(reader.GetOrdinal("MK_CTARGET")) ? null : reader.GetString("MK_CTARGET"),
                                    MK_CFACTIVO = reader.IsDBNull(reader.GetOrdinal("MK_CFACTIVO")) ? null : reader.GetString("MK_CFACTIVO"),
                                    MK_CIMGURL = reader.IsDBNull(reader.GetOrdinal("MK_CIMGURL")) ? null : reader.GetString("MK_CIMGURL"),
                                    MK_CLINKURL = reader.IsDBNull(reader.GetOrdinal("MK_CLINKURL")) ? null : reader.GetString("MK_CLINKURL"),
                                    MK_CUSUMOD = reader.IsDBNull(reader.GetOrdinal("MK_CUSUMOD")) ? null : reader.GetString("MK_CUSUMOD"),
                                    MK_CTITLE = reader.IsDBNull(reader.GetOrdinal("MK_CTITLE")) ? null : reader.GetString("MK_CTITLE"),
                                    MK_CCONTENT = string.Empty, // Se deja explícitamente null
                                    MK_CTDESCRIPTION = reader.IsDBNull(reader.GetOrdinal("MK_CCONTENT")) ? null : reader.GetString("MK_CCONTENT"),
                                    MK_NUBIC = reader.IsDBNull(reader.GetOrdinal("MK_NUBIC")) ? (int?)null ?? 0 : reader.GetInt32("MK_NUBIC"),
                                    MK_CUUID = reader.IsDBNull(reader.GetOrdinal("MK_CUUID")) ? null : reader.GetString("MK_CUUID"),
                                    CR_NOMBRE = reader.IsDBNull(reader.GetOrdinal("CREADOR_NOMBRE")) ? null : reader.GetString("CREADOR_NOMBRE"),
                                    MR_NOMBRE = reader.IsDBNull(reader.GetOrdinal("MODIFICADOR_NOMBRE")) ? null : reader.GetString("MODIFICADOR_NOMBRE"),
                                    CR_AMB = reader.IsDBNull(reader.GetOrdinal("CREADOR_AMBIENTE")) ? null : reader.GetString("CREADOR_AMBIENTE"),
                                    MR_AMB = reader.IsDBNull(reader.GetOrdinal("MODIFICADOR_AMBIENTE")) ? null : reader.GetString("MODIFICADOR_AMBIENTE"),
                                    MK_DFECMOD = reader.IsDBNull(reader.GetOrdinal("MK_DFECMOD"))
                                                   ? (DateTime?)null
                                                   : reader.GetDateTime(reader.GetOrdinal("MK_DFECMOD")),

                                    MK_DFECCRE = reader.IsDBNull(reader.GetOrdinal("MK_DFECCRE"))
                                                   ? (DateTime?)null
                                                   : reader.GetDateTime(reader.GetOrdinal("MK_DFECCRE")),

                                    MK_DFECDESDE = reader.IsDBNull(reader.GetOrdinal("MK_DFECDESDE"))
                                                   ? (DateTime?)null
                                                   : reader.GetDateTime(reader.GetOrdinal("MK_DFECDESDE")),
                                    MK_DFECHASTA = reader.IsDBNull(reader.GetOrdinal("MK_DFECHASTA"))
                                                   ? (DateTime?)null
                                                   : reader.GetDateTime(reader.GetOrdinal("MK_DFECHASTA")),

                                };

                                mkts.Add(mkt);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // Manejar el error de la consulta
                throw new Exception("Error al listar", e);
            }

            return mkts;
        }
        public async Task<Error> Eliminar(long id)
        {
            var Error = new Error();
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();

                        // Consulta de eliminación
                        var deleteQuery = "DELETE FROM MKTPUBLIC WHERE MK_NID = @Id";

                        // Asignar parámetros
                        cmd.CommandText = deleteQuery;
                        cmd.Parameters.AddWithValue("@Id", id);

                        await cmd.ExecuteNonQueryAsync();
                        Error.Status_code = "0"; // Éxito
                    }
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1"; // Error
            }
            return Error;
        }
        public async Task<Error> CrearAlerta(MKTPUBLIC model)
        {
            var Error = new Error();
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();

                        // Consulta de inserción
                        var insertQuery = @"
                    INSERT INTO MKTPUBLIC (MK_CNOMBRE, MK_CTARGET, MK_CTIPO,MK_CFACTIVO, MK_CIMGURL, MK_CLINKURL, MK_CUSUMOD, MK_DFECMOD, MK_DFECCRE, MK_DFECDESDE, MK_DFECHASTA, MK_NUBIC, MK_CUSUCRE, MK_DFECTSTP, MK_CTITLE, MK_CCONTENT)
                    VALUES (@Nombre, @Target , @Tipo , @Activo, '', '', @UsuarioMod, NOW(), NOW(),@Desde, @Hasta, '0', @UsuarioCr, NOW() , '', @Content)";

                        // Asignar parámetros
                        cmd.CommandText = insertQuery;
                        cmd.Parameters.AddWithValue("@Nombre", model.MK_CNOMBRE);
                        cmd.Parameters.AddWithValue("@Tipo", model.MK_CTIPO);
                        cmd.Parameters.AddWithValue("@Activo", model.MK_CFACTIVO);
                        cmd.Parameters.AddWithValue("@UsuarioMod", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@Target", model.MK_CTARGET);
                        cmd.Parameters.AddWithValue("@Content", model.MK_CTDESCRIPTION);
                        cmd.Parameters.AddWithValue("@Desde", model.MK_DFECDESDE);
                        cmd.Parameters.AddWithValue("@Hasta", model.MK_DFECHASTA);
                        cmd.Parameters.AddWithValue("@UsuarioCr", model.MK_CUSUCRE);

                        await cmd.ExecuteNonQueryAsync();
                        Error.Status_code = "0"; // Éxito
                        Error.Status_message = "Se creo con exito";
                    }
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1"; // Error
                Error.Status_message = "Ocurrió un error al crear";
            }
            return Error;
        }
        public async Task<Error> EditarAlerta(MKTPUBLIC model)
        {
            var Error = new Error();
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();

                        // Consulta de actualización
                        var updateQuery = @"
                    UPDATE MKTPUBLIC
                    SET MK_CNOMBRE = @Nombre,
                        MK_CFACTIVO = @Activo,
                        MK_CUSUMOD = @UsuarioMod,
                        MK_DFECMOD = NOW(),
                        MK_CTARGET = @Target,
                        MK_CCONTENT = @Content,
                        MK_DFECDESDE = @Desde,
                        MK_DFECHASTA = @Hasta
                    WHERE MK_NID = @Id AND MK_CTIPO = @Tipo";

                        // Asignar parámetros
                        cmd.CommandText = updateQuery;
                        cmd.Parameters.AddWithValue("@Nombre", model.MK_CNOMBRE);
                        cmd.Parameters.AddWithValue("@Activo", model.MK_CFACTIVO);
                        cmd.Parameters.AddWithValue("@UsuarioMod", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@Content", model.MK_CTDESCRIPTION);
                        cmd.Parameters.AddWithValue("@Target", model.MK_CTARGET);
                        cmd.Parameters.AddWithValue("@Id", model.MK_NID);
                        cmd.Parameters.AddWithValue("@Tipo", model.MK_CTIPO);
                        cmd.Parameters.AddWithValue("@Desde", model.MK_DFECDESDE);
                        cmd.Parameters.AddWithValue("@Hasta", model.MK_DFECHASTA);

                        await cmd.ExecuteNonQueryAsync();
                        Error.Status_code = "0"; // Éxito
                        Error.Status_message = "Se actualizo correctamente";
                    }
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1"; // Error
                Error.Status_message = "Ocurrió un error al editar";
            }
            return Error;
        }
        public async Task<Error> PublishAlert(List<MKTPUBLIC> models)
        {
            var Error = new Error();

            foreach (var model in models)
            {
                if (model.MK_CTIPO != "ALE" || model.publicar != "S")
                {
                    Error.Status_message = "Tipo de alerta inválido o no está activo para publicar.";
                    return Error;
                }
            }

            // Crear la lista de objetos para el JSON
            var jsonAlerts = models.Select(b => new
            {
                target = b.MK_CTARGET,
                fecha_desde = b.MK_DFECDESDE?.ToString("yyyy-MM-dd"),
                fecha_hasta = b.MK_DFECHASTA?.ToString("yyyy-MM-dd"),
                content = b.MK_CTDESCRIPTION
            }).ToList();

            // Convertir a JSON
            var jsonString = JsonConvert.SerializeObject(jsonAlerts);

            // Enviar JSON al servicio Update
            await UploadJsonToS3(jsonString, "ALE", new string[] { }, new string[] { });

            Error.Status_message = "Alertas publicadas correctamente";
            return Error;
        }
        public async Task<Error> CrearNotification(MKTPUBLIC model)
        {
            var Error = new Error();
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();

                        // Consulta de inserción
                        var insertQuery = @"
                    INSERT INTO MKTPUBLIC (MK_CNOMBRE, MK_CTARGET, MK_CTIPO,MK_CFACTIVO, MK_CIMGURL, MK_CLINKURL, MK_CUSUMOD, MK_DFECMOD, MK_DFECCRE, MK_DFECDESDE, MK_DFECHASTA, MK_NUBIC, MK_CUSUCRE, MK_DFECTSTP, MK_CTITLE, MK_CCONTENT, MK_CUUID)
                    VALUES (@Nombre, @Target , @Tipo , @Activo,  @ImgUrl, @LinkUrl, @UsuarioMod, NOW(), NOW(),@Desde, @Hasta, '0', @UsuarioCr, NOW(), @Title , @Content, @Cuid)";

                        // Asignar parámetros
                        cmd.CommandText = insertQuery;
                        cmd.Parameters.AddWithValue("@Nombre", model.MK_CNOMBRE);
                        cmd.Parameters.AddWithValue("@Target", model.MK_CTARGET);
                        cmd.Parameters.AddWithValue("@Tipo", model.MK_CTIPO);
                        cmd.Parameters.AddWithValue("@Activo", model.MK_CFACTIVO);
                        cmd.Parameters.AddWithValue("@ImgUrl", model.MK_CIMGURL);
                        cmd.Parameters.AddWithValue("@LinkUrl", model.MK_CLINKURL);
                        cmd.Parameters.AddWithValue("@UsuarioMod", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@UsuarioCr", model.MK_CUSUCRE);
                        cmd.Parameters.AddWithValue("@Desde", model.MK_DFECDESDE);
                        cmd.Parameters.AddWithValue("@Hasta", model.MK_DFECHASTA);
                        cmd.Parameters.AddWithValue("@Title", model.MK_CTITLE);
                        cmd.Parameters.AddWithValue("@Content", model.MK_CTDESCRIPTION);
                        cmd.Parameters.AddWithValue("@Cuid", model.MK_CUUID);



                        await cmd.ExecuteNonQueryAsync();
                        Error.Status_code = "0"; // Éxito
                        Error.Status_message = "Se creo con exito";
                    }
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1"; // Error
                Error.Status_message = "Ocurrió un error al crear";
            }
            return Error;
        }
        public async Task<Error> EditarNotification(MKTPUBLIC model)
        {
            var Error = new Error();
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();

                        // Consulta de actualización
                        var updateQuery = @"
                    UPDATE MKTPUBLIC
                    SET MK_CNOMBRE = @Nombre,
                        MK_CFACTIVO = @Activo,
                        MK_CUSUMOD = @UsuarioMod,
                        MK_CIMGURL = @ImgUrl,
                        MK_CLINKURL = @LinkUrl,
                        MK_DFECMOD = NOW(),
                        MK_CCONTENT = @Content,
                        MK_CTARGET = @Target,
                        MK_CTITLE = @Title,
                        MK_DFECDESDE = @Desde,
                        MK_DFECHASTA = @Hasta,
                        MK_CUUID = @Cuid
                    WHERE MK_NID = @Id AND MK_CTIPO = @Tipo";

                        // Asignar parámetros
                        cmd.CommandText = updateQuery;
                        cmd.Parameters.AddWithValue("@Nombre", model.MK_CNOMBRE);
                        cmd.Parameters.AddWithValue("@Activo", model.MK_CFACTIVO);
                        cmd.Parameters.AddWithValue("@UsuarioMod", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@ImgUrl", model.MK_CIMGURL);
                        cmd.Parameters.AddWithValue("@LinkUrl", model.MK_CLINKURL);
                        cmd.Parameters.AddWithValue("@Content", model.MK_CTDESCRIPTION);
                        cmd.Parameters.AddWithValue("@Target", model.MK_CTARGET);
                        cmd.Parameters.AddWithValue("@Title", model.MK_CTITLE);
                        cmd.Parameters.AddWithValue("@Desde", model.MK_DFECDESDE);
                        cmd.Parameters.AddWithValue("@Hasta", model.MK_DFECHASTA);
                        cmd.Parameters.AddWithValue("@Id", model.MK_NID);
                        cmd.Parameters.AddWithValue("@Tipo", model.MK_CTIPO);
                        cmd.Parameters.AddWithValue("@Cuid", model.MK_CUUID);

                        await cmd.ExecuteNonQueryAsync();
                        Error.Status_code = "0"; // Éxito
                        Error.Status_message = "Se actualizo correctamente";
                    }
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1"; // Error
                Error.Status_message = "Ocurrió un error al editar";
            }
            return Error;
        }
        public async Task<Error> PublishNotification(List<MKTPUBLIC> models)
        {
            var Error = new Error();

            // Verificar que todos los modelos tienen tipo "NOT" y están marcados para publicación
            foreach (var model in models)
            {
                if (model.MK_CTIPO != "NOT" || model.publicar != "S")
                {
                    Error.Status_message = "Tipo de Notificación inválido o no está activo para publicar.";
                    return Error;
                }
            }

            // Agrupar modelos por el campo "target"
            var groupedModels = models.GroupBy(m => m.MK_CTARGET);

            // Crear la lista de objetos para el JSON
            var jsonAlerts = groupedModels.Select(group => new
            {
                target = group.Key,
                content = group.Select(b => new
                {
                    image_url = $"{_baseUrl}assets/img/{b.MK_CIMGURL}",
                    title = b.MK_CTITLE,
                    description = b.MK_CTDESCRIPTION,
                    link_url = b.MK_CLINKURL,
                    fecha = b.MK_DFECCRE?.ToString("dd/MM/yyyy HH:mm"),  // Formatear con fecha y hora
                    fecha_desde = b.MK_DFECDESDE?.ToString("yyyy-MM-dd"),  // Formatear DATETIME
                    fecha_hasta = b.MK_DFECHASTA?.ToString("yyyy-MM-dd")   // Formatear DATETIME
                }).ToList()
            }).ToList();

            // Convertir a JSON
            var jsonString = JsonConvert.SerializeObject(jsonAlerts);

            // Enviar JSON al servicio Update
            await UploadJsonToS3(jsonString, "NOT", models.Select(b => b.MK_CCONTENT).ToArray(), models.Select(b => b.MK_CIMGURL).ToArray());

            Error.Status_message = "Notificaciones publicadas correctamente";
            return Error;
        }
        #endregion
        private async Task<string> ObtenerCadenaConexion(string id)
        {
            string nuevaCadenaConexion = "";

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @$"
                    SELECT CI_CDBFACCAR 
                    FROM FWCIAS 
                    WHERE CI_CID = '{id}'
                    LIMIT 1;";

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            if (rd.Read())
                            {
                                nuevaCadenaConexion = rd["CI_CDBFACCAR"].ToString().Trim();
                            }
                            rd.Close();
                        }
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            return nuevaCadenaConexion;
        }

        private async Task UploadJsonToS3(string jsonString, string tipo, string[] images, string[] imageNames)
        {
            // Configuración para S3
            string bucketName = _configuracion["s3-bucket-arn"];
            string accessKey = _configuracion["s3-access-key-id"];
            string secretKey = _configuracion["s3-secret-access-key"];

            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            using var client = new AmazonS3Client(credentials, RegionEndpoint.USEast1);

            // Determinar el nombre del archivo JSON
            string nombreArchivo = tipo == "PPE" ? "facweb-popup-externo.json" : tipo == "BAN" ? "facweb-banner.json" : tipo == "PPI" ? "facweb-popup-interno.json" : tipo == "ALE" ? "facweb-alerta.json" : "facweb-notification.json";

            try
            {
                // Convertir JSON string a byte array
                byte[] fileByteArray = System.Text.Encoding.UTF8.GetBytes(jsonString);
                using MemoryStream fileToUpload = new MemoryStream(fileByteArray);

                var requestPut = new PutObjectRequest()
                {
                    BucketName = bucketName,
                    Key = $"{nombreArchivo}",
                    InputStream = fileToUpload,
                    ContentType = "application/json"
                };

                Console.WriteLine($"Subiendo archivo JSON a S3: {requestPut.Key}");
                await client.PutObjectAsync(requestPut);

                for (int i = 0; i < images.Length; i++)
                {
                    byte[] imageBytes = Convert.FromBase64String(images[i]);
                    using MemoryStream imageFileStream = new MemoryStream(imageBytes);

                    string imagePath = tipo switch
                    {
                        "BAN" => $"banner/{imageNames[i]}",      // Para tipo BAN
                        "PPE" => $"popup/{imageNames[i]}",       // Para tipo EXT
                        "PPI" => $"popup/{imageNames[i]}",       // Para tipo INT
                        "ALT" => $"assets/img/{imageNames[i]}",  // Para tipo ALT
                        "NOT" => $"assets/img/{imageNames[i]}",  // Para tipo NOT
                        _ => imageNames[i]                       // Ruta por defecto
                    };

                    string extension = Path.GetExtension(imageNames[i]).ToLower();
                    string contentType = extension switch
                    {
                        ".png" => "image/png",
                        ".jpg" => "image/jpeg",
                        ".jpeg" => "image/jpeg",
                        ".gif" => "image/gif",
                        ".svg" => "image/svg+xml",
                        _ => "application/octet-stream"  // Tipo por defecto si la extensión no coincide
                    };

                    var imageRequest = new PutObjectRequest
                    {
                        BucketName = bucketName,
                        Key = imagePath,
                        InputStream = imageFileStream,
                        ContentType = contentType
                    };

                    Console.WriteLine($"Subiendo imagen a S3: {imageRequest.Key}");
                    await client.PutObjectAsync(imageRequest);
                }

            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine($"Amazon S3 Error: {e.Message}");
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine($"General Error: {e.Message}");
                throw;
            }

        }
        private void OpenSecurityWeb()
        {
            System.Net.ServicePointManager.ServerCertificateValidationCallback
                = delegate (object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
                {
                    return true;
                };
        }
    }
}
