using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;
using RSFacLocal.Entities.PFX;
using RSFacLocal.Entities.REPORTE;
using RSFacLocal.ModelsView.FWFTCABF;
using RSFacWeb.Entities;
using RSFacWeb.Entities.FWCIAPLAN;
using RSFacWeb.Entities.FWUSUCIA;
using RSFacWeb.Entities.PLANES;
using RSFacWeb.Interfaces;
using RSFacWeb.Models.Databases;
using RSFacWeb.ModelsView;
using RSFacWeb.ModelsView.FELOG;
using RSFacWeb.ModelsView.FTAGEN;
using RSFacWeb.ModelsView.FWCNFG;
using RSFacWeb.ModelsView.PLAN;
using RSFacWeb.ModelsView.PLANCIA;
using RSFacWeb.Util;
using RSFacWeb.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace RSFacWeb.Models
{
    public class CompaniaDAL : ICompaniaDAL
    {
        private readonly MySQLDatabase _MySqlDatabase;

        public CompaniaDAL(MySQLDatabase MySqlDatabase)
        {
            _MySqlDatabase = MySqlDatabase;
        }

        public async Task<List<FWCIAS_LOGIN>> ObtenerCias(int US_CIDUSU)
        {
            var FWCIAS_LOGIN = new List<FWCIAS_LOGIN>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT CI_CCIA, UC_CCODCIA, CI_CSWURL, CI_CFSTOREFILE, " +
                                           "CI_CUUID, CI_CFCERTORI, CI_CLOGOTKB64, CI_CPROVMOD, " +
                                           "CI_CRAZON, CI_CSWUSU, CI_CSWPASS, CI_CFDB, CI_CFDEBUG, " +
                                           "CI_CDBFACCAR, CI_CDBCONCAR ,CI_CFMODO, CI_CRKV " +
                                           "FROM FWUSUCIA UCIA INNER JOIN FWCIAS CIA ON UC_CCODCIA = CI_CID " +
                                           "WHERE UCIA.UC_CIDUSU = " + US_CIDUSU + " AND CIA.CI_CFACTIVO = 'S';";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWCIAS_LOGIN.Add(new FWCIAS_LOGIN()
                                {
                                    PUCODEMP = rd["CI_CCIA"].ToString(),
                                    PUCODCIA = rd["UC_CCODCIA"].ToString(),
                                    PUCSWURL = rd["CI_CSWURL"].ToString(),
                                    PUCRAZON = rd["CI_CRAZON"].ToString(),
                                    PUCIAUUID = rd["CI_CUUID"].ToString(),
                                    PUCFCERTORI = rd["CI_CFCERTORI"].ToString(),
                                    PUCSWUSU = rd["CI_CSWUSU"].ToString(),
                                    PUCSWPASS = rd["CI_CSWPASS"].ToString(),
                                    PUCFDB = rd["CI_CFDB"].ToString(),
                                    PUCLOGOTKB64 = rd["CI_CLOGOTKB64"].ToString(),
                                    PUCBDFACCAR = Security.Encrypt(rd["CI_CDBFACCAR"].ToString()),
                                    PUCBDCONCAR = Security.Encrypt(rd["CI_CDBCONCAR"].ToString()),
                                    PUCFMODO = rd["CI_CFMODO"].ToString(),
                                    PUCFPROVMOD = rd["CI_CPROVMOD"].ToString(),
                                    PUCFDEBUG = rd["CI_CFDEBUG"].ToString(),
                                    PUCFSTOREFILE = string.IsNullOrEmpty(rd["CI_CFSTOREFILE"].ToString()) ? "LC" : rd["CI_CFSTOREFILE"].ToString(),
                                    PUCCRKV = rd["CI_CRKV"].ToString(),
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
            return FWCIAS_LOGIN;
        }
        public async Task<List<FWCIAS_LOGIN>> ObtenerCiasFcha(int US_CIDUSU)
        {
            var FWCIAS_LOGIN = new List<FWCIAS_LOGIN>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT CI_CCIA, UC_CCODCIA, CI_CSWURL, CI_CFSTOREFILE, " +
                                           "CI_CUUID, CI_CFCERTORI, CI_CLOGOTKB64, CI_CPROVMOD, " +
                                           "CI_CRAZON, CI_CSWUSU, CI_CSWPASS, CI_CFDB, CI_CFDEBUG, " +
                                           "CI_CDBFACCAR, CI_CDBCONCAR ,CI_CFMODO, CI_CRKV " +
                                           "FROM FWUSUCIA UCIA INNER JOIN FWCIAS CIA ON UC_CCODCIA = CI_CID " +
                                           "WHERE UCIA.UC_CIDUSU = " + US_CIDUSU + " AND CIA.CI_DFECVEN >= CURRENT_DATE;;";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWCIAS_LOGIN.Add(new FWCIAS_LOGIN()
                                {
                                    PUCODEMP = rd["CI_CCIA"].ToString(),
                                    PUCODCIA = rd["UC_CCODCIA"].ToString(),
                                    PUCSWURL = rd["CI_CSWURL"].ToString(),
                                    PUCRAZON = rd["CI_CRAZON"].ToString(),
                                    PUCIAUUID = rd["CI_CUUID"].ToString(),
                                    PUCFCERTORI = rd["CI_CFCERTORI"].ToString(),
                                    PUCSWUSU = rd["CI_CSWUSU"].ToString(),
                                    PUCSWPASS = rd["CI_CSWPASS"].ToString(),
                                    PUCFDB = rd["CI_CFDB"].ToString(),
                                    PUCLOGOTKB64 = rd["CI_CLOGOTKB64"].ToString(),
                                    PUCBDFACCAR = Security.Encrypt(rd["CI_CDBFACCAR"].ToString()),
                                    PUCBDCONCAR = Security.Encrypt(rd["CI_CDBCONCAR"].ToString()),
                                    PUCFMODO = rd["CI_CFMODO"].ToString(),
                                    PUCFPROVMOD = rd["CI_CPROVMOD"].ToString(),
                                    PUCFDEBUG = rd["CI_CFDEBUG"].ToString(),
                                    PUCFSTOREFILE = string.IsNullOrEmpty(rd["CI_CFSTOREFILE"].ToString()) ? "LC" : rd["CI_CFSTOREFILE"].ToString(),
                                    PUCCRKV = rd["CI_CRKV"].ToString(),
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
            return FWCIAS_LOGIN;
        }
        public async Task<List<FWFTCABF>> GetConfigCabFacturacion(string CF_CCODCIA, string CF_CPROGRA)
        {
            var FWFTCABF = new List<FWFTCABF>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT CF_CNOMOBJ, CF_CFLAHAB, CF_CDEFAUL " +
                                           "FROM FWFTCABF " +
                                           "WHERE CF_CCODCIA='" + CF_CCODCIA + "' " +
                                           "AND CF_CPROGRA='" + CF_CPROGRA + "';";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWFTCABF.Add(new FWFTCABF()
                                {
                                    CF_CNOMOBJ = rd["CF_CNOMOBJ"].ToString(),
                                    CF_CFLAHAB = rd["CF_CFLAHAB"].ToString(),
                                    CF_CDEFAUL = rd["CF_CDEFAUL"].ToString()
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
            return FWFTCABF;
        }

        public async Task<List<FWFTCABF>> GetListaFWTCABF(ListaRequest request)
        {
            var FWFTCABF = new List<FWFTCABF>();

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(request.Ruc);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;

                        // Construcción de la consulta con el filtro opcional
                        var query = "SELECT CF_CPROGRA, CF_CDESOBJ, CF_CNOMOBJ, CF_CFLAHAB, CF_CFLAVIS, CF_CDEFAUL " +
                                    "FROM FT" + request.Cia + "CABF " +
                                    "WHERE CF_CPROGRA=@CF_CPROGRA";

                        if (!string.IsNullOrEmpty(request.Filtro))
                        {
                            query += " AND CF_CDESOBJ LIKE @CF_CDESOBJ";
                        }

                        cmd.CommandText = query;

                        // Parámetros para evitar inyección SQL
                        cmd.Parameters.AddWithValue("@CF_CPROGRA", request.Data);

                        if (!string.IsNullOrEmpty(request.Filtro))
                        {
                            cmd.Parameters.AddWithValue("@CF_CDESOBJ", "%" + request.Filtro + "%");
                        }

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWFTCABF.Add(new FWFTCABF()
                                {
                                    CF_CPROGRA = rd["CF_CPROGRA"].ToString(),
                                    CF_CDESOBJ = rd["CF_CDESOBJ"].ToString(),
                                    CF_CNOMOBJ = rd["CF_CNOMOBJ"].ToString(),
                                    CF_CFLAHAB = rd["CF_CFLAHAB"].ToString(),
                                    CF_CFLAVIS = rd["CF_CFLAVIS"].ToString(),
                                    CF_CDEFAUL = rd["CF_CDEFAUL"].ToString()
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
            return FWFTCABF;
        }

        public async Task<List<FWFTCABF>> GetListaCombo(ListaRequest request)
        {
            List<FWFTCABF> Lista = new List<FWFTCABF>();
            string tabla = "FT" + request.Cia + "CABF";
            string Query = " SELECT CF_CPROGRA FROM " + tabla + " GROUP BY CF_CPROGRA";
            string filtro = request.Filtro.Trim();

            if (!string.IsNullOrEmpty(filtro))
            {
                Query += $" AND (CF_CDESOBJ LIKE '%{filtro}%')";
            }

            try
            {
                string nuevaConexion = await ObtenerCadenaConexion(request.Ruc);

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
                            Lista.Add(new FWFTCABF()
                            {
                                CF_CPROGRA = rd["CF_CPROGRA"].ToString(),
                            });
                        }
                        await rd.CloseAsync();
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("GetLista: " + e.Message);
            }

            return Lista;
        }
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
                        cmd.CommandText = @"SELECT CI_CCIA, CI_CRUC, CI_CRAZON, CI_CFACTIVO, CI_CIDCUENTA, " +
                                           "DATE_FORMAT(CI_DFECVEN, '%d/%m/%Y') AS CI_DFECVEN, " +
                                           "DATE_FORMAT(CI_DFECVENCD, '%d/%m/%Y') AS CI_DFECVENCD, " +
                                           "CI_CLOGOTKB64, CI_CLOGOA4B64, CI_CDBFACCAR, CI_CDBCONCAR, CI_CFDEBUG, " +
                                           "CI_CUUID, CI_CSWURL, CI_CSWUSU, CI_CSWPASS, CI_CFDB, " +
                                           "CI_CFSYNC, CI_CFCERTORI, CI_CFTIPFIR, CI_CPROVMOD, CI_CCODPROV, CI_CFMODO, CI_CFMONITOREO, CI_NMAXUSR, CI_CFRKV, CI_CFAPI, CI_CFSTOREFILE " +
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
                                    CI_CFACTIVO = rd["CI_CFACTIVO"].ToString().Trim(),
                                    CI_CIDCUENTA = rd["CI_CIDCUENTA"].ToString().Trim(),
                                    CI_DFECVEN = rd["CI_DFECVEN"].ToString().Trim(),
                                    CI_CUUID = rd["CI_CUUID"].ToString().Trim(),
                                    CI_CSWURL = rd["CI_CSWURL"].ToString().Trim(),
                                    CI_CSWUSU = rd["CI_CSWUSU"].ToString().Trim(),
                                    CI_CSWPASS = rd["CI_CSWPASS"].ToString().Trim(),
                                    CI_CFDB = rd["CI_CFDB"].ToString().Trim(),
                                    CI_CFSYNC = rd["CI_CFSYNC"].ToString().Trim(),
                                    CI_CLOGOTKB64 = rd["CI_CLOGOTKB64"].ToString().Trim(),
                                    CI_CLOGOA4B64 = rd["CI_CLOGOA4B64"].ToString().Trim(),
                                    CI_DFECVENCD = rd["CI_DFECVENCD"].ToString().Trim(),
                                    CI_CDBFACCAR = rd["CI_CDBFACCAR"].ToString().Trim(),
                                    CI_CDBCONCAR = rd["CI_CDBCONCAR"].ToString().Trim(),
                                    CI_CFCERTORI = rd["CI_CFCERTORI"].ToString().Trim(),
                                    CI_CFTIPFIR = rd["CI_CFTIPFIR"].ToString().Trim(),
                                    CI_CPROVMOD = rd["CI_CPROVMOD"].ToString().Trim(),
                                    CI_CCODPROV = rd["CI_CCODPROV"].ToString().Trim(),
                                    CI_CFMODO = rd["CI_CFMODO"].ToString().Trim(),
                                    CI_NMAXUSR = rd["CI_NMAXUSR"].ToString().Trim(),
                                    CI_CFRKV = rd["CI_CFRKV"].ToString().Trim(),
                                    CI_CFMONITOREO = rd["CI_CFMONITOREO"].ToString().Trim(),
                                    CI_CFAPI = rd["CI_CFAPI"].ToString().Trim(),
                                    CI_CFSTOREFILE = rd["CI_CFSTOREFILE"].ToString().Trim(),
                                    CI_CFDEBUG = rd["CI_CFDEBUG"].ToString().Trim()
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

        public async Task<List<FELOG>> ObtenerLog(FELOG_REQUEST request)
        {
            var LOGS = new List<FELOG>();
            string condicion = "WHERE 1=1";

            if (!string.IsNullOrEmpty(request.FechaInicio) && !string.IsNullOrEmpty(request.FechaFin))
            {
                condicion += $" AND LG_DFEC BETWEEN '{request.FechaInicio}' AND '{request.FechaFin}'";
            }

            if (!string.IsNullOrEmpty(request.Agencia))
            {
                condicion += $" AND LG_CCODAGE = '{request.Agencia}'";
            }

            if (!string.IsNullOrEmpty(request.TipoDeMsj))
            {
                condicion += $" AND UPPER(LG_CTYPE) = '{request.TipoDeMsj.ToUpper()}'";
            }

            if (!string.IsNullOrEmpty(request.TD))
            {
                condicion += $" AND LG_CTD = '{request.TD}'";
            }

            if (!string.IsNullOrEmpty(request.Serie))
            {
                condicion += $" AND LG_CNUMSER = '{request.Serie}'";
            }

            if (!string.IsNullOrEmpty(request.NroDocumento))
            {
                condicion += $" AND LG_CNUMDOC = '{request.NroDocumento}'";
            }

            if (!string.IsNullOrEmpty(request.Resumen))
            {
                condicion += $" AND LG_CBZRESID = '{request.Resumen}'";
            }

            string limite;
            if (request.RegistrosMostrar.HasValue)
            {
                limite = $" LIMIT {request.RegistrosMostrar.Value}";
            }
            else
            {
                limite = "";
            }

            string query = @$"
                        SELECT 
                        UPPER(LG_CTYPE) AS LG_CTYPE,
                        DATE_FORMAT(LG_DFEC, '%d/%m/%Y %H:%i:%s') AS LG_DFEC2,
                        LG_CDESCR,
                        RTRIM(LG_CERROR) AS LG_CERROR,
                        LG_CIDCRON,
                        LG_CPROGR,
                        DATE_FORMAT(LG_DFECFIN, '%d/%m/%Y %H:%i:%s') AS LG_DFECFIN,
                        LG_CCODAGE,
                        CONCAT(LG_CCODAGE, ' ') AS AGENCIA,
                        CONCAT(RTRIM(LG_CTD), '-', RTRIM(LG_CNUMSER), '-', RTRIM(LG_CNUMDOC)) AS DOCUMENTO,
                        LG_CBZRESID,
                        LG_CEST,
                        LG_CUSU,
                        LG_CIP,
                        LG_CDEBUG,
                        LG_CPRMOD,
                        LG_NLOG,
                        LG_CPSE,
                        LG_CVERSION,
                        LG_DFEC
                        FROM FE{request.Cia}LOG 
                        {condicion}
                        ORDER BY LG_DFEC DESC, LG_NLOG DESC
                        {limite};
                    ";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(request.Id);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = query;

                        await cn.OpenAsync();

                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                LOGS.Add(new FELOG
                                {
                                    LG_CTYPE = rd["LG_CTYPE"].ToString(),
                                    LG_DFEC2 = rd["LG_DFEC2"].ToString(),
                                    LG_CDESCR = rd["LG_CDESCR"].ToString(),
                                    LG_CERROR = rd["LG_CERROR"].ToString(),
                                    LG_CIDCRON = rd["LG_CIDCRON"].ToString(),
                                    LG_CPROGR = rd["LG_CPROGR"].ToString(),
                                    LG_DFECFIN = rd["LG_DFECFIN"].ToString(),
                                    LG_CCODAGE = rd["LG_CCODAGE"].ToString(),
                                    AGENCIA = rd["AGENCIA"].ToString(),
                                    DOCUMENTO = rd["DOCUMENTO"].ToString(),
                                    LG_CBZRESID = rd["LG_CBZRESID"].ToString(),
                                    LG_CEST = rd["LG_CEST"].ToString(),
                                    LG_CUSU = rd["LG_CUSU"].ToString(),
                                    LG_CIP = rd["LG_CIP"].ToString(),
                                    LG_CDEBUG = rd["LG_CDEBUG"].ToString(),
                                    LG_CPRMOD = rd["LG_CPRMOD"].ToString(),
                                    LG_NLOG = rd["LG_NLOG"].ToString(),
                                    LG_CPSE = rd["LG_CPSE"].ToString(),
                                    LG_CVERSION = rd["LG_CVERSION"].ToString(),
                                    LG_DFEC = rd["LG_DFEC"].ToString()
                                });
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return LOGS;
        }


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


        public async Task<List<FWUSUCIA>> ObtenerCiaAsigUsu(string UC_CIDUSU)
        {
            var ciaAsignadaUsuarios = new List<FWUSUCIA>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"
                    SELECT
                        FUC.UC_CIDUSU,
                        FUC.UC_CCODCIA,
                        FCI.CI_CRUC,
                        FCI.CI_CCIA,
                        FCI.CI_CRAZON
                    FROM FWUSUCIA FUC
                    INNER JOIN FWCIAS FCI
                        ON FUC.UC_CCODCIA = FCI.CI_CID
                    WHERE FUC.UC_CIDUSU = @UC_CIDUSU;
                ";

                        // Añadir el parámetro para evitar SQL Injection
                        cmd.Parameters.AddWithValue("@UC_CIDUSU", UC_CIDUSU);

                        await cn.OpenAsync();

                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                ciaAsignadaUsuarios.Add(new FWUSUCIA()
                                {
                                    UC_CIDUSU = rd["UC_CIDUSU"].ToString(),
                                    UC_CCODCIA = rd["UC_CCODCIA"].ToString(),
                                    CI_CRUC = rd["CI_CRUC"].ToString(), // Añadido
                                    CI_CCIA = rd["CI_CCIA"].ToString(), // Añadido
                                    CI_CRAZON = rd["CI_CRAZON"].ToString() // Añadido
                                });
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // Manejo de errores
                throw new Exception("Error al obtener datos de la base de datos.", e);
            }

            return ciaAsignadaUsuarios;
        }


        public async Task<List<FWFTCABF>> ObtenerTotalCabeceraConfig(string UC_CCODCIA)
        {
            var FWFTCABF = new List<FWFTCABF>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT CF_CPROGRA, CF_CNOMOBJ, CF_CSECUEN, CF_CFLAHAB, " +
                                           "CF_CDESOBJ, CF_CLABEL, CF_CFLAVIS, CF_CDEFAUL, CF_CMODVIS " +
                                           "FROM FWFTCABF " +
                                           "WHERE CF_CCODCIA = '" + UC_CCODCIA + "';";

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWFTCABF.Add(new FWFTCABF()
                                {
                                    CF_CPROGRA = rd["CF_CPROGRA"].ToString(),
                                    CF_CNOMOBJ = rd["CF_CNOMOBJ"].ToString(),
                                    CF_CSECUEN = rd["CF_CSECUEN"].ToString(),
                                    CF_CFLAHAB = rd["CF_CFLAHAB"].ToString(),
                                    CF_CDEFAUL = rd["CF_CDEFAUL"].ToString(),
                                    CF_CDESOBJ = rd["CF_CDESOBJ"].ToString(),
                                    CF_CLABEL = rd["CF_CLABEL"].ToString(),
                                    CF_CFLAVIS = rd["CF_CFLAVIS"].ToString(),
                                    CF_CMODVIS = rd["CF_CMODVIS"].ToString()
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
            return FWFTCABF;
        }
        public async Task<List<FWUSERS>> ObtenerTotalUsuarios(string UC_CCODCIA)
        {
            var FWUSERS = new List<FWUSERS>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT USUA.US_CEMAIL,USUA.US_CNOMBRE, USUA.US_CCODAMB, USUA.US_CFACTIVO, USUA.US_CUSRFACCAR, USUA.US_CTIPUSU, " +
                                           "DATE_FORMAT(USUA.US_DFECLOGIN, '%d/%m/%Y') AS US_DFECLOGIN, USUA.US_CIDUSU, " +
                                           "USUA.US_CPASSW " +
                                           "FROM FWUSUCIA UCIA INNER JOIN FWUSERS USUA " +
                                           "ON UCIA.UC_CIDUSU = USUA.US_CIDUSU " +
                                           "WHERE UCIA.UC_CCODCIA = '" + UC_CCODCIA + "';";

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWUSERS.Add(new FWUSERS()
                                {
                                    US_CIDUSU = rd["US_CIDUSU"].ToString(),
                                    US_CPASSW = rd["US_CPASSW"].ToString(),
                                    US_CEMAIL = rd["US_CEMAIL"].ToString(),
                                    US_CNOMBRE = rd["US_CNOMBRE"].ToString(),
                                    US_CCODAMB = rd["US_CCODAMB"].ToString(),
                                    US_CFACTIVO = rd["US_CFACTIVO"].ToString(),
                                    US_CUSRFACCAR = rd["US_CUSRFACCAR"].ToString(),
                                    US_CTIPUSU = rd["US_CTIPUSU"].ToString(),
                                    US_DFECLOGIN = rd["US_DFECLOGIN"].ToString()
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
            return FWUSERS;
        }
        public async Task<List<Entities.FTAGEN>> ObtenerAgencias(string Id, string Cia)
        {
            var FTAGEN = new List<Entities.FTAGEN>();
            string tabla = "FT" + Cia + "AGEN";
            //string filtro = Regex.Replace(search, "[^a-zA-Z0-9]", "");
            string Query = @"SELECT AG_CCODAGE,  AG_CDESCRI " +
                            "FROM " + tabla + "";


            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(Id);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = Query;

                        await cn.OpenAsync();

                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FTAGEN.Add(new Entities.FTAGEN
                                {
                                    AG_CCODAGE = ConvertHelper.ToNonNullString(rd["AG_CCODAGE"]),
                                    AG_CDESCRI = ConvertHelper.ToNonNullString(rd["AG_CDESCRI"]),
                                });
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return FTAGEN;
        }

        public async Task<List<FWUSERS>> ObtenerTotalUsuariosNoAsociado(string searchTerm, string UC_CCODCIA)
        {
            var FWUSERS = new List<FWUSERS>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;

                        var query = @"SELECT USU.US_CIDUSU, USU.US_CEMAIL, USU.US_CNOMBRE 
                          FROM FWUSERS USU
                          WHERE 1 = 1"; // para facilitar concatenación de filtros

                        if (UC_CCODCIA == "00000000000")
                        {
                            query += " AND USU.US_CCODAMB <> 'CL'"; // Excluye los de CL
                        }
                        else
                        {
                            query += " AND USU.US_CCODAMB = 'CL'"; // Solo los de CL
                        }

                        if (!string.IsNullOrEmpty(searchTerm))
                        {
                            query += " AND (USU.US_CEMAIL LIKE @SearchTerm OR USU.US_CNOMBRE LIKE @SearchTerm)";
                            cmd.Parameters.AddWithValue("@SearchTerm", "%" + searchTerm + "%");
                        }

                        cmd.CommandText = query;

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                FWUSERS.Add(new FWUSERS()
                                {
                                    US_CIDUSU = rd["US_CIDUSU"].ToString(),
                                    US_CEMAIL = rd["US_CEMAIL"].ToString(),
                                    US_CNOMBRE = rd["US_CNOMBRE"].ToString()
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
            return FWUSERS;
        }

        public async Task<List<Entities.FWCNFG>> ObtenerTotalConfigUsuario(string US_CIDUSU, string US_CCODAMB)
        {
            var FWCNFG = new List<Entities.FWCNFG>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT CNFG.TG_CCLAVE, CNFG.TG_CDESCRI, " +
                                           "(SELECT COUNT(USUP.UP_CIDUSU) FROM FWUSUP USUP WHERE " +
                                           "USUP.UP_CPROG = CNFG.TG_CCLAVE AND USUP.UP_CIDUSU = '" + US_CIDUSU + "') AS EXISTENCIA " +
                                           "FROM FWCNFG CNFG WHERE " +
                                           "CNFG.TG_CVALOR = '" + US_CCODAMB + "' AND " +
                                           "CNFG.TG_CCOD = '06';";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWCNFG.Add(new Entities.FWCNFG()
                                {
                                    TG_CCLAVE = rd["TG_CCLAVE"].ToString(),
                                    TG_CDESCRI = rd["TG_CDESCRI"].ToString(),
                                    EXISTENCIA = Convert.ToInt32(rd["EXISTENCIA"])
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
            return FWCNFG;
        }
        public async Task<int> ValidarExistenciaCia(string CI_CID)
        {
            int Rpta = 0;
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT COUNT(CI_CID) FROM FWCIAS WHERE " +
                                           "CI_CID = '" + CI_CID + "';";
                        await cn.OpenAsync();
                        var Exec = await cmd.ExecuteScalarAsync();
                        Rpta = Convert.ToInt32(Exec);
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return Rpta;
        }
        //public async Task<Error> RegistrarCia(FWCIAS FWCIAS)
        //{
        //    var Error = new Error();
        //    FWCIAS.CI_CUUID = Guid.NewGuid().ToString();

        //    MySqlTransaction transaction = null;

        //    string queryInsert = @"INSERT INTO FWCIAS (" +
        //                           "CI_CID,CI_CIDCUENTA, CI_CRUC, CI_CCIA,CI_CUUID,CI_DFECVEN,CI_DFECVENCD,CI_CRAZON,CI_CFMODO,CI_CFACTIVO,CI_CFSYNC," +
        //                           "CI_CUSUCRE,CI_DFECCRE,CI_CUSUMOD,CI_DFECMOD,CI_CSWURL,CI_CSWUSU,CI_CSWPASS," +
        //                           "CI_CDBFACCAR,CI_CDBCONCAR,CI_CFDB,CI_CSOFTVER,CI_CRETVRE,CI_CDLLVER," +
        //                           "CI_CLOGOTKB64,CI_CLOGOA4B64,CI_CDBHASH,CI_CRKV,CI_CVERDB) VALUES (" +
        //                           "'" + FWCIAS.CI_CRUC + "'," +
        //                           "'" + FWCIAS.CI_CIDCUENTA + "'," +
        //                           "'" + FWCIAS.CI_CRUC + "'," +
        //                           "'" + FWCIAS.CI_CCIA + "'," +
        //                           "'" + FWCIAS.CI_CUUID + "'," +
        //                           "'" + FWCIAS.CI_DFECVEN + "'," +
        //                           "DATE_ADD(NOW(),INTERVAL 60 DAY)," +
        //                           "'" + FWCIAS.CI_CRAZON + "'," +
        //                           "'" + FWCIAS.CI_CFMODO + "'," +
        //                           "'" + FWCIAS.CI_CFACTIVO + "','N','0',NOW(),'0',NOW(),'','','','','','','','','','','','','','');";
        //    try
        //    {
        //        using (var cn = _MySqlDatabase.GetConnection())
        //        {
        //            await cn.OpenAsync();
        //            transaction = await cn.BeginTransactionAsync(IsolationLevel.ReadCommitted);
        //            using (MySqlCommand cmd = new MySqlCommand(queryInsert, cn, transaction)) { await cmd.ExecuteNonQueryAsync(); }
        //            await transaction.CommitAsync();
        //            Error.Status_code = "0";
        //            Error.Status_message = "Se registro con exito el RUC: " + FWCIAS.CI_CRUC;
        //            await cn.CloseAsync();
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Error.Status_code = "-1";
        //        Error.Status_message = e.Message;
        //        await transaction.RollbackAsync();
        //        throw e;
        //    }
        //    return Error;
        //}
        public async Task<Error> RegistrarCia(FWCIAS FWCIAS)
        {
            var Error = new Error();
            FWCIAS.CI_CUUID = Guid.NewGuid().ToString();

            MySqlTransaction transaction = null;

            string queryInsert = @"INSERT INTO FWCIAS (
                                CI_CID, CI_CIDCUENTA, CI_CRUC, CI_CCIA, CI_CUUID, CI_DFECVEN, CI_DFECVENCD, CI_CRAZON, CI_CFMODO, CI_CFACTIVO, CI_CFSYNC,
                                CI_CUSUCRE, CI_DFECCRE, CI_CUSUMOD, CI_DFECMOD, CI_CSWURL, CI_CSWUSU, CI_CSWPASS,
                                CI_CDBFACCAR, CI_CDBCONCAR, CI_CFDB, CI_CSOFTVER, CI_CRETVRE, CI_CDLLVER,
                                CI_CLOGOTKB64, CI_CLOGOA4B64, CI_CDBHASH, CI_CRKV, CI_CVERDB,
                                CI_CFTIPFIR, CI_CFCERTORI, CI_CPROVMOD, CI_CCODPROV, CI_CFSTOREFILE, CI_CFRKV, CI_CFAPI, CI_CFMONITOREO
                            ) VALUES (
                                @CI_CID, @CI_CIDCUENTA, @CI_CRUC, @CI_CCIA, @CI_CUUID, @CI_DFECVEN, DATE_ADD(NOW(), INTERVAL 60 DAY), @CI_CRAZON, @CI_CFMODO, @CI_CFACTIVO, 'N',
                                '0', NOW(), '0', NOW(), '', '', '',
                                '', '', '', '', '', '',
                                '', '', '', '', '',
                                @CI_CFTIPFIR, @CI_CFCERTORI, @CI_CPROVMOD, @CI_CCODPROV, @CI_CFSTOREFILE, @CI_CFRKV, @CI_CFAPI, @CI_CFMONITOREO
                            );";

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    await cn.OpenAsync();
                    transaction = await cn.BeginTransactionAsync(IsolationLevel.ReadCommitted);

                    using (MySqlCommand cmd = new MySqlCommand(queryInsert, cn, transaction))
                    {
                        cmd.Parameters.AddWithValue("@CI_CID", FWCIAS.CI_CRUC);
                        cmd.Parameters.AddWithValue("@CI_CIDCUENTA", FWCIAS.CI_CIDCUENTA);
                        cmd.Parameters.AddWithValue("@CI_CRUC", FWCIAS.CI_CRUC);
                        cmd.Parameters.AddWithValue("@CI_CCIA", FWCIAS.CI_CCIA);
                        cmd.Parameters.AddWithValue("@CI_CUUID", FWCIAS.CI_CUUID);
                        cmd.Parameters.AddWithValue("@CI_DFECVEN", FWCIAS.CI_DFECVEN);
                        cmd.Parameters.AddWithValue("@CI_CRAZON", FWCIAS.CI_CRAZON);
                        cmd.Parameters.AddWithValue("@CI_CFMODO", FWCIAS.CI_CFMODO);
                        cmd.Parameters.AddWithValue("@CI_CFACTIVO", FWCIAS.CI_CFACTIVO);

                        // Nuevos campos
                        cmd.Parameters.AddWithValue("@CI_CFTIPFIR", FWCIAS.CI_CFTIPFIR);
                        cmd.Parameters.AddWithValue("@CI_CFCERTORI", FWCIAS.CI_CFCERTORI);
                        cmd.Parameters.AddWithValue("@CI_CPROVMOD", FWCIAS.CI_CPROVMOD);
                        cmd.Parameters.AddWithValue("@CI_CCODPROV", FWCIAS.CI_CCODPROV);
                        cmd.Parameters.AddWithValue("@CI_CFSTOREFILE", FWCIAS.CI_CFSTOREFILE);
                        cmd.Parameters.AddWithValue("@CI_CFRKV", FWCIAS.CI_CFRKV);
                        cmd.Parameters.AddWithValue("@CI_CFAPI", FWCIAS.CI_CFAPI);
                        cmd.Parameters.AddWithValue("@CI_CFMONITOREO", FWCIAS.CI_CFMONITOREO);

                        await cmd.ExecuteNonQueryAsync();
                    }

                    await transaction.CommitAsync();
                    Error.Status_code = "0";
                    Error.Status_message = "Se registró con éxito el RUC: " + FWCIAS.CI_CRUC;
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                Error.Status_message = e.Message;
                await transaction?.RollbackAsync();
                throw new Exception(e.Message);
            }

            return Error;
        }

        //public async Task<Error> ActualizarCia(FWCIAS FWCIAS)
        //{
        //    var Error = new Error();
        //    MySqlTransaction transaction = null;

        //    string queryUpdate = @"UPDATE FWCIAS SET " +
        //                          "CI_CCIA = '" + FWCIAS.CI_CCIA + "', " +
        //                          "CI_CRUC = '" + FWCIAS.CI_CRUC + "', " +
        //                          "CI_CFACTIVO = '" + FWCIAS.CI_CFACTIVO + "', " +
        //                          "CI_CIDCUENTA = '" + FWCIAS.CI_CIDCUENTA + "', " +
        //                          "CI_CRAZON = '" + FWCIAS.CI_CRAZON + "', " +
        //                          "CI_CFMODO = '" + FWCIAS.CI_CFMODO + "', " +
        //                          //"CI_DFECVEN = '" + FWCIAS.CI_DFECVEN + "', " +
        //                          //"CI_CFDB = '" + FWCIAS.CI_CFDB + "', " +
        //                          //"CI_CDBFACCAR = '" + FWCIAS.CI_CDBFACCAR + "', " +
        //                          //"CI_CDBCONCAR = '" + FWCIAS.CI_CDBCONCAR + "', " +
        //                          //"CI_CSWURL = '" + FWCIAS.CI_CSWURL + "', " +
        //                          //"CI_CSWUSU = '" + FWCIAS.CI_CSWUSU + "', " +
        //                          //"CI_CSWPASS = '" + FWCIAS.CI_CSWPASS + "', " +
        //                          //"CI_CFSYNC = '" + FWCIAS.CI_CFSYNC + "', " +
        //                          //"CI_DFECVENCD = '" + FWCIAS.CI_DFECVEN + "', " +
        //                          //"CI_CFCERTORI = '" + FWCIAS.CI_CFCERTORI + "', " +
        //                          "CI_DFECVEN = '" + FWCIAS.CI_DFECVEN + "', " +
        //                          "CI_DFECMOD = NOW() " +
        //                          "WHERE " +
        //                          "CI_CID = '" + FWCIAS.KEY_RUC + "';";
        //    try
        //    {
        //        using (var cn = _MySqlDatabase.GetConnection())
        //        {
        //            await cn.OpenAsync();
        //            transaction = await cn.BeginTransactionAsync(IsolationLevel.ReadCommitted);
        //            using (MySqlCommand cmd = new MySqlCommand(queryUpdate, cn, transaction)) { await cmd.ExecuteNonQueryAsync(); }
        //            await transaction.CommitAsync();
        //            Error.Status_code = "0";
        //            Error.Status_message = "Se actualizo con exito el RUC: " + FWCIAS.KEY_RUC;
        //            await cn.CloseAsync();
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Error.Status_code = "-1";
        //        Error.Status_message = e.Message;
        //        await transaction.RollbackAsync();
        //        throw e;
        //    }
        //    return Error;
        //}
        public async Task<Error> ActualizarCia(FWCIAS FWCIAS)
        {
            var Error = new Error();
            MySqlTransaction transaction = null;

            string queryUpdate = @"UPDATE FWCIAS SET 
        CI_CCIA = '" + FWCIAS.CI_CCIA + @"', 
        CI_CRUC = '" + FWCIAS.CI_CRUC + @"', 
        CI_CFACTIVO = '" + FWCIAS.CI_CFACTIVO + @"', 
        CI_CIDCUENTA = '" + FWCIAS.CI_CIDCUENTA + @"', 
        CI_CRAZON = '" + FWCIAS.CI_CRAZON + @"', 
        CI_CFMODO = '" + FWCIAS.CI_CFMODO + @"', 
        CI_CFTIPFIR = '" + FWCIAS.CI_CFTIPFIR + @"', 
        CI_CFCERTORI = '" + FWCIAS.CI_CFCERTORI + @"', 
        CI_CPROVMOD = '" + FWCIAS.CI_CPROVMOD + @"', 
        CI_CCODPROV = '" + FWCIAS.CI_CCODPROV + @"', 
        CI_CFSTOREFILE = '" + FWCIAS.CI_CFSTOREFILE + @"', 
        CI_CFRKV = '" + FWCIAS.CI_CFRKV + @"', 
        CI_CFAPI = '" + FWCIAS.CI_CFAPI + @"', 
        CI_CFMONITOREO = '" + FWCIAS.CI_CFMONITOREO + @"', 
        CI_DFECVEN = '" + FWCIAS.CI_DFECVEN + @"', 
        CI_DFECMOD = NOW() 
        WHERE CI_CID = '" + FWCIAS.KEY_RUC + "';";

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    await cn.OpenAsync();
                    transaction = await cn.BeginTransactionAsync(IsolationLevel.ReadCommitted);

                    using (MySqlCommand cmd = new MySqlCommand(queryUpdate, cn, transaction))
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }

                    await transaction.CommitAsync();
                    Error.Status_code = "0";
                    Error.Status_message = "Se actualizó con éxito el RUC: " + FWCIAS.KEY_RUC;
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                Error.Status_message = e.Message;
                await transaction?.RollbackAsync();
                throw;
            }

            return Error;
        }

        public async Task<Error> ActualizarModod(FWCIAS FWCIAS)
        {
            var Error = new Error();
            MySqlTransaction transaction = null;

            string queryUpdate = @"UPDATE FWCIAS SET " +
                                  "CI_CFDEBUG = '" + FWCIAS.CI_CFDEBUG + "', " +
                                  "CI_DFECMOD = NOW() " +
                                  "WHERE " +
                                  "CI_CID = '" + FWCIAS.KEY_RUC + "';";
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    await cn.OpenAsync();
                    transaction = await cn.BeginTransactionAsync(IsolationLevel.ReadCommitted);
                    using (MySqlCommand cmd = new MySqlCommand(queryUpdate, cn, transaction)) { await cmd.ExecuteNonQueryAsync(); }
                    await transaction.CommitAsync();
                    Error.Status_code = "0";
                    Error.Status_message = "Se actualizo con exito el RUC: " + FWCIAS.KEY_RUC;
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                Error.Status_message = e.Message;
                await transaction.RollbackAsync();
                throw e;
            }
            return Error;
        }
        public async Task<Error> UsuarioCrear(FWUSERS FWUSERS)
        {
            var Error = new Error();
            MySqlTransaction transaction = null;

            string queryInsert = "INSERT INTO FWUSERS (US_CIDUSU,US_CEMAIL,US_CPASSW,US_CUUID,US_CCODAMB,US_NCONDBL,US_CTIPUSU,US_CFACTIVO,US_DFECLOGIN,US_CUSRFACCAR,US_CUSUCRE,US_DFECCRE,US_CUSUMOD,US_DFECMOD,US_CNOMBRE) " +
                                                " VALUES(@US_CIDUSU,@US_CEMAIL,@US_CPASSW,@US_CUUID,@US_CCODAMB,0,@US_CTIPUSU,@US_CFACTIVO,@US_DFECLOGIN,@US_CUSRFACCAR,@US_CUSUCRE,NOW(),@US_CUSUCRE,NOW(),@US_CNOMBRE)";

            try
            {
                string maxId = "1";
                maxId = (await UsuarioMaxId() + 1).ToString();

                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();
                        transaction = cn.BeginTransaction(IsolationLevel.ReadCommitted);
                        cmd.Transaction = transaction;
                        cmd.CommandText = @"" + queryInsert;
                        cmd.Parameters.AddWithValue("@US_CIDUSU", maxId);
                        cmd.Parameters.AddWithValue("@US_CEMAIL", FWUSERS.US_CEMAIL);
                        cmd.Parameters.AddWithValue("@US_CPASSW", Security.Encrypt(FWUSERS.US_CPASSW));
                        cmd.Parameters.AddWithValue("@US_CCODAMB", FWUSERS.US_CCODAMB);
                        cmd.Parameters.AddWithValue("@US_CUUID", FWUSERS.US_CUUID);
                        cmd.Parameters.AddWithValue("@US_CTIPUSU", FWUSERS.US_CTIPUSU);
                        cmd.Parameters.AddWithValue("@US_CFACTIVO", FWUSERS.US_CFACTIVO);
                        cmd.Parameters.AddWithValue("@US_DFECLOGIN", FWUSERS.US_DFECLOGIN);
                        cmd.Parameters.AddWithValue("@US_CUSRFACCAR", FWUSERS.US_CUSRFACCAR);
                        cmd.Parameters.AddWithValue("@US_CUSUCRE", FWUSERS.US_CUSUCRE);
                        cmd.Parameters.AddWithValue("@US_CNOMBRE", FWUSERS.US_CNOMBRE);
                        cmd.ExecuteNonQuery();
                        await transaction.CommitAsync();
                        Error.Status_code = "0";
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                await transaction.RollbackAsync();
            }
            return Error;
        }
        public async Task<Error> UsuarioActualizar(FWUSERS FWUSERS)
        {
            var Error = new Error();
            MySqlTransaction transaction = null;

            string queryInsert = "UPDATE FWUSERS SET US_CPASSW = @US_CPASSW, US_CUSUMOD = @US_CUSUMOD, US_CCODAMB = @US_CCODAMB,US_CTIPUSU = @US_CTIPUSU, US_CFACTIVO = @US_CFACTIVO, US_CUSRFACCAR = @US_CUSRFACCAR, US_DFECMOD = NOW(), US_CNOMBRE = @US_CNOMBRE " +
                                                " WHERE US_CIDUSU = @US_CIDUSU";

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();
                        transaction = cn.BeginTransaction(IsolationLevel.ReadCommitted);
                        cmd.Transaction = transaction;
                        cmd.CommandText = @"" + queryInsert;
                        cmd.Parameters.AddWithValue("@US_CIDUSU", FWUSERS.US_CIDUSU);
                        //cmd.Parameters.AddWithValue("@US_CEMAIL", FWUSERS.US_CEMAIL);
                        cmd.Parameters.AddWithValue("@US_CPASSW", Security.Encrypt(FWUSERS.US_CPASSW));
                        cmd.Parameters.AddWithValue("@US_CUSUMOD", FWUSERS.US_CUSUMOD);
                        cmd.Parameters.AddWithValue("@US_CNOMBRE", FWUSERS.US_CNOMBRE);
                        cmd.Parameters.AddWithValue("@US_CCODAMB", FWUSERS.US_CCODAMB);
                        cmd.Parameters.AddWithValue("@US_CTIPUSU", FWUSERS.US_CTIPUSU);
                        cmd.Parameters.AddWithValue("@US_CFACTIVO", FWUSERS.US_CFACTIVO);
                        cmd.Parameters.AddWithValue("@US_CUSRFACCAR", FWUSERS.US_CUSRFACCAR);
                        cmd.ExecuteNonQuery();
                        await transaction.CommitAsync();
                        Error.Status_code = "0";
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                await transaction.RollbackAsync();
            }
            return Error;
        }
        public async Task<int> UsuarioMaxId()
        {
            int MaxId = 1;
            var select = "SELECT US_CIDUSU FROM FWUSERS ORDER BY CAST(US_CIDUSU AS SIGNED) DESC LIMIT 1";
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"" + select;
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                MaxId = Convert.ToInt32(rd["US_CIDUSU"]);
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return MaxId;
        }
        public async Task<bool> UsuarioEmailRepetido(FWUSERS FWUSERS)
        {
            bool existe = false;
            var select = "SELECT US_CIDUSU FROM FWUSERS WHERE US_CEMAIL='" + FWUSERS.US_CEMAIL.Trim() + "'";
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"" + select;
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                existe = true;
                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return existe;
        }
        public async Task<FWUSERS_LISTA_PAGINACION> UsuarioListar(FWUSERS_LISTA_REQUEST FWUSERS_LISTA_REQUEST)
        {
            var condicion = "";
            List<FWUSERS_LISTA> FWUSERS_LISTA = new List<FWUSERS_LISTA>();
            var FWUSERS_LISTA_PAGINACION = new FWUSERS_LISTA_PAGINACION();

            var count = "COUNT(US_CIDUSU) AS TOTALES ";
            var Tabla = "FWUSERS";
            var select = " US_CIDUSU,US_CEMAIL,US_CPASSW,US_CCODAMB,US_CFACTIVO,US_CUSRFACCAR,US_DFECLOGIN,US_CUSUCRE,US_DFECCRE,US_CUSUMOD,US_DFECMOD,US_CNOMBRE ";
            var from = "FROM " + Tabla + " AS U ";

            var join = "";
            if (FWUSERS_LISTA_REQUEST.Asignado)
            {
                join = " LEFT OUTER JOIN FWUSUCIA C ON  U.US_CIDUSU = C.UC_CIDUSU";
            }

            if (!string.IsNullOrEmpty(FWUSERS_LISTA_REQUEST.US_CEMAIL))
                condicion += " WHERE U.US_CEMAIL LIKE '%" + FWUSERS_LISTA_REQUEST.US_CEMAIL.Trim() + "%'";

            if (!string.IsNullOrEmpty(FWUSERS_LISTA_REQUEST.US_CNOMBRE))
                condicion += (condicion.Contains("WHERE") ? " AND " : " WHERE ") + " U.US_CNOMBRE LIKE '%" + FWUSERS_LISTA_REQUEST.US_CNOMBRE.Trim() + "%' ";

            if (!string.IsNullOrEmpty(FWUSERS_LISTA_REQUEST.US_CFACTIVO))
                condicion += (condicion.Contains("WHERE") ? " AND " : " WHERE ") + " U.US_CFACTIVO = '" + FWUSERS_LISTA_REQUEST.US_CFACTIVO.Trim() + "' ";

            condicion += (condicion.Contains("WHERE") ? " AND " : " WHERE ") + " U.US_CIDUSU != '0' ";


            if (FWUSERS_LISTA_REQUEST.Asignado)
                condicion += (condicion.Contains("WHERE") ? " AND " : " WHERE ") + " C.UC_CIDUSU IS NULL ";

            var skipreg = (FWUSERS_LISTA_REQUEST.Total_paginas * FWUSERS_LISTA_REQUEST.Pagina_actual) - FWUSERS_LISTA_REQUEST.Total_paginas;
            var paginacion = "Limit " + skipreg + "," + FWUSERS_LISTA_REQUEST.Total_paginas + ";";
            var query = select + from + join + condicion + " ORDER BY U.US_CIDUSU " + paginacion;
            var totalquery = count + from + join + condicion + ";";

            try
            {
                var Total_resultados = 0;
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT " + query + " " +
                                           "SELECT " + totalquery;
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync())
                            {
                                skipreg = skipreg + 1;
                                string password = Security.Decrypt(rd["US_CPASSW"].ToString());
                                FWUSERS_LISTA.Add(new FWUSERS_LISTA()
                                {
                                    ROWNUMBER = skipreg,
                                    US_CIDUSU = rd["US_CIDUSU"].ToString(),
                                    US_CEMAIL = rd["US_CEMAIL"].ToString(),
                                    US_CPASSW = password, //rd["US_CPASSW"].ToString(),
                                    US_CCODAMB = rd["US_CCODAMB"].ToString(),
                                    US_CFACTIVO = rd["US_CFACTIVO"].ToString(),
                                    US_CUSRFACCAR = rd["US_CUSRFACCAR"].ToString(),
                                    US_DFECLOGIN = rd["US_DFECLOGIN"].ToString(),
                                    US_CUSUCRE = rd["US_CUSUCRE"].ToString(),
                                    US_DFECCRE = Convert.ToDateTime(rd["US_DFECCRE"]).ToString("dd/MM/yyyy HH:mm:ss"),
                                    US_CUSUMOD = rd["US_CUSUMOD"].ToString(),
                                    US_DFECMOD = Convert.ToDateTime(rd["US_DFECMOD"]).ToString("dd/MM/yyyy HH:mm:ss"),
                                    US_CNOMBRE = rd["US_CNOMBRE"].ToString()
                                });
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
                    FWUSERS_LISTA_PAGINACION.FWUSERS_LISTA = FWUSERS_LISTA;
                    FWUSERS_LISTA_PAGINACION.Total_resultados = Total_resultados;
                    FWUSERS_LISTA_PAGINACION.Total_paginas = FWUSERS_LISTA_REQUEST.Total_paginas;
                    FWUSERS_LISTA_PAGINACION.Pagina_actual = FWUSERS_LISTA_REQUEST.Pagina_actual;
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return FWUSERS_LISTA_PAGINACION;
        }

        public async Task<List<FWUSERS>> ObtenerUsuariosActivos(string RUC)
        {
            var FWCIAS_LOGIN = new List<FWUSERS>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT U.US_CIDUSU,U.US_CNOMBRE,U.US_CEMAIL,U.US_CCODAMB,U.US_CUSRFACCAR,C.UC_CCODCIA,US_CTIPUSU, US_CFACTIVO "+
                                            "FROM FWUSERS U  "+
                                            "INNER JOIN FWUSUCIA C ON U.US_CIDUSU=C.UC_CIDUSU "+
                                            "WHERE C.UC_CCODCIA='"+RUC+"' "+
                                            "AND U.US_CCODAMB='CL' ORDER BY U.US_CNOMBRE; ";
                         await cn.OpenAsync();
                            using (var rd = await cmd.ExecuteReaderAsync())
                            {
                                while (rd.Read())
                                {
                                    FWCIAS_LOGIN.Add(new FWUSERS()
                                    {
                                        US_CIDUSU = rd["US_CIDUSU"].ToString(),
                                        US_CNOMBRE = rd["US_CNOMBRE"].ToString(),
                                        US_CEMAIL = rd["US_CEMAIL"].ToString(),
                                        US_CCODAMB = rd["US_CCODAMB"].ToString(),
                                        US_CUSRFACCAR = rd["US_CUSRFACCAR"].ToString(),
                                        UC_CCODCIA = rd["UC_CCODCIA"].ToString(),
                                        US_CTIPUSU = rd["US_CTIPUSU"].ToString(),
                                        US_CFACTIVO = rd["US_CFACTIVO"].ToString()
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
            return FWCIAS_LOGIN;
        }

        //Impresion / Logo

        public async Task<bool> Actualizar(FWCIAS impresion)
        {
            bool Execute = false;
            string tabla = "FWCIAS";
            impresion.CI_CLOGOTKB64 = impresion.CI_CLOGOTKB64.Replace(" id=\"imgPreveia\"", "");
            string Query = "UPDATE " + tabla + " SET CI_CLOGOTKB64 = '" + impresion.CI_CLOGOTKB64 + "' WHERE CI_CID = '" + impresion.CI_CID + "';";
            //string Query = "UPDATE " + tabla + " SET CI_CLOGOTKB64 = @CI_CLOGOTKB64 WHERE CI_CID = @CI_CID;";


            try
            {
                using var cn = _MySqlDatabase.GetConnection();
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = Query;
                    //cmd.Parameters.Add("@CI_CID", MySqlDbType.VarChar).Value = impresion.CI_CID;
                    //cmd.Parameters.Add("@CI_CLOGOTKB64", MySqlDbType.VarChar).Value = impresion.CI_CLOGOTKB64;

                    await cn.OpenAsync();
                    int i = await cmd.ExecuteNonQueryAsync();
                    Execute = i.Equals(1);
                }
                await cn.CloseAsync();
            }
            catch (Exception e)
            {
                throw new ArgumentException("Insertar: " + e.Message);
            }

            return Execute;
        }

        public Task<bool> Eliminar(FWCIAS agencia)
        {
            throw new NotImplementedException();
        }

        public async Task<FWCIAS> GetLista(FWCIAS RUCEMP)
        {
            FWCIAS Lista = new FWCIAS();
            string tabla = "FWCIAS";
            string Query = "SELECT CI_CRUC,CI_CRAZON,CI_CLOGOTKB64 FROM " + tabla + " WHERE CI_CID = '" + RUCEMP.CI_CID + "'";
            try
            {
                using var cn = _MySqlDatabase.GetConnection();
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = Query;
                    await cn.OpenAsync();
                    using var rd = await cmd.ExecuteReaderAsync();
                    while (await rd.ReadAsync())
                    {
                        Lista.CI_CRUC = rd["CI_CRUC"].ToString().Trim();
                        Lista.CI_CRAZON = rd["CI_CRAZON"].ToString().Trim();
                        Lista.CI_CLOGOTKB64 = rd["CI_CLOGOTKB64"].ToString().Trim();
                    }
                    await rd.CloseAsync();
                }
                await cn.CloseAsync();
            }
            catch (Exception e)
            {
                throw new ArgumentException("GetLista: " + e.Message);
            }

            return Lista;
        }

        public async Task<bool> Insertar(FWCIAS impresion)
        {
            bool Execute = false;
            string tabla = "FWCIAS";
            string Query = "";

            Query = "INSERT INTO " + tabla + " (CI_CLOGOTKB64) " +
                       "VALUES(@CI_CLOGOTKB64);";

            try
            {
                using var cn = _MySqlDatabase.GetConnection();
                using (var cmd = cn.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = Query;
                    cmd.Parameters.Add("@CI_CLOGOTKB64", MySqlDbType.VarChar).Value = impresion.CI_CLOGOTKB64;
                    cmd.Parameters.Add("@CI_CLOGOTKB64", MySqlDbType.VarChar).Value = impresion.CI_CLOGOTKB64;
                    await cn.OpenAsync();
                    int i = await cmd.ExecuteNonQueryAsync();
                    Execute = i.Equals(1);
                }
                await cn.CloseAsync();
            }
            catch (Exception e)
            {
                throw new ArgumentException("Insertar: " + e.Message);
            }

            return Execute;
        }
        public async Task<Pfx> ObtenerPfx(string Id, string Cia)
        {
            Pfx pfx = new Pfx();
            string FETABL = $"FE{Cia}TABL";
            string queryCertificado = $"SELECT (SELECT TG_CDESCRI FROM {FETABL} WHERE TG_CCOD = '03' AND TG_CCLAVE = 'CERT_DESDE') as CERT_DESDE," +
                $" (SELECT TG_CDESCRI FROM {FETABL} WHERE TG_CCOD = '03' AND TG_CCLAVE = 'CERT_HASTA') as CERT_HASTA, " +
                $" (SELECT TG_CDESCRI FROM {FETABL} WHERE TG_CCOD = '03' AND TG_CCLAVE = 'CERT_ID') as CERT_ID," +
                $" (SELECT TG_CDESCRI FROM {FETABL} WHERE TG_CCOD = '03' AND TG_CCLAVE = 'CERT_RUC') as CERT_RUC," +
                $" (SELECT TG_CDESCRI FROM {FETABL} WHERE TG_CCOD = '03' AND TG_CCLAVE = 'CERT_SUJETO') as CERT_SUJETO FROM {FETABL} LIMIT 1";

            try
            {


                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(Id);

                using (var cn = new MySqlConnection(nuevaConexion))
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = queryCertificado;
                        await cn.OpenAsync();
                        using var rd = await cmd.ExecuteReaderAsync();
                        if (await rd.ReadAsync())
                        {

                            pfx.Serie = ConvertHelper.ToNonNullString(rd["CERT_ID"]);
                            pfx.FechaDesde = ConvertHelper.ToNonNullString(rd["CERT_DESDE"]);
                            pfx.FechaHasta = ConvertHelper.ToNonNullString(rd["CERT_HASTA"]);
                            pfx.Ruc = ConvertHelper.ToNonNullString(rd["CERT_RUC"]);
                            pfx.Sujeto = ConvertHelper.ToNonNullString(rd["CERT_SUJETO"]);

                        }
                        await rd.CloseAsync();
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error al obtener el certificado Pfx: " + ex.Message);
            }

            return pfx;
        }
        public FWCIAS UUIDPfx(FWCIAS entidad)
        {
            FWCIAS cia = new FWCIAS(); // Inicializamos uuid como cadena vacía
            var query = "SELECT CI_CUUID, CI_CID FROM FWCIAS WHERE CI_CID ='" + entidad.CI_CID.Trim() + "' LIMIT 1;";
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = query; // No es necesario concatenar el string de la consulta
                        cn.Open();
                        using (var rd = cmd.ExecuteReader())
                        {
                            while (rd.Read())
                            {
                                // Asignamos el valor leído a la variable uuid
                                cia.CI_CUUID = rd["CI_CUUID"].ToString();
                                cia.CI_CID = rd["CI_CID"].ToString();

                            }
                            rd.Close();
                        }
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return cia;
        }

        public async Task<List<REPORAPIDOS>> GetListReporteRapidos(string ruc, string cia, string pcuser)
        {
            List<REPORAPIDOS> Lista = new List<REPORAPIDOS>();
            string Query = "SELECT F.FB_ALIAS, F.FB_CCLAVE, F.FB_CCOD, F.FB_CFVISIBLE, FB_CFACTIVO, F.FB_NPOSIC, T.TG_CDESCRI, U.TU_NOMUSU " +
                           "FROM FW" + cia + "FAVO F " +
                           "INNER JOIN FW" + cia + "TABL T ON F.FB_CCLAVE = T.TG_CCLAVE " +
                           "INNER JOIN UT0030 U ON U.TU_ALIAS = F.FB_ALIAS " +
                           "WHERE F.FB_ALIAS = '" + pcuser + "' AND F.FB_CCOD = '02' " +
                           "ORDER BY F.FB_NPOSIC";

            try
            {
                // Obtener nueva cadena de conexión con el ID proporcionado
                string nuevaConexion = await ObtenerCadenaConexion(ruc);

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
                            Lista.Add(new REPORAPIDOS()
                            {
                                FB_ALIAS = rd["FB_ALIAS"].ToString().Trim(),
                                FB_CCLAVE = rd["FB_CCLAVE"].ToString().Trim(),
                                FB_CCOD = rd["FB_CCOD"].ToString().Trim(),
                                FB_CFVISIBLE = rd["FB_CFVISIBLE"].ToString().Trim(),
                                FB_CFACTIVO = rd["FB_CFACTIVO"].ToString().Trim(),
                                FB_NPOSIC = rd["FB_NPOSIC"].ToString().Trim(),
                                TG_CDESCRI = rd["TG_CDESCRI"].ToString().Trim(),
                                TU_NOMUSU = rd["TU_NOMUSU"].ToString().Trim(),
                            });
                        }
                        await rd.CloseAsync();
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException("GetLista: " + e.Message);
            }

            return Lista;
        }

        public async Task<List<FWCIAS>> GetCertificados()
        {
            var FWCIASList = new List<FWCIAS>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT CI_CCIA, CI_CRUC, CI_CRAZON " +
                                           "FROM FWCIAS " +
                                           "WHERE CI_CRUC <> '00000000000' AND CI_CFACTIVO = 'S'";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWCIASList.Add(new FWCIAS()
                                {
                                    CI_CCIA = rd["CI_CCIA"].ToString().Trim(),
                                    CI_CRUC = rd["CI_CRUC"].ToString().Trim(),
                                    CI_CRAZON = rd["CI_CRAZON"].ToString().Trim(),
                                });
                            }
                            rd.Close();
                        }
                    }
                    await cn.CloseAsync();
                }

                // Segunda consulta usando otra conexión para cada compañía
                foreach (var item in FWCIASList.Where(c => c.CI_CRUC != "00000000000"))
                {
                    try
                    {
                        // Obtener nueva cadena de conexión con el ID proporcionado
                        string nuevaConexion = await ObtenerCadenaConexion(item.CI_CRUC);

                        using (var cn2 = new MySqlConnection(nuevaConexion))
                        {
                            using (var cmd2 = cn2.CreateCommand())
                            {
                                cmd2.CommandType = CommandType.Text;
                                cmd2.CommandText = @"SELECT AC_DFECVENCD FROM ALCIAS WHERE AC_CRUC = @RUC";
                                cmd2.Parameters.AddWithValue("@RUC", item.CI_CRUC);
                                await cn2.OpenAsync();
                                using (var rd2 = await cmd2.ExecuteReaderAsync())
                                {
                                    while (rd2.Read())
                                    {
                                        // Supongamos que queremos almacenar este dato en una propiedad nueva
                                        item.CI_DFECVENCD = rd2["AC_DFECVENCD"].ToString().Trim();
                                    }
                                    rd2.Close();
                                }
                            }
                            await cn2.CloseAsync();
                        }
                    }
                    catch (Exception)
                    {
                        // Si hay un error en la conexión, se mantiene OtroCampo como vacío
                        item.CI_DFECVENCD = "";
                    }
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return FWCIASList;
        }
        public async Task<List<FWCIAPLAN>> ListarCiaPlan(string ruc)
        {
            var planes = new List<FWCIAPLAN>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    await cn.OpenAsync();

                    // Obtener las compañías y sus planes
                    using (var cmdCiaPlan = cn.CreateCommand())
                    {
                        cmdCiaPlan.CommandText = @"
                                                SELECT 
                                                    C.CC_NID, 
                                                    C.CC_CRUC, 
                                                    C.CC_CCIA, 
                                                    C.CC_CPLAN, 
                                                    C.CC_CFVIGENTE,
                                                    L.CL_NID, 
                                                    L.CL_CRUC, 
                                                    L.CL_CCIA, 
                                                    L.CL_CVARIABLE, 
                                                    L.CL_NLIMITE, 
                                                    L.CL_NUSADO, 
                                                    L.CL_DFECPERI, 
                                                    P.PC_CDESCRI 
                                                FROM FWCIAPLAN C
                                                LEFT JOIN FWCIAPLANLIM L 
                                                    ON C.CC_CRUC = L.CL_CRUC 
                                                    AND C.CC_CCIA = L.CL_CCIA 
                                                    AND C.CC_NID = L.CL_NID
                                                LEFT JOIN FWPLANC P 
                                                    ON C.CC_CPLAN = P.PC_CPLAN
                                                WHERE C.CC_CRUC = @Ruc
                                                ORDER BY C.CC_NID ASC";

                        // Definir el parámetro @Ruc
                        cmdCiaPlan.Parameters.AddWithValue("@Ruc", ruc);


                        using (var rd = await cmdCiaPlan.ExecuteReaderAsync())
                        {
                            var lookup = new Dictionary<int, FWCIAPLAN>();

                            while (rd.Read())
                            {
                                int id = Convert.ToInt32(rd["CC_NID"]);

                                if (!lookup.TryGetValue(id, out var existingPlan))
                                {
                                    existingPlan = new FWCIAPLAN()
                                    {
                                        CC_NID = id,
                                        CC_CRUC = rd["CC_CRUC"].ToString().Trim(),
                                        CC_CCIA = rd["CC_CCIA"].ToString().Trim(),
                                        CC_CPLAN = rd["CC_CPLAN"].ToString().Trim(),
                                        CC_CFVIGENTE = rd.IsDBNull(rd.GetOrdinal("CC_CFVIGENTE"))
                                                        ? null
                                                        : rd["CC_CFVIGENTE"].ToString().Trim(),
                                        CC_CDESCRI = rd["PC_CDESCRI"].ToString().Trim(),// Guardar como string
                                        LIMITES = new List<FWCIAPLANLIM>()
                                    };
                                    lookup.Add(id, existingPlan);
                                }

                                if (!rd.IsDBNull(rd.GetOrdinal("CL_NID")))
                                {
                                    existingPlan.LIMITES.Add(new FWCIAPLANLIM()
                                    {
                                        CL_NID = Convert.ToInt32(rd["CL_NID"]),
                                        CL_CRUC = rd["CL_CRUC"].ToString().Trim(),
                                        CL_CCIA = rd["CL_CCIA"].ToString().Trim(),
                                        CL_CVARIABLE = rd["CL_CVARIABLE"].ToString().Trim(),
                                        CL_NLIMITE = rd.IsDBNull(rd.GetOrdinal("CL_NLIMITE")) ? (int?)null : Convert.ToInt32(rd["CL_NLIMITE"]),
                                        CL_NUSADO = rd.IsDBNull(rd.GetOrdinal("CL_NUSADO")) ? (int?)null : Convert.ToInt32(rd["CL_NUSADO"]),
                                        CL_DFECPERI = rd.IsDBNull(rd.GetOrdinal("CL_DFECPERI"))
                                                      ? null
                                                      : rd["CL_DFECPERI"].ToString().Trim()  // Guardar como string
                                    });
                                }

                            }

                            planes = new List<FWCIAPLAN>(lookup.Values);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error al listar compañías y planes", e);
            }

            return planes;
        }

        public async Task<List<Entities.FWCNFG>> ListarModuloPlan(string plan)
        {
            var FWCNFG = new List<Entities.FWCNFG>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    await cn.OpenAsync(); // Abre la conexión antes de crear el comando

                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT 
                                        C.TG_CCLAVE, 
                                        C.TG_CDESCRI,
                                        CASE 
                                            WHEN P.PD_CMODULO IS NOT NULL THEN 'S' 
                                            ELSE 'N' 
                                        END AS PERMISO
                                    FROM FWCNFG C
                                    LEFT JOIN FWPLAND P 
                                        ON C.TG_CCLAVE = P.PD_CMODULO
                                        AND P.PD_CPLAN = @PlanCodigo
                                    WHERE C.TG_CCOD = '06' 
                                    AND C.TG_CCLAVE LIKE 'CL_%';";

                        // Agregar el parámetro correctamente
                        cmd.Parameters.AddWithValue("@PlanCodigo", plan);

                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (await rd.ReadAsync()) // Usar ReadAsync para mejor rendimiento
                            {
                                FWCNFG.Add(new Entities.FWCNFG()
                                {
                                    TG_CCLAVE = rd["TG_CCLAVE"].ToString().Trim(),
                                    TG_CDESCRI = rd["TG_CDESCRI"].ToString().Trim(),
                                    PERMISO = rd["PERMISO"].ToString().Trim(),
                                });
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error al listar módulos del plan", e);
            }

            return FWCNFG;
        }

        public async Task<bool> AsociarPlan(REQUEST_CIA_PLANES request)
        {
            using (var cn = _MySqlDatabase.GetConnection())
            {
                await cn.OpenAsync(); // Abre la conexión
                using (var transaction = cn.BeginTransaction())
                {
                    try
                    {
                        var nuevoPlanId = await InsertarNuevoPlan(request, cn, transaction);

                        // 3. Insertar las limitaciones en FWCIAPLANLIM para el nuevo plan
                        await InsertarPlanLimitaciones(request, nuevoPlanId, cn, transaction);

                        await ActualizarFechaCia(request, cn, transaction);

                        transaction.Commit();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        // Maneja o registra el error según corresponda
                        throw new Exception("Error al cambiar el plan: " + ex.Message, ex);
                    }
                }
            }
        }

        public async Task<bool> CambiarPlan(REQUEST_CIA_PLANES request)
        {
            using (var cn = _MySqlDatabase.GetConnection())
            {
                await cn.OpenAsync(); // Abre la conexión
                using (var transaction = cn.BeginTransaction())
                {
                    try
                    {
                        // 1. Actualizar el plan actual en FWCIAPLAN (ponerlo en no vigente)
                        await ActualizarPlanActual(request, cn, transaction);

                        // 2. Insertar el nuevo plan en FWCIAPLAN
                        var nuevoPlanId = await InsertarNuevoPlan(request, cn, transaction);

                        // 3. Insertar las limitaciones en FWCIAPLANLIM para el nuevo plan
                        await InsertarPlanLimitaciones(request, nuevoPlanId, cn, transaction);

                        await ActualizarFechaCia(request, cn, transaction);

                        transaction.Commit();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        // Maneja o registra el error según corresponda
                        throw new Exception("Error al cambiar el plan: " + ex.Message, ex);
                    }
                }
            }
        }


        // Método privado para actualizar el plan actual a 'N' (no vigente)
        private async Task ActualizarPlanActual(REQUEST_CIA_PLANES request, MySqlConnection cn, MySqlTransaction transaction)
        {
            string query = @"
                            UPDATE FWCIAPLAN
                            SET CC_CFVIGENTE = 'N', CC_CUSUMOD = @Usuario
                            WHERE CC_CPLAN = @PlanActual AND CC_CRUC = @Ruc";

            try
            {
                using (var cmd = cn.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query;
                    cmd.Parameters.AddWithValue("@PlanActual", request.PlanActual);
                    cmd.Parameters.AddWithValue("@Ruc", request.CC_CRUC);
                    cmd.Parameters.AddWithValue("@Usuario", request.CC_CUSUCRE);
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                // Aquí puedes registrar el error o manejarlo de la manera que necesites
                throw new Exception("Error al actualizar el plan actual: " + ex.Message, ex);
            }
        }

        private async Task ActualizarFechaCia(REQUEST_CIA_PLANES request, MySqlConnection cn, MySqlTransaction transaction)
        {
            string query = @"
                            UPDATE FWCIAS
                            SET CI_DFECVEN = @Fechafin
                            WHERE CI_CID = @Ruc";

            try
            {
                using (var cmd = cn.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query;
                    cmd.Parameters.AddWithValue("@Ruc", request.CC_CRUC);
                    cmd.Parameters.AddWithValue("@Fechafin", request.Fchaven);

                    // Ejecutar la actualización
                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                // Aquí puedes registrar el error o manejarlo según sea necesario
                Console.WriteLine($"Error al actualizar la fecha de la compañía: {ex.Message}");
                throw;  // Opcional, si quieres volver a lanzar la excepción después de manejarla
            }
        }

        // Método privado para insertar el nuevo plan
        // Retornamos el identificador del nuevo plan (por ejemplo, CC_CPLAN)
        private async Task<string> InsertarNuevoPlan(REQUEST_CIA_PLANES request, MySqlConnection cn, MySqlTransaction transaction)
        {
            string query = @"
        INSERT INTO FWCIAPLAN (CC_CPLAN, CC_CFVIGENTE, CC_CFCICLOFACT, CC_DFECINI, CC_DFECFIN, CC_CRUC, CC_CCIA, CC_CUSUCRE, CC_DFECCRE, CC_CUSUMOD, CC_DFECMOD)
        VALUES (@NuevoPlan, 'S', @CicloFac, @FechaInicio, @FechaFin, @Cruc, @Ccia, @Usuario, NOW(), @Usuario , NOW());
        SELECT LAST_INSERT_ID();"; 

            try
            {
                using (var cmd = cn.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = query;
                    cmd.Parameters.AddWithValue("@NuevoPlan", request.NuevoPlan);
                    cmd.Parameters.AddWithValue("@CicloFac", request.CicloFac);
                    cmd.Parameters.AddWithValue("@FechaInicio", DateTime.Parse(request.Fchaini));
                    cmd.Parameters.AddWithValue("@FechaFin", DateTime.Parse(request.Fchaven));
                    cmd.Parameters.AddWithValue("@Cruc", request.CC_CRUC);
                    cmd.Parameters.AddWithValue("@Usuario", request.CC_CUSUCRE);
                    cmd.Parameters.AddWithValue("@Ccia", request.CC_CCIA);

                    // Si usas LAST_INSERT_ID() y la clave es auto incremental:
                    var result = await cmd.ExecuteScalarAsync();
                    return result?.ToString() ?? request.NuevoPlan;
                }
            }
            catch (Exception ex)
            {
                // Manejo de excepciones
                Console.WriteLine($"Error al insertar el nuevo plan: {ex.Message}");
                throw;  // Re-lanzar la excepción para manejarla más arriba si es necesario
            }
        }

        // Método privado para insertar las limitaciones en FWCIAPLANLIM
        private async Task InsertarPlanLimitaciones(REQUEST_CIA_PLANES request, string nuevoPlanId, MySqlConnection cn, MySqlTransaction transaction)
        {
            string sqlGetCampos = "SHOW FULL FIELDS FROM FWPLANC WHERE FIELD LIKE 'PC_NL%'";
            var campos = new List<CampoFWPLANC>();

            try
            {
                // Obtener los campos de FWPLANC
                using (var cmd = cn.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = sqlGetCampos;
                    using (var rd = await cmd.ExecuteReaderAsync())
                    {
                        while (await rd.ReadAsync())
                        {
                            campos.Add(new CampoFWPLANC
                            {
                                FIELD = rd["Field"].ToString().Trim()
                            });
                        }
                    }
                }

                // Para cada campo, obtener su valor actual en FWPLANC
                foreach (var campo in campos)
                {
                    string sqlSelectValor = $"SELECT {campo.FIELD} FROM FWPLANC WHERE PC_CPLAN = @PlanCodigo LIMIT 1";
                    string valor = null;

                    try
                    {
                        using (var cmdValor = cn.CreateCommand())
                        {
                            cmdValor.Transaction = transaction;
                            cmdValor.CommandText = sqlSelectValor;
                            cmdValor.Parameters.AddWithValue("@PlanCodigo", request.NuevoPlan);
                            object result = await cmdValor.ExecuteScalarAsync();
                            valor = result?.ToString();
                        }
                    }
                    catch (Exception ex)
                    {
                        // Captura el error si ocurre durante la selección de valor
                        Console.WriteLine($"Error al obtener valor del campo {campo.FIELD}: {ex.Message}");
                        continue;  // Si no podemos obtener el valor, pasamos al siguiente campo
                    }

                    // Insertar limitación en FWCIAPLANLIM
                    string sqlInsertLimite = @"
            INSERT INTO FWCIAPLANLIM (CL_CVARIABLE, CL_NLIMITE, CL_NID, CL_CCIA, CL_CRUC, CL_DFECPERI)
            VALUES (@Variable, @Limite, @PlanId, @CompaniaId, @Cruc, @FechaInicio)";

                    try
                    {
                        using (var cmdInsert = cn.CreateCommand())
                        {
                            cmdInsert.Transaction = transaction;
                            cmdInsert.CommandText = sqlInsertLimite;
                            cmdInsert.Parameters.AddWithValue("@Variable", campo.FIELD);
                            cmdInsert.Parameters.AddWithValue("@Limite", valor);
                            cmdInsert.Parameters.AddWithValue("@PlanId", nuevoPlanId);
                            cmdInsert.Parameters.AddWithValue("@CompaniaId", request.CC_CCIA);
                            cmdInsert.Parameters.AddWithValue("@Cruc", request.CC_CRUC);
                            cmdInsert.Parameters.AddWithValue("@FechaInicio", DateTime.Parse(request.Fchaini));
                            await cmdInsert.ExecuteNonQueryAsync();
                        }
                    }
                    catch (Exception ex)
                    {
                        // Captura el error si ocurre durante la inserción de limitación
                        Console.WriteLine($"Error al insertar la limitación para el campo {campo.FIELD}: {ex.Message}");
                        continue;  // Si no podemos insertar la limitación, pasamos al siguiente campo
                    }
                }
            }
            catch (Exception ex)
            {
                // Captura cualquier error general durante el proceso de inserción de limitaciones
                Console.WriteLine($"Error al procesar las limitaciones: {ex.Message}");
                throw;
            }
        }

        public async Task<bool> ActualizarLimites(List<REQUEST_ACTUALIZAR_LIMITE> request)
        {
            using (var cn = _MySqlDatabase.GetConnection())
            {
                await cn.OpenAsync();
                using (var transaction = await cn.BeginTransactionAsync())
                {
                    try
                    {
                        foreach (var item in request)
                        {
                            string query = @"
                        UPDATE FWCIAPLANLIM
                        SET CL_NLIMITE = @NuevoLimite
                        WHERE CL_NID = @Nid
                        AND CL_CRUC = @Cruc
                        AND CL_CCIA = @Ccia
                        AND CL_CVARIABLE = @Variable";

                            using (var cmd = new MySqlCommand(query, cn, (MySqlTransaction)transaction))
                            {
                                cmd.Parameters.AddWithValue("@NuevoLimite", item.CL_NLIMITE);
                                cmd.Parameters.AddWithValue("@Nid", item.CL_NID);
                                cmd.Parameters.AddWithValue("@Cruc", item.CL_CRUC);
                                cmd.Parameters.AddWithValue("@Ccia", item.CL_CCIA);
                                cmd.Parameters.AddWithValue("@Variable", item.CL_CVARIABLE);

                                await cmd.ExecuteNonQueryAsync();
                            }
                        }

                        await transaction.CommitAsync();
                        return true;
                    }
                    catch (Exception e)
                    {
                        await transaction.RollbackAsync();
                        throw;
                    }
                }
            }
        }

        public async Task<Dictionary<string, object>> ObtenerLimitacionesPorPlan(string planCodigo)
        {
            string sqlGetCampos = "SHOW FULL FIELDS FROM FWPLANC WHERE FIELD LIKE 'PC_NL%'";
            var limitaciones = new Dictionary<string, object>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    await cn.OpenAsync();

                    using (var transaction = await cn.BeginTransactionAsync())
                    {
                        var campos = new List<string>();

                        // Obtener los nombres de los campos de limitaciones
                        using (var cmd = cn.CreateCommand())
                        {
                            cmd.Transaction = (MySqlTransaction)transaction;
                            cmd.CommandText = sqlGetCampos;
                            using (var rd = await cmd.ExecuteReaderAsync())
                            {
                                while (await rd.ReadAsync())
                                {
                                    campos.Add(rd["Field"].ToString().Trim());
                                }
                            }
                        }

                        if (campos.Count == 0)
                            return limitaciones;

                        // Construir la consulta dinámica para obtener los valores
                        string sqlSelectValores = $"SELECT {string.Join(",", campos)} FROM FWPLANC WHERE PC_CPLAN = @PlanCodigo LIMIT 1";

                        using (var cmd = cn.CreateCommand())
                        {
                            cmd.Transaction = (MySqlTransaction)transaction;
                            cmd.CommandText = sqlSelectValores;
                            cmd.Parameters.AddWithValue("@PlanCodigo", planCodigo);

                            using (var rd = await cmd.ExecuteReaderAsync())
                            {
                                if (await rd.ReadAsync())
                                {
                                    foreach (var campo in campos)
                                    {
                                        limitaciones[campo] = rd[campo] != DBNull.Value ? rd[campo] : null;
                                    }
                                }
                            }
                        }

                        await transaction.CommitAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener limitaciones: {ex.Message}");
            }

            return limitaciones;
        }


    }
}
