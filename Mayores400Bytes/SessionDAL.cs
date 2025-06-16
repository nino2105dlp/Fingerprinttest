using MySql.Data.MySqlClient;
using RSFacWeb.Entities;
using RSFacWeb.Interfaces;
using RSFacWeb.Models.Databases;
using RSFacWeb.ModelsView;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace RSFacWeb.Models
{
    public class SessionDAL : ISessionDAL
    {
        private readonly MySQLDatabase _MySqlDatabase;

        public SessionDAL(MySQLDatabase MySqlDatabase)
        {
            _MySqlDatabase = MySqlDatabase;
        }

        public async Task<string> ObtenerActividadSession(string US_CIDUSU, int SE_NIDSESI)
        {
            string Estado = "";
            try
            {

                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT SE_NACSESI FROM FWSESSION WHERE SE_NIDSESI = " + SE_NIDSESI + " AND " +
                                           "SE_CIDUSU = '" + US_CIDUSU + "';";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                Estado = rd["SE_NACSESI"].ToString();
                            }
                            rd.Close();
                        }
                    }
                    await cn.CloseAsync();
                }
            }
            catch (MySqlException my)
            {
                var msj = my.Message;
                throw my;
            }
            catch (Exception e)
            {
                throw e;
            }
            return Estado;
        }
        public async Task<int> ValidarSession(string US_CIDUSU)
        {
            int Rpta = 0;
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT COUNT(SE_NIDSESI) FROM FWSESSION WHERE SE_NACSESI = 1 " +
                                           "AND SE_CIDUSU = '" + US_CIDUSU + "';";
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
        public async Task<bool> LimpiarSession(string US_CIDUSU)
        {
            bool Execute = false;
            MySqlTransaction transaction = null;

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();
                        transaction = cn.BeginTransaction(IsolationLevel.ReadCommitted);
                        cmd.Transaction = transaction;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"UPDATE FWSESSION SET SE_NACSESI = 0 WHERE " +
                                           "SE_CIDUSU = '" + US_CIDUSU + "';";
                        cmd.ExecuteNonQuery();
                        transaction.Commit();
                        Execute = true;
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                transaction.Rollback();
                throw e;
            }

            return Execute;
        }
        public async Task<long> RegistroSession(string US_CIDUSU, string US_CIPUSER, string US_CBROWSER)
        {
            long PUNSESION = 0;
            MySqlTransaction transaction = null;

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();
                        transaction = cn.BeginTransaction(IsolationLevel.ReadCommitted);
                        cmd.Transaction = transaction;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"INSERT INTO FWSESSION (SE_CIDUSU,SE_CIPUSU,SE_CBWUSU,SE_DFECREG,SE_NACSESI) " +
                                           "VALUES ('" + US_CIDUSU + "','" + US_CIPUSER + "','" + US_CBROWSER + "', NOW(), 1);";
                        cmd.ExecuteNonQuery();
                        transaction.Commit();
                        PUNSESION = cmd.LastInsertedId;
                    }

                    cn.Close();
                }
            }
            catch (Exception e)
            {
                transaction.Rollback();
                throw e;
            }

            return PUNSESION;
        }
        public async Task<FWSESSION_LISTA_PAGINACION> SessionListar(FWSESSION_LISTA_REQUEST FWSESSION_LISTA_REQUEST)
        {
            string condicion = "";
            List<FWSESSION_LISTA> FWSESSION_LISTA = new List<FWSESSION_LISTA>();
            FWSESSION_LISTA_PAGINACION FWSESSION_LISTA_PAGINACION = new FWSESSION_LISTA_PAGINACION();

            string count = "COUNT(U.US_CEMAIL) AS TOTALES ";

            string select = "U.US_CEMAIL, U.US_CIDUSU" +
                            "(SELECT CONCAT(S.SE_CBWUSU, '|', S.SE_CIPUSU, '|', S.SE_DFECREG) FROM FWSESSION S " +
                            "WHERE S.SE_CIDUSU = U.US_CIDUSU AND S.SE_NACSESI = 1) AS US_CINFO ";

            string from = "FROM FWUSERS U WHERE U.US_CCODAMB = 'CL' AND U.US_CFACTIVO = 'S'";

            if (!string.IsNullOrEmpty(FWSESSION_LISTA_REQUEST.US_CEMAIL))
            {
                condicion = " AND U.US_CEMAIL LIKE '% " + FWSESSION_LISTA_REQUEST.US_CEMAIL.Trim() + " %'";
            }

            var skipreg = (FWSESSION_LISTA_REQUEST.Total_paginas * FWSESSION_LISTA_REQUEST.Pagina_actual) - FWSESSION_LISTA_REQUEST.Total_paginas;
            var paginacion = "Limit " + skipreg + "," + FWSESSION_LISTA_REQUEST.Total_paginas + ";";
            var query = select + from + condicion + " ORDER BY U.US_CIDUSU " + paginacion;
            var totalquery = count + from + condicion + ";";

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
                                FWSESSION_LISTA.Add(new FWSESSION_LISTA()
                                {
                                    ROWNUMBER = skipreg,
                                    US_CIDUSU = rd["US_CEMAIL"].ToString(),
                                    US_CEMAIL = rd["US_CEMAIL"].ToString(),
                                    US_CINFO = rd["US_CINFO"].ToString()
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
                    FWSESSION_LISTA_PAGINACION.FWSESSION_LISTA = FWSESSION_LISTA;
                    FWSESSION_LISTA_PAGINACION.Total_resultados = Total_resultados;
                    FWSESSION_LISTA_PAGINACION.Total_paginas = FWSESSION_LISTA_REQUEST.Total_paginas;
                    FWSESSION_LISTA_PAGINACION.Pagina_actual = FWSESSION_LISTA_REQUEST.Pagina_actual;
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            return FWSESSION_LISTA_PAGINACION;
        }

        public async Task<FWSESSION_LISTA_PAGINACION> SessionListarUsers(FWSESSION_LISTA_REQUEST request)
        {
            List<FWSESSION_LISTA> sessionList = new List<FWSESSION_LISTA>();
            FWSESSION_LISTA_PAGINACION pagination = new FWSESSION_LISTA_PAGINACION();

            string selectQuery = @"SELECT U.US_CNOMBRE,U.US_CIDUSU, U.US_CEMAIL, U.US_CTIPUSU, U.US_CCODAMB, 
                           C.CI_CRAZON, C.CI_CRUC, S.SE_CIPUSU, S.SE_NACSESI, S.SE_CBWUSU, S.SE_DFECREG ";
            string fromQuery = @"FROM FWUSERS U 
                         JOIN FWUSUCIA SU ON U.US_CIDUSU = SU.UC_CIDUSU
                         JOIN FWCIAS C ON SU.UC_CCODCIA = C.CI_CID
                         LEFT JOIN FWSESSION S ON U.US_CIDUSU = S.SE_CIDUSU AND S.SE_NACSESI = 1 
                         WHERE U.US_CFACTIVO = 'S' ";

            // Construir condiciones de filtro
            string conditions = "";
            if (!string.IsNullOrEmpty(request.US_CEMAIL))
            {
                conditions += " AND U.US_CEMAIL LIKE '%" + request.US_CEMAIL.Trim() + "%'";
            }
            if (!string.IsNullOrEmpty(request.CI_CID))
            {
                conditions += " AND C.CI_CID = '" + request.CI_CID.Trim() + "'";
            }
            if (!string.IsNullOrEmpty(request.CI_CRAZON))
            {
                conditions += " AND C.CI_CRAZON LIKE '%" + request.CI_CRAZON.Trim() + "%'";
            }
            if (!string.IsNullOrEmpty(request.US_CNOMBRE))
            {
                conditions += " AND U.US_CNOMBRE LIKE '%" + request.US_CNOMBRE.Trim() + "%'";
            }

            if (!string.IsNullOrEmpty(request.US_CCODAMB))
            {
                conditions += " AND U.US_CCODAMB LIKE '%" + request.US_CCODAMB.Trim() + "%'";
            }
            // Paginación
            int skip = (request.Total_paginas * (request.Pagina_actual - 1));
            string paginationClause = $" ORDER BY S.SE_DFECREG DESC LIMIT {skip}, {request.Total_paginas};";

            // Consultas de datos y total
            string dataQuery = selectQuery + fromQuery + conditions + paginationClause;
            string totalQuery = "SELECT COUNT(*) AS TOTALES " + fromQuery + conditions + ";";

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = dataQuery + " " + totalQuery;

                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            // Leer resultados de datos
                            while (await rd.ReadAsync())
                            {
                                sessionList.Add(new FWSESSION_LISTA()
                                {
                                    US_CIDUSU = rd["US_CIDUSU"].ToString(),
                                    US_CNOMBRE = rd["US_CNOMBRE"].ToString(),
                                    US_CEMAIL = rd["US_CEMAIL"].ToString(),
                                    US_CTIPUSU = rd["US_CTIPUSU"].ToString(),
                                    US_CCODAMB = rd["US_CCODAMB"].ToString(),
                                    CI_CRAZON = rd["CI_CRAZON"].ToString(),
                                    CI_CRUC = rd["CI_CRUC"].ToString(),
                                    SE_CIPUSU = rd["SE_CIPUSU"].ToString(),
                                    SE_CBWUSU = rd["SE_CBWUSU"].ToString(),
                                    SE_NACSESI = rd["SE_NACSESI"].ToString(),
                                    SE_DFECREG = rd["SE_DFECREG"].ToString(),
                                });
                            }

                            // Leer resultado total
                            await rd.NextResultAsync();
                            while (await rd.ReadAsync())
                            {
                                pagination.Total_resultados = Convert.ToInt32(rd["TOTALES"]);
                            }
                        }
                    }
                    pagination.FWSESSION_LISTA = sessionList;
                    pagination.Total_paginas = request.Total_paginas;
                    pagination.Pagina_actual = request.Pagina_actual;
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error al obtener datos de sesión", e);
            }

            return pagination;
        }

        public async Task<bool> CerrarSessionAd(string US_CIDUSU)
        {
            bool Execute = false;
            MySqlTransaction transaction = null;

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();
                        transaction = cn.BeginTransaction(IsolationLevel.ReadCommitted);
                        cmd.Transaction = transaction;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"UPDATE FWSESSION SET SE_NACSESI = 0 WHERE " +
                                           "SE_CIDUSU = '" + US_CIDUSU + "';";
                        cmd.ExecuteNonQuery();
                        transaction.Commit();
                        Execute = true;
                    }
                    cn.Close();
                }
            }
            catch (Exception e)
            {
                transaction.Rollback();
                throw e;
            }

            return Execute;
        }

        public async Task<List<FWSESSION>> SessionUltimosMov(string US_CIDUSU)
        {
            var FWSESSION = new List<FWSESSION>();

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = @"SELECT SE_CIPUSU, SE_CBWUSU, DATE_FORMAT(SE_DFECREG, '%d/%m/%Y %H:%i:%s') AS SE_DFECREG " +
                                           "FROM FWSESSION WHERE SE_CIDUSU = '" + US_CIDUSU + "' AND SE_NACSESI = 0 " +
                                           "ORDER BY SE_DFECREG DESC LIMIT 5;";
                        await cn.OpenAsync();
                        using (var rd = await cmd.ExecuteReaderAsync())
                        {
                            while (rd.Read())
                            {
                                FWSESSION.Add(new FWSESSION()
                                {
                                    SE_CIPUSU = rd["SE_CIPUSU"].ToString(),
                                    SE_CBWUSU = rd["SE_CBWUSU"].ToString(),
                                    SE_DFECREG = rd["SE_DFECREG"].ToString()
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
            return FWSESSION;
        }

    }
}
