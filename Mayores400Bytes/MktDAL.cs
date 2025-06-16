using RSFacWeb.Interfaces;
using System.Threading.Tasks;
using System;
using RSFacWeb.Entities.MKTPUBLIC;
using MySql.Data.MySqlClient;
using RSFacWeb.ModelsView;
using RSFacWeb.Models.Databases;
using System.IO;
using Microsoft.Extensions.Configuration;
using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon;
using Amazon.Runtime;
using Amazon.S3.Model;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
using Microsoft.AspNetCore.Mvc;
using System.Linq;
using Org.BouncyCastle.Asn1.X509;
using RSFacWeb.Entities;


namespace RSFacWeb.Models
{
    public class MktDAL : IMktDAL
    {
        private readonly MySQLDatabase _MySqlDatabase;
        private readonly IConfiguration _configuracion;
        private readonly string _baseUrl;

        public MktDAL(MySQLDatabase MySqlDatabase, IConfiguration configuracion)
        {
            _MySqlDatabase = MySqlDatabase;
            _configuracion = configuracion;
            _baseUrl = configuracion["s3-domain"];
        }

        public async Task<Error> Guardarpopup(MKTPUBLIC model)
        {
            var Error = new Error();
            MySqlTransaction transaction = null;

            // Consulta para verificar si el registro existe
            var checkQuery = "SELECT COUNT(*) FROM MKTPUBLIC WHERE MK_CTIPO = @Tipo";

            // Consulta para actualización
            var updateQuery = @"
                        UPDATE MKTPUBLIC
                        SET MK_CNOMBRE = @Nombre,
                            MK_CTARGET = @Target,
                            MK_CFACTIVO = @Activo,
                            MK_CIMGURL = @ImgUrl,
                            MK_CCONTENT = @Image,
                            MK_CLINKURL = @LinkUrl,
                            MK_CUSUMOD = @UsuarioMod,
                            MK_DFECMOD = NOW(),
                            MK_DFECDESDE = @FechaDesde,
                            MK_DFECHASTA = @FechaHasta
                        WHERE MK_CTIPO = @Tipo";

            // Consulta para inserción
            var insertQuery = @"INSERT INTO MKTPUBLIC (MK_CNOMBRE, MK_CTARGET, MK_CFACTIVO, MK_CTIPO, MK_CIMGURL, MK_CLINKURL, MK_CUSUMOD, MK_DFECMOD, MK_DFECCRE, MK_NUBIC,MK_CUSUCRE,MK_DFECTSTP, MK_CTITLE,MK_CCONTENT, MK_DFECDESDE, MK_DFECHASTA)
                                VALUES (@Nombre, @Target, @Activo, @Tipo, @ImgUrl, @LinkUrl, @UsuarioMod, NOW(), NOW(),'0',@UsuarioCr,NOW(),'',@Image, @FechaDesde, @FechaHasta)";

            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();
                        transaction = cn.BeginTransaction();
                        cmd.Transaction = transaction;

                        // Verificar si el registro existe
                        cmd.CommandText = checkQuery;
                        cmd.Parameters.AddWithValue("@Tipo", model.MK_CTIPO);
                        var exists = Convert.ToInt32(await cmd.ExecuteScalarAsync());

                        if (exists > 0)
                        {
                            // Registro existe, actualizar
                            cmd.CommandText = updateQuery;
                        }
                        else
                        {
                            // Registro no existe, insertar
                            cmd.CommandText = insertQuery;
                        }

                        // Agregar parámetros para la inserción o actualización
                        cmd.Parameters.AddWithValue("@Nombre", model.MK_CNOMBRE);
                        cmd.Parameters.AddWithValue("@Target", model.MK_CTARGET);
                        cmd.Parameters.AddWithValue("@Activo", model.MK_CFACTIVO);
                        cmd.Parameters.AddWithValue("@FechaDesde", model.MK_DFECDESDE);
                        cmd.Parameters.AddWithValue("@FechaHasta", model.MK_DFECHASTA);
                        cmd.Parameters.AddWithValue("@ImgUrl", model.MK_CIMGURL);
                        cmd.Parameters.AddWithValue("@Image", string.Empty);
                        cmd.Parameters.AddWithValue("@LinkUrl", model.MK_CLINKURL);
                        cmd.Parameters.AddWithValue("@UsuarioMod", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@UsuarioCr", model.MK_CUSUCRE);

                        await cmd.ExecuteNonQueryAsync();
                        await transaction.CommitAsync();

                        if (model.publicar.Equals("S") && model.MK_CFACTIVO.Equals("S"))
                        {
                            string baseUrl = $"{_baseUrl}popup/";
                            // Crear JSON para S3
                            var jsonData = new[]
                            {
                                new
                                {
                                    target = model.MK_CTARGET,
                                    fecha_desde = model.MK_DFECDESDE?.ToString("yyyy-MM-dd"),
                                    fecha_hasta = model.MK_DFECHASTA?.ToString("yyyy-MM-dd"),
                                    image_url = $"{baseUrl}{model.MK_CIMGURL}",
                                    link_url = model.MK_CLINKURL,
                                }
                            };

                            // Convertir a JSON
                            var jsonString = JsonConvert.SerializeObject(jsonData);
                            var tipo = model.MK_CTIPO;
                            var imagen = model.MK_CCONTENT;
                            var nimagen = model.MK_CIMGURL;

                            try
                            {
                                // Enviar JSON a S3
                                await UploadJsonToS3(jsonString, tipo, new[] { imagen }, new[] { nimagen });
                            }
                            catch (Exception ex)
                            {
                                // Manejar el error al subir JSON a S3
                                throw new Exception("Error al subir JSON a S3", ex);
                            }
                        }

                        Error.Status_code = "0";
                        Error.Status_message = "Se actualizo correctamente";
                    }
                    await cn.CloseAsync();
                }
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                Error.Status_message = "Ocurrio un error al editar";
                if (transaction != null)
                {
                    await transaction.RollbackAsync();
                }
            }
            return Error;

        }
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
                    MODI.US_CNOMBRE AS MODIFICADOR_NOMBRE
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
                                    //MK_DFECDESDE = reader.IsDBNull(reader.GetOrdinal("MK_DFECDESDE"))
                                    //               ? (DateTime?)null
                                    //               : reader.GetDateTime(reader.GetOrdinal("MK_DFECDESDE")),
                                    //MK_DFECHASTA = reader.IsDBNull(reader.GetOrdinal("MK_DFECHASTA"))
                                    //               ? (DateTime?)null
                                    //               : reader.GetDateTime(reader.GetOrdinal("MK_DFECHASTA")),
                                    //MK_NUBIC = reader.GetInt32("MK_NUBIC"),
                                    //MK_CUUID = reader.GetString("MK_CUUID"),
                                    //CR_NOMBRE = reader.GetString("CREADOR_NOMBRE"),
                                    //MR_NOMBRE = reader.GetString("MODIFICADOR_NOMBRE"),
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


        public async Task<Error> CrearBanner(MKTPUBLIC model)
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
                    INSERT INTO MKTPUBLIC (MK_CNOMBRE, MK_CTARGET, MK_CTIPO,MK_CFACTIVO, MK_CIMGURL, MK_CLINKURL, MK_CUSUMOD, MK_DFECMOD, MK_DFECCRE, MK_NUBIC, MK_CUSUCRE, MK_DFECTSTP, MK_CTITLE, MK_CCONTENT, MK_CUUID)
                    VALUES (@Nombre, 'ALL',@Tipo, @Activo, @ImgUrl, @LinkUrl, @UsuarioMod, NOW(), NOW(), '0', @UsuarioCr, NOW(),'', @Content, @Cuid)";

                        // Asignar parámetros
                        cmd.CommandText = insertQuery;
                        cmd.Parameters.AddWithValue("@Nombre", model.MK_CNOMBRE);
                        cmd.Parameters.AddWithValue("@Tipo", model.MK_CTIPO);
                        cmd.Parameters.AddWithValue("@Activo", model.MK_CFACTIVO);
                        cmd.Parameters.AddWithValue("@ImgUrl", model.MK_CIMGURL);
                        cmd.Parameters.AddWithValue("@LinkUrl", model.MK_CLINKURL);
                        cmd.Parameters.AddWithValue("@UsuarioMod", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@UsuarioCr", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@Content", string.Empty);
                        cmd.Parameters.AddWithValue("@Cuid", model.MK_CUUID);

                        await cmd.ExecuteNonQueryAsync();
                        Error.Status_code = "0"; // Éxito
                        Error.Status_message = "Se creo con éxito";
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

        public async Task<Error> EditarBanner(MKTPUBLIC model)
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
                        MK_CIMGURL = @ImgUrl,
                        MK_CLINKURL = @LinkUrl,
                        MK_CUSUMOD = @UsuarioMod,
                        MK_DFECMOD = NOW(),
                        MK_CCONTENT = @Content,
                        MK_CUUID = @Cuid
                    WHERE MK_NID = @Id AND MK_CTIPO = @Tipo";

                        // Asignar parámetros
                        cmd.CommandText = updateQuery;
                        cmd.Parameters.AddWithValue("@Nombre", model.MK_CNOMBRE);
                        cmd.Parameters.AddWithValue("@Activo", model.MK_CFACTIVO);
                        cmd.Parameters.AddWithValue("@ImgUrl", model.MK_CIMGURL);
                        cmd.Parameters.AddWithValue("@LinkUrl", model.MK_CLINKURL);
                        cmd.Parameters.AddWithValue("@UsuarioMod", model.MK_CUSUMOD);
                        cmd.Parameters.AddWithValue("@Content", string.Empty);
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
        public async Task<List<MKTPUBLIC>> ActualizarOrdenBanners(List<BannerOrden> ordenes, string tipo)
        {
            try
            {
                using (var cn = _MySqlDatabase.GetConnection())
                {
                    using (var cmd = cn.CreateCommand())
                    {
                        await cn.OpenAsync();

                        // Iniciar transacción
                        using (var transaction = await cn.BeginTransactionAsync())
                        {
                            try
                            {
                                // Actualizar el campo MK_NUBIC con el nuevo orden basado en la lista de BannerOrden
                                foreach (var orden in ordenes)
                                {
                                    var updateQuery = @"
                            UPDATE MKTPUBLIC
                            SET MK_NUBIC = @NuevaUbicacion
                            WHERE MK_NID = @Id AND MK_CTIPO = @Tipo";

                                    cmd.CommandText = updateQuery;
                                    cmd.Parameters.Clear();
                                    cmd.Parameters.AddWithValue("@NuevaUbicacion", orden.Orden);
                                    cmd.Parameters.AddWithValue("@Id", orden.Id);
                                    cmd.Parameters.AddWithValue("@Tipo", tipo);

                                    await cmd.ExecuteNonQueryAsync();
                                }

                                // Commit de la transacción
                                await transaction.CommitAsync();
                            }
                            catch (Exception e)
                            {
                                // Rollback en caso de error
                                await transaction.RollbackAsync();
                                throw new Exception("Error al actualizar el orden de los banners", e);
                            }
                        }
                    }
                }

                // Obtener la lista actualizada de banners
                return await ListarTipo(tipo);
            }
            catch (Exception e)
            {
                throw new Exception("Error en la operación", e);
            }
        }

        public async Task<Error> PublishBanners(List<BannerModel> banners)
        {
            var Error = new Error();

            // Verifica que todos los banners sean válidos
            foreach (var banner in banners)
            {
                if (banner.Tipo != "BAN" || banner.Publicar != "S")
                {
                    Error.Status_message = "Tipo de banner inválido o no está activo para publicar.";
                    return Error;
                }
            }

            // Crear la lista de objetos para el JSON
            var jsonBanners = banners.Select(b => new
            {
                image_url = $"{_baseUrl}banner/{b.Nimagen}",  // Combinar base URL con nombre de imagen
                link_url = b.Link
            }).ToList();

            // Convertir a JSON
            var jsonString = JsonConvert.SerializeObject(jsonBanners);

            // Enviar JSON al servicio Update
            await UploadJsonToS3(jsonString, "BAN", banners.Select(b => b.Imagen).ToArray(), banners.Select(b => b.Nimagen).ToArray());

            Error.Status_message = "Banners publicados correctamente";
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



    }
}
