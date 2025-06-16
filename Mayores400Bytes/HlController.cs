using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using RS_FacturadorWeb.ClienteJWT;
using RS_FacturadorWeb.EntityJWT;
using RSFacWeb.Interfaces;
using RSFacWeb.Util;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System;
using System.Linq;
using RSFacWeb.ModelsView;
using RSFacWeb.Entities.MKTPUBLIC;
using RSFacWeb.Entities;
using RSFacWeb.ModelsView.FWCIAS;
using RSFacLocal.ModelsView;
using RSFacWeb.ModelsView.XML.ModelsView;
using RSFacWeb.ModelsView.FELOG;
using Amazon.Runtime.Internal.Util;
using Microsoft.Extensions.Logging;
using RSFacLocal.Entities;
using RSFacLocal.ModelsView.CPE;
using RSFacWeb.Utils;
using RSFacWeb.ModelsView.FERESC;
using RSFacLocal.ModelsView.FERESC;
using RSFacWeb.ModelsView.FTAGEN;
using RSFacWeb.ModelsView.PDPENC;
using RSFacWeb.ModelsView.FEERROR;

namespace RSFacWeb.Controllers
{
    [Produces("application/json")]
    [Route("api/auth/v1/facweb/hl/[action]")]
    [Authorize]
    [ApiController]
    public class HlController : RsControllerBase
    {
        private readonly IHlDAL _hl;
        private readonly IConfiguration _configuration;
        private readonly ILogger<HlController> _logger;
        public HlController(IHlDAL hl, IConfiguration configuration, ILogger<HlController> logger)
        {
            _hl = hl;
            _configuration = configuration;
            _logger = logger;   
        }
        #region HlAlertas
        [HttpGet]
        public async Task<IActionResult> ListarTipo(string tipo)
        {
            var Claims = (GLOBALES)HttpContext.Items["User"];

            _logger.LogInformation($"Iniciando endpoint Listar Tipo {tipo}");

            try
            {
                var result = await _hl.ListarTipo(tipo);

                if (result == null || !result.Any())
                {
                    return Ok(new { message = "No se encontraron datos." });
                }

                // Llamar al directorio principal de appsetting.json 
                string WorkDir = _configuration["AppWorkDir"];

                // Validar que el directorio de trabajo no sea null ni vacío
                if (string.IsNullOrEmpty(WorkDir))
                {
                    return StatusCode(500, "El directorio de trabajo no está configurado.");
                }

                // Verificar que el directorio exista en el servidor
                if (!Directory.Exists(WorkDir))
                {
                    return StatusCode(500, $"El directorio de trabajo '{WorkDir}' no existe en el servidor.");
                }

                //// Obtener el ambiente del usuario que consume el endpoint
                //string Ambiente = Claims.PUCCODAMB?.ToLower();

                //if (string.IsNullOrEmpty(Ambiente))
                //{
                //    return StatusCode(500, "El ambiente del usuario no está definido.");
                //}

                foreach (var item in result)
                {
                    // Obtener la extensión del archivo
                    var extension = Path.GetExtension(item.MK_CIMGURL);
                    var nombreBase = Path.GetFileNameWithoutExtension(item.MK_CIMGURL);

                    // Construir los nombres de archivo
                    var nuevoNombreArchivonot = $"notification-{item.MK_CUUID}{extension}";
                    var nuevoNombreArchivoban = $"banner-{item.MK_CUUID}{extension}";

                    // Definir rutas posibles
                    var rutasPosibles = new List<string>
            {
                Path.Combine(WorkDir, "co", "assets", "banner", nuevoNombreArchivoban),
                Path.Combine(WorkDir, "co", "assets", "popup", item.MK_CIMGURL),
                Path.Combine(WorkDir, "co", "assets", "notification", nuevoNombreArchivonot)
            };

                    bool rutaEncontrada = false;

                    foreach (var ruta in rutasPosibles)
                    {
                        // Verificar si la ruta existe
                        if (System.IO.File.Exists(ruta))
                        {
                            try
                            {
                                // Convertir la imagen a base64
                                item.MK_CCONTENT = Convert.ToBase64String(System.IO.File.ReadAllBytes(ruta));

                                // Ajustar la URL según la ruta encontrada
                                if (ruta.Contains(Path.Combine(WorkDir, "co", "assets", "popup")))
                                {
                                    item.MK_CIMGURL = Path.GetFileName(ruta); // Mantener el nombre original
                                }
                                else if (ruta.Contains(Path.Combine(WorkDir, "co", "assets", "banner")))
                                {
                                    item.MK_CIMGURL = nuevoNombreArchivoban; // Nombre nuevo para banner
                                }
                                else if (ruta.Contains(Path.Combine(WorkDir, "co", "assets", "notification")))
                                {
                                    item.MK_CIMGURL = nuevoNombreArchivonot; // Nombre nuevo para notification
                                }

                                rutaEncontrada = true;
                                break; // Salir si encontramos la ruta
                            }
                            catch (IOException ioEx)
                            {
                                return StatusCode(500, $"Error al leer el archivo: {ioEx.Message}");
                            }
                        }
                    }

                    // Si no se encontró una ruta válida, puedes asignar un valor predeterminado o manejar el error
                    if (!rutaEncontrada)
                    {
                        item.MK_CCONTENT = null; // O cualquier valor predeterminado
                    }
                }

                return Ok(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,$"Error: {ex.Message}");

                return StatusCode(500, $"Error interno del servidor: {ex.Message}");
            }
        }
        [HttpPost]
        public async Task<IActionResult> CrearAlerta(MKTPUBLIC model)
        {
            var Error = new Error();
            try
            {
                Error = await _hl.CrearAlerta(model);
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }
        [HttpPut]
        public async Task<IActionResult> EditarAlerta(MKTPUBLIC model)
        {
            var Error = new Error();
            try
            {
                Error = await _hl.EditarAlerta(model);
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }

        [HttpPost]
        public async Task<IActionResult> PublishAlert([FromBody] List<MKTPUBLIC> model)
        {
            var Error = new Error();
            try
            {
                // Pasar la lista completa al DAL
                Error = await _hl.PublishAlert(model);
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }

        [HttpPost]
        public async Task<IActionResult> CrearNotification(MKTPUBLIC model)
        {

            var Claims = (GLOBALES)HttpContext.Items["User"];

            try
            {
                Error Error = await _hl.CrearNotification(model);

                if (Error.Status_code.Equals("0"))
                {
                    if (!string.IsNullOrEmpty(model.MK_CCONTENT))
                    {
                        // Obtener la imagen y convertirla a byte
                        byte[] ImagenBanner = Convert.FromBase64String(model.MK_CCONTENT);

                        // Llamar al directorio principal de appsetting.json
                        string WorkDir = _configuration["AppWorkDir"];

                        // Obtener el ambiente del usuario que consume el endpoint
                        string Ambiente = Claims.PUCCODAMB.ToLower();

                        // Obtener el nombre del archivo original con su extensión
                        string nombreArchivoOriginal = model.MK_CIMGURL;

                        // Obtener el UUID
                        string uuid = model.MK_CUUID;

                        // Obtener el nombre del archivo sin la extensión
                        string nombreSinExtension = Path.GetFileNameWithoutExtension(nombreArchivoOriginal);

                        // Obtener la extensión del archivo original
                        string extension = Path.GetExtension(nombreArchivoOriginal);

                        // Construir el nuevo nombre del archivo concatenando el UUID y el nombre sin extensión, seguido de la extensión
                        string nuevoNombreArchivo = $"notification-{uuid}{extension}";

                        // Construir ruta donde se guardará la imagen (incluyendo el nombre del archivo)
                        string RutaArchivo = Path.Combine(WorkDir, "co", "assets", "notification", nuevoNombreArchivo);

                        // Verificar si existe la carpeta, si no, crearla
                        string directorio = Path.GetDirectoryName(RutaArchivo);
                        if (!Directory.Exists(directorio))
                        {
                            Directory.CreateDirectory(directorio);
                        }

                        // Guardar ImagenBanner en la ruta especificada
                        System.IO.File.WriteAllBytes(RutaArchivo, ImagenBanner);
                    }
                }

                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }

        [HttpPut]
        public async Task<IActionResult> EditarNotification(MKTPUBLIC model)
        {
            var Claims = (GLOBALES)HttpContext.Items["User"];

            try
            {
                Error Error = await _hl.EditarNotification(model);

                if (Error.Status_code.Equals("0"))
                {
                    if (!string.IsNullOrEmpty(model.MK_CCONTENT))
                    {
                        // Obtener la imagen y convertirla a byte
                        byte[] ImagenBanner = Convert.FromBase64String(model.MK_CCONTENT);

                        // Llamar al directorio principal de appsetting.json
                        string WorkDir = _configuration["AppWorkDir"];

                        // Obtener el ambiente del usuario que consume el endpoint
                        string Ambiente = Claims.PUCCODAMB.ToLower();

                        // Obtener el nombre del archivo original con su extensión
                        string nombreArchivoOriginal = model.MK_CIMGURL;

                        // Obtener el UUID
                        string uuid = model.MK_CUUID;

                        // Obtener el nombre del archivo sin la extensión
                        string nombreSinExtension = Path.GetFileNameWithoutExtension(nombreArchivoOriginal);

                        // Obtener la extensión del archivo original
                        string extension = Path.GetExtension(nombreArchivoOriginal);

                        // Construir el nuevo nombre del archivo concatenando el UUID y el nombre sin extensión, seguido de la extensión
                        string nuevoNombreArchivo = $"notification-{uuid}{extension}";

                        // Construir ruta donde se guardará la imagen (incluyendo el nombre del archivo)
                        string RutaArchivo = Path.Combine(WorkDir, "co", "assets", "notification", nuevoNombreArchivo);

                        // Verificar si existe la carpeta, si no, crearla
                        string directorio = Path.GetDirectoryName(RutaArchivo);
                        if (!Directory.Exists(directorio))
                        {
                            Directory.CreateDirectory(directorio);
                        }

                        // Guardar ImagenBanner en la ruta especificada
                        System.IO.File.WriteAllBytes(RutaArchivo, ImagenBanner);
                    }
                }

                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }

        [HttpPost]
        public async Task<IActionResult> PublishNotification([FromBody] List<MKTPUBLIC> model)
        {
            var Error = new Error();
            try
            {
                // Pasar la lista completa al DAL
                Error = await _hl.PublishNotification(model);
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }
        [HttpDelete]
        public async Task<IActionResult> Eliminar(long id)
        {
            var Error = new Error();
            try
            {
                Error = await _hl.Eliminar(id);
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }
        #endregion

        #region Consola Documentos

        [HttpGet]
        public async Task<IActionResult> FacturaCabListar([FromQuery] REIMPRESION_CABECERA_REQUEST REIMPRESION_CABECERA_REQUEST)
        {
            try
            {

                REIMPRESION_CABECERA_PAGINACION REIMPRESION_CABECERA_PAGINACION;


                    REIMPRESION_CABECERA_PAGINACION = await _hl.FacturaCabListar( REIMPRESION_CABECERA_REQUEST);

                REIMPRESION_CABECERA_PAGINACION.Status_code = "0";
                return Ok(REIMPRESION_CABECERA_PAGINACION);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }

        [HttpGet]
        public async Task<IActionResult> FacturaDetListarSimple([FromQuery] REIMPRESION_DETALLE_REQUEST REIMPRESION_DETALLE_REQUEST)
        {
            try
            {
                var Lista = await _hl.FacturaDetListarSimple(REIMPRESION_DETALLE_REQUEST);

                return Ok(Lista);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }

        #region Listar Errores de FE-ERROR
        [HttpGet]
        public async Task<IActionResult> GetErrors([FromQuery] DOCUMENTO_REQUEST request)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCUSER = Claims.AC_CWSUSER;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                if (string.IsNullOrEmpty(request.CODAGE))
                {
                    return BadRequest(new Error() { Status_code = "400", Status_message = "Agencia es requerida para listar los errores." });
                }

                if (string.IsNullOrEmpty(request.TIPDOC))
                {
                    return BadRequest(new Error() { Status_code = "400", Status_message = "Tipo de documento es requerido para listar los errores." });
                }

                if (string.IsNullOrEmpty(request.NUMSER))
                {
                    return BadRequest(new Error() { Status_code = "400", Status_message = "Serie es requerida para listar los errores." });
                }

                if (string.IsNullOrEmpty(request.NUMDOC))
                {
                    return BadRequest(new Error() { Status_code = "400", Status_message = "Número de documento es requerido para listar los errores." });
                }

                ListaResponseFeerror lista = await _hl.GetErrors(request);

                return Ok(lista);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }
        #endregion

        #region Obtener Estado de los Emails
        [HttpPost]
        public async Task<IActionResult> EstadoCorreo([FromBody] DOCUMENTO_REQUEST request)
        {

            //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

            //var PUCODEMP = Claims.AC_CCIA;
            //var PUCBASED = Claims.CI_CFDB;
            //var PUCUSER = Claims.AC_CWSUSER;
            //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

            try
            {
                SUPROV objConfig = await _hl.ObtenerConfiguracion(request.CCID,request.CIA, request.TIPDOC, "CPE1");

                if (objConfig == null)
                {
                    return BadRequest($"No se encontro la configuración electronica para la compañia {request.CIA}-RSF1-{request.TIPDOC} ");
                }

                string TipoDocumentoSunat = string.Empty;

                //Pendiente: Implementar la funcionalidad para comprobantes de Retención (20) y Percepción (40)
                switch (request.TIPDOC)
                {
                    case "FT": TipoDocumentoSunat = "01"; break;
                    case "BV": TipoDocumentoSunat = "03"; break;
                    case "NC": TipoDocumentoSunat = "07"; break;
                    case "ND": TipoDocumentoSunat = "08"; break;
                    case "GS": TipoDocumentoSunat = "09"; break;
                    default:
                        return BadRequest($"Tipo de comprobante no valido para la consulta de estado de correos. - Tipo: {request.TIPDOC}");
                }

                RequestStatusEmail requestStatusEmail = new RequestStatusEmail
                {
                    Usuario = objConfig.PR_CWSUSER,
                    Password = objConfig.PR_CWSPASS,
                    Ruc = request.CODCIA,
                    Serie = request.NUMSER,
                    Numero = request.NUMDOC,
                    Tipo = TipoDocumentoSunat,
                    Proveedor = "RSF1",
                    Ambiente = objConfig.PR_CPROVMOD
                };

                //await _logDALMySQL.InsertTablaFELOG(PUCBDFACCAR, PUCODEMP, request.CODAGE, "INFO", request.TIPDOC, request.NUMSER, request.NUMDOC, "", "", "P", "-", "", $"Consultando en RS-CLOUD estado de los correos enviados - CPE {request.TIPDOC}-{request.NUMSER}-{request.NUMDOC}.", "", "", PUCUSER, "", "", "", "");
                var response = await _hl.ConsultarEstadosEmail(request.CCID, request.CIA, objConfig.PR_CURL, requestStatusEmail);

                if (response.Data.Count > 0)
                {
                    foreach (var item in response.Data)
                    {
                        item.FechaEnvioFormateada = item.FechaEnvio.ToString("dd/MM/yyyy HH:mm:ss");

                        switch (ConvertHelper.ToNonNullString(item.Estado).Trim())
                        {
                            case "Send":
                                item.Estado = "NO ENTREGADO";
                                break;
                            case "Delivery":
                                item.Estado = "ENVIADO";
                                break;
                            case "Open":
                                item.Estado = "ENTREGADO";
                                break;
                            case "Click":
                                item.Estado = "ENTREGADO y ABIERTO";
                                break;
                            default:
                                item.Estado = "";
                                break;
                        }

                    }
                }

                //await _logDALMySQL.InsertTablaFELOG(PUCBDFACCAR, PUCODEMP, request.CODAGE, "INFO", request.TIPDOC, request.NUMSER, request.NUMDOC, "", "", "P", "-", "", $"Finalizando consulta en RS-CLOUD estado de los correos enviados - CPE {request.TIPDOC}-{request.NUMSER}-{request.NUMDOC}.", "", "", PUCUSER, "", "", "", "");

                return Ok(response);
            }
            catch (Exception ex)
            {
                //await _logDALMySQL.InsertTablaFELOG(PUCBDFACCAR, PUCODEMP, request.CODAGE, "ERROR", request.TIPDOC, request.NUMSER, request.NUMDOC, "", "", "P", "-", "", ex.Message, "", "", PUCUSER, "", "", "", "");
                return BadRequest(ex.Message);
            }

        }
        #endregion

        [HttpPost]
        public async Task<IActionResult> ConsultarLogExcel([FromBody] DOCUMENTO_REQUEST DOCUMENTO_REQUEST)
        {
            try
            {

                List<FELOG> Lista = await _hl.ConsultarLogExcel(DOCUMENTO_REQUEST);

                return Ok(Lista);
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }


        #endregion

        #region Consola Guia
        [HttpGet]
        public async Task<IActionResult> ListarGuias([FromQuery] LISTAR_GUIAS_REQUEST REQUEST)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;
                //var PUCUSER = Claims.AC_CWSUSER;

                var RESPONSE = await _hl.ListarGuias(REQUEST);

                RESPONSE.Status_code = "0";
                return Ok(RESPONSE);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }

        [HttpGet]
        public async Task<IActionResult> ListarDetallesGuia([FromQuery] CONSOLA_GUIA REQUEST)
        {
            try
            {

                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;
                //var PUCUSER = Claims.AC_CWSUSER;

                var lista = await _hl.ListarDetallesGuia(REQUEST);

                var response = new DATA_RESPONSE<List<CONSOLA_GUIA_DET>>()
                {
                    Data = lista,
                    Status_code = "0"
                };

                return Ok(response);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }

        #region CARGA ALMACENES
        [HttpGet]
        public async Task<IActionResult> GetAlmacen(string Cia, string Ruc)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCUSER = Claims.AC_CWSUSER;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                var ListaAlmacenes = await _hl.GetAlmacen(Ruc, Cia);

                var ALALMA_LISTA = new ALALMA_LISTA()
                {
                    LST_ALMACENES = ListaAlmacenes,
                    Status_code = "0"
                };

                return Ok(ALALMA_LISTA);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }
        #endregion
        [HttpGet]
        public async Task<IActionResult> GuiaListaMovimiento(string Cia, string Ruc)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCUSER = Claims.AC_CWSUSER;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                List<ALTABM> LST_MOVIMIENTO = await _hl.GuiaListaMovimiento(Ruc, Cia);

                return Ok(LST_MOVIMIENTO);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }

        [HttpGet]
        public async Task<IActionResult> ConsultarLogExcelGuia([FromQuery] DOCUMENTO_REQUEST DOCUMENTO_REQUEST)
        {

            DOCUMENTO_REQUEST.TIPDOC = ConvertHelper.NonNullStringRemoveSpecialCharacters(DOCUMENTO_REQUEST.TIPDOC);
            DOCUMENTO_REQUEST.CODAGE = ConvertHelper.NonNullStringRemoveSpecialCharacters(DOCUMENTO_REQUEST.CODAGE);
            DOCUMENTO_REQUEST.NUMSER = ConvertHelper.NonNullStringRemoveSpecialCharacters(DOCUMENTO_REQUEST.NUMSER);
            DOCUMENTO_REQUEST.NUMDOC = ConvertHelper.NonNullStringRemoveSpecialCharacters(DOCUMENTO_REQUEST.NUMDOC);

            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                List<FELOG> Lista = await _hl.ConsultarLogExcelGuia(DOCUMENTO_REQUEST);

                return Ok(Lista);
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }

        #region Obtiene Tickets
        [HttpGet]
        public async Task<IActionResult> GetTickets(string Tipo, string Numero, string Serie, string Almacen, string Ruc, string Cia)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                List<FETICKET> Lista = await _hl.ObtenerTickets(Cia, Ruc, Tipo, Serie, Numero, Almacen);

                return Ok(Lista);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
        #endregion

        #endregion

        #region Consola Resumenes
        [HttpGet]
        public async Task<ActionResult> GetListaResumenes([FromQuery] REIMPRESION_RESUMEN_REQUEST REIMPRESION_RESUMEN_REQUEST)
        {
            //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

            //var PUCODEMP = Claims.AC_CCIA;
            //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

            REIMPRESION_RESUMEN_REQUEST.CNUMSER = ConvertHelper.ToNonNullString(REIMPRESION_RESUMEN_REQUEST.CNUMSER);
            REIMPRESION_RESUMEN_REQUEST.CNUMDOC = ConvertHelper.ToNonNullString(REIMPRESION_RESUMEN_REQUEST.CNUMDOC);
            REIMPRESION_RESUMEN_REQUEST.PUFECPRO = ConvertHelper.ToNonNullString(REIMPRESION_RESUMEN_REQUEST.PUFECPRO);
            REIMPRESION_RESUMEN_REQUEST.PUFECPRO2 = ConvertHelper.ToNonNullString(REIMPRESION_RESUMEN_REQUEST.PUFECPRO2);
            REIMPRESION_RESUMEN_REQUEST.CTIPORES = ConvertHelper.ToNonNullString(REIMPRESION_RESUMEN_REQUEST.CTIPORES);


            try
            {

                if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CNUMSER) && REIMPRESION_RESUMEN_REQUEST.CNUMSER.Length > 4)
                {

                    return BadRequest(new ErrorR<Transaccion> { Status_code = "400", Status_message = "Número de serie debe tener un maximo de 4 caracteres", Data = new Transaccion() { Execute = false } });

                }

                if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CNUMDOC) && REIMPRESION_RESUMEN_REQUEST.CNUMDOC.Length > 7)
                {

                    return BadRequest(new ErrorR<Transaccion> { Status_code = "400", Status_message = "Número de serie debe tener un maximo de 4 caracteres", Data = new Transaccion() { Execute = false } });

                }

                if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.PUFECPRO) && !string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.PUFECPRO2))
                {
                    if (DateTime.Parse(REIMPRESION_RESUMEN_REQUEST.PUFECPRO) > DateTime.Parse(REIMPRESION_RESUMEN_REQUEST.PUFECPRO2))
                    {

                        return BadRequest(new ErrorR<Transaccion> { Status_code = "400", Status_message = "Fecha final no puede ser menor a la fecha actual", Data = new Transaccion() { Execute = false } });

                    }
                }

                if (!string.IsNullOrEmpty(REIMPRESION_RESUMEN_REQUEST.CTIPORES) && REIMPRESION_RESUMEN_REQUEST.CTIPORES != "RA" && REIMPRESION_RESUMEN_REQUEST.CTIPORES != "RC")
                {
                    return BadRequest(new ErrorR<Transaccion> { Status_code = "400", Status_message = "Elegir entre la opción RA y RC.", Data = new Transaccion() { Execute = false } });

                }

                REIMPRESION_RESUMEN_REQUEST response = await _hl.GetListaResumenes(REIMPRESION_RESUMEN_REQUEST);
                return Ok(response);
            }
            catch (Exception)
            {
                return BadRequest(new ErrorR<Transaccion> { Status_code = "400", Status_message = "No se pudo listar los resúmenes.", Data = new Transaccion() { Execute = false } });
                throw;
            }

        }

        #region Descargar XML
        [HttpPost]
        public async Task<IActionResult> DescargarXML([FromBody] REIMPRESION_RESUMEN_REQUEST DOCUMENTO_REQUEST)
        {
            //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

            //var PUCODEMP = Claims.AC_CCIA;
            //var PUCDBFACCAR = Claims.CI_CDBFACCAR;
            string Base64 = string.Empty;

            try
            {
                string Base64File = await _hl.GetZipXMLResumenes(DOCUMENTO_REQUEST);

                if (string.IsNullOrEmpty(Base64File))
                {
                    if (DOCUMENTO_REQUEST.TIPOXML == "CDR")
                    {
                        return BadRequest("No se encontro archivo CDR.");

                    }
                    else
                    {
                        return BadRequest("No se encontro archivo XML.");

                    }
                }
                else
                {
                    Base64 = Base64File;
                }
            }
            catch (Exception ex)
            {
                return BadRequest("ERROR BUSCANDO XML: " + ex.Message);
            }

            return Ok(Base64);
        }
        #endregion

        #region Listar CDR
        [HttpGet]
        public async Task<IActionResult> ResumenCDR([FromQuery] REIMPRESION_RESUMEN_REQUEST REIMPRESION_RESUMEN_REQUEST)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                var Lista = await _hl.ResumenCDR(REIMPRESION_RESUMEN_REQUEST);

                return Ok(Lista);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }
        [HttpPost]
        public async Task<IActionResult> ConsultarLogExcelResumen([FromBody] DOCUMENTO_REQUEST DOCUMENTO_REQUEST)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                List<FELOG> Lista = await _hl.ConsultarLogExcelResumen(DOCUMENTO_REQUEST);

                return Ok(Lista);
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }
        #endregion

        #endregion

        #region Consola de Partes
        [HttpGet]
        public async Task<IActionResult> ListaConsolaCab([FromQuery] RequestParteConsola request)
        {
            //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

            try
            {
                (List<ALMOVC> Lista, int TotalRegistros) = await _hl.GetListaConsolaCab(request);

                return Ok(new ALALMA_PE_RESPONSE { ListaConsolaCab = Lista, TotalRegistros = TotalRegistros });
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }
        [HttpGet]
        public async Task<IActionResult> ImprimirParte(string Ruc, string Cia, string CodAlmacen, string TipoDoc, string NumDoc)
        {
            //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

            try
            {
                (ALMOVC Cabecera, List<ALMOVD> Detalle) = await _hl.GetImpresion(Cia, Ruc, new ALMOVC { C5_CALMA = CodAlmacen, C5_CTD = TipoDoc, C5_CNUMDOC = NumDoc });

                ALALMA_PARTE_RESPONSE_IMP response = new ALALMA_PARTE_RESPONSE_IMP
                {
                    Cabecera = Cabecera,
                    Detalle = Detalle
                };

                return Ok(response);
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }
        [HttpGet]
        public async Task<IActionResult> ParteMovimiento(string Ruc, string Cia)
        {
            //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

            try
            {
                List<ALTABM> LST_MOVIMIENTO = await _hl.ParteListaMovimiento(Ruc, Cia);

                //ALALMA_PARTE_RESPONSE_IMP response = new ALALMA_PARTE_RESPONSE_IMP
                //{
                //    Cabecera = Cabecera,
                //    Detalle = Detalle
                //};

                return Ok(LST_MOVIMIENTO);
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }
        #endregion

        #region Consola de Pedidos
        [HttpGet]
        public async Task<IActionResult> CargaGlobales(string Ruc, string Cia)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBASED = Claims.CI_CFDB;
                //var PUCUSER = Claims.AC_CWSUSER;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                //FTTCAJ_VALIDACION oCaja = await _cajaMySQL.GetCodCaja(PUCBDFACCAR, PUCUSER, PUCODEMP);

                //if (string.IsNullOrEmpty(oCaja.PF_CNROCAJ))
                //{
                //    throw new ArgumentException("No tiene configurado una caja el usuario " + PUCUSER);
                //}

                //FTTCAJ oConfigCaja = await _cajaMySQL.GetCod22(PUCBDFACCAR, oCaja.PF_CNROCAJ, PUCODEMP);

                //if (string.IsNullOrEmpty(oConfigCaja.PF_CNUMSERT))
                //{
                //    throw new ArgumentException("No tiene configurado una serie para emitir pedidos Caja: " + oCaja.PF_CNROCAJ);
                //}

                //string PF_CCODMON = await _configProcesosMySQL.GetTipoMoneda(PUCBDFACCAR, PUCODEMP);

                //if (string.IsNullOrEmpty(PF_CCODMON))
                //{
                //    throw new ArgumentException("No tiene configurado un tipo de moneda");
                //}

                //PF_CCODMON = PF_CCODMON.Substring(0, 2);

                //string PF_CDESMON = string.Empty;

                //if (PF_CCODMON.Equals("MN"))
                //{
                //    PF_CDESMON = "EN SOLES";
                //}
                //else
                //{
                //    PF_CDESMON = "EN DOLARES";
                //}

                List<FTVEND_LISTA> ListaVendedor = await _hl.GetListaVendedor(Ruc, Cia);

                List<Entities.FTAGEN> ListaAgencia = await _hl.GetListaShort(Ruc, Cia, null);

                GlobalesPedido globales = new GlobalesPedido()
                {
                    ListaVendedor = ListaVendedor,
                    ListaAgencias = ListaAgencia,
                };

                return Ok(globales);
            }
            catch (Exception e)
            {
                return BadRequest(new Error { Status_code = "-1", Status_message = e.Message });
            }
        }

        [HttpGet]
        public async Task<IActionResult> ListarPedidos([FromQuery] RequestListaPedido request)
        {
            ResponseListaPedido response = new ResponseListaPedido();

            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                response = await _hl.ListarPedidosConsola(request);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }

            return Ok(response);
        }

        [HttpGet]
        public async Task<IActionResult> ListarDetallePedido([FromQuery] RequestPedido request)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                if (string.IsNullOrEmpty(request.Agencia))
                {
                    return BadRequest(new RsResponse() { StatusCode = "400", StatusMessage = "Código de Agencia es requerido." });
                }

                if (string.IsNullOrEmpty(request.Numero))
                {
                    return BadRequest(new RsResponse() { StatusCode = "400", StatusMessage = "Número de Pedido es requerido." });
                }

                ResponseDetallePedido response = await _hl.ListarDetallePedido(request);
                return Ok(response);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }

        }
        #endregion

        [HttpPost]
        public async Task<IActionResult> ObtenerTotalCias([FromBody] FWCIAS_REQUEST FWCIAS_REQUEST)
        {
            // var Claims = (GLOBALES)HttpContext.Items["User"];

            //var FWCIAS = await _compania.ObtenerTotalCias(FWCIAS_REQUEST.FWCCRUC, FWCIAS_REQUEST.FWCCRAZON, FWCIAS_REQUEST.FWCFACTIVO);

            //return Ok(FWCIAS);

            try
            {
                List<FWCIAS> response = await _hl.ObtenerTotalCias(FWCIAS_REQUEST);
                return Ok(new ErrorR<FWCIAS_RESPONSE> { Status_code = "0", Status_message = "OK", Data = new FWCIAS_RESPONSE() { Lista = response, Total = response.Count } });
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }

        [HttpPost]
        public async Task<IActionResult> DescargarArchivo([FromBody] DOCUMENTO_REQUEST DOCUMENTO_REQUEST)
        {

            string Base64 = string.Empty;

            try
            {
                byte[] FileZip = await _hl.GetZipXML(DOCUMENTO_REQUEST);

                if (FileZip == null)
                {
                    if (DOCUMENTO_REQUEST.TIPOXML == "CDR")
                    {
                        return BadRequest("No se encontro archivo CDR.");

                    }
                    else
                    {
                        return BadRequest("No se encontro archivo XML.");

                    }
                }
                else
                {
                    Base64 = Convert.ToBase64String(FileZip);
                }
            }
            catch (Exception ex)
            {
                return BadRequest("ERROR BUSCANDO XML:" + ex.Message);
            }

            return Ok(Base64);
        }

        [HttpGet]
        public async Task<IActionResult> FacturaCDR([FromQuery] REIMPRESION_DETALLE_REQUEST REIMPRESION_DETALLE_REQUEST)
        {
            try
            {

                var Lista = await _hl.FacturaCDR(REIMPRESION_DETALLE_REQUEST);

                return Ok(Lista);

            }
            catch (Exception ex)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = ex.Message });
            }
        }
    }
}
