using Google.Protobuf;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using RS_FacturadorWeb.ClienteJWT;
using RS_FacturadorWeb.EntityJWT;
using RSFacWeb.Entities.MKTPUBLIC;
using RSFacWeb.Interfaces;
using RSFacWeb.ModelsView;
using RSFacWeb.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;

namespace RSFacWeb.Controllers
{
    [Produces("application/json")]
    [Route("api/auth/v1/facweb/mkt/[action]")]
    [Authorize]
    [ApiController]
    public class MktController : RsControllerBase
    {
        private readonly IMktDAL _mkt;
        private readonly IConfiguration _configuration;
        public MktController(IMktDAL mkt, IConfiguration configuration)
        {
            _mkt = mkt;
            _configuration = configuration;
        }

        [HttpGet]
        public async Task<IActionResult> ListarTipo(string tipo)
        {
            var Claims = (GLOBALES)HttpContext.Items["User"];

            try
            {
                var result = await _mkt.ListarTipo(tipo);

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

                // Obtener el ambiente del usuario que consume el endpoint
                string Ambiente = Claims.PUCCODAMB?.ToLower();

                if (string.IsNullOrEmpty(Ambiente))
                {
                    return StatusCode(500, "El ambiente del usuario no está definido.");
                }

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
                Path.Combine(WorkDir, Ambiente, "assets", "banner", nuevoNombreArchivoban),
                Path.Combine(WorkDir, Ambiente, "assets", "popup", item.MK_CIMGURL),
                Path.Combine(WorkDir, Ambiente, "assets", "notification", nuevoNombreArchivonot)
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
                                if (ruta.Contains(Path.Combine(WorkDir, Ambiente, "assets", "popup")))
                                {
                                    item.MK_CIMGURL = Path.GetFileName(ruta); // Mantener el nombre original
                                }
                                else if (ruta.Contains(Path.Combine(WorkDir, Ambiente, "assets", "banner")))
                                {
                                    item.MK_CIMGURL = nuevoNombreArchivoban; // Nombre nuevo para banner
                                }
                                else if (ruta.Contains(Path.Combine(WorkDir, Ambiente, "assets", "notification")))
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
                return StatusCode(500, $"Error interno del servidor: {ex.Message}");
            }
        }


        [HttpPost]
        public async Task<IActionResult> Guardarpopup(MKTPUBLIC model)
        {
            var Claims = (GLOBALES)HttpContext.Items["User"];
            try
            {
                Error Error = await _mkt.Guardarpopup(model);

                if (Error.Status_code.Equals("0"))
                {
                    if (!string.IsNullOrEmpty(model.MK_CCONTENT))
                    {
                        //Obtener la imagen y convertirla a byte
                        byte[] ImagenBanner = Convert.FromBase64String(model.MK_CCONTENT);

                        //llamar al directorio principal de appsetting.json 
                        string WorkDir = _configuration["AppWorkDir"];

                        //Obtener el ambiente del usuario que consume el endpoint
                        string Ambiente = Claims.PUCCODAMB.ToLower();

                        // Construir ruta donde se guardará la imagen (incluyendo el nombre del archivo)
                        string nombreArchivo = model.MK_CIMGURL;
                        string RutaArchivo = Path.Combine(WorkDir, Ambiente, "assets", "popup", nombreArchivo);

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
        public async Task<IActionResult> CrearBanner(MKTPUBLIC model)
        {
            var Claims = (GLOBALES)HttpContext.Items["User"];

            try
            {
                Error Error = await _mkt.CrearBanner(model);

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
                        string nuevoNombreArchivo = $"banner-{uuid}{extension}";

                        // Construir ruta donde se guardará la imagen (incluyendo el nombre del archivo)
                        string RutaArchivo = Path.Combine(WorkDir, Ambiente, "assets", "banner", nuevoNombreArchivo);

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
        public async Task<IActionResult> EditarBanner(MKTPUBLIC model)
        {
            var Claims = (GLOBALES)HttpContext.Items["User"];

            try
            {
                Error Error = await _mkt.EditarBanner(model);

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
                        string nuevoNombreArchivo = $"banner-{uuid}{extension}";

                        // Construir ruta donde se guardará la imagen (incluyendo el nombre del archivo)
                        string RutaArchivo = Path.Combine(WorkDir, Ambiente, "assets", "banner", nuevoNombreArchivo);

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
        public async Task<IActionResult> ActualizarOrdenBanners([FromBody] List<BannerOrden> ordenes, string tipo)
        {
            try
            {
                var bannersActualizados = await _mkt.ActualizarOrdenBanners(ordenes, tipo);
                return Ok(bannersActualizados);
            }
            catch (Exception ex)
            {
                return BadRequest(new { message = ex.Message });
            }
        }

        [HttpPost]
        public async Task<IActionResult> PublishBanners([FromBody] List<BannerModel> banners)
        {
            var Error = new Error();
            try
            {
                // Pasar la lista completa al DAL
                Error = await _mkt.PublishBanners(banners);
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }
        [HttpPost]
        public async Task<IActionResult> CrearAlerta(MKTPUBLIC model)
        {
            var Error = new Error();
            try
            {
                Error = await _mkt.CrearAlerta(model);
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
                Error = await _mkt.EditarAlerta(model);
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
                Error = await _mkt.PublishAlert(model);
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
                Error Error = await _mkt.CrearNotification(model);

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
                        string RutaArchivo = Path.Combine(WorkDir, Ambiente, "assets", "notification", nuevoNombreArchivo);

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
                Error Error = await _mkt.EditarNotification(model);

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
                        string RutaArchivo = Path.Combine(WorkDir, Ambiente, "assets", "notification", nuevoNombreArchivo);

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
                Error = await _mkt.PublishNotification(model);
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
                Error = await _mkt.Eliminar(id);
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }
    }

}
