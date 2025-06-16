using Microsoft.AspNetCore.Mvc;
using Org.BouncyCastle.Crypto.Tls;
using RS_FacturadorWeb.ClienteJWT;
using RS_FacturadorWeb.EntityJWT;
using RSFacLocal.Entities.REPORTE;
using RSFacLocal.ModelsView.FWFTCABF;
using RSFacLocal.ModelsView.REPORTES;
using RSFacWeb.Entities;
using RSFacWeb.Entities.FWCIAPLAN;
using RSFacWeb.Entities.FWUSUCIA;
using RSFacWeb.Entities.PLANES;
using RSFacWeb.Interfaces;
using RSFacWeb.ModelsView;
using RSFacWeb.ModelsView.FELOG;
using RSFacWeb.ModelsView.FWCIAS;
using RSFacWeb.ModelsView.FWCNFG;
using RSFacWeb.ModelsView.PLAN;
using RSFacWeb.ModelsView.PLANCIA;
using RSFacWeb.Util;
using RSFacWeb.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using FWCNFG = RSFacWeb.Entities.FWCNFG;

namespace RSFacWeb.Controllers
{
    [Produces("application/json")]
    [Route("api/auth/v1/facweb/compania/[action]")]
    [Authorize]
    [ApiController]
    public class CompaniaController : RsControllerBase
    {
        private readonly ICompaniaDAL _compania;

        public CompaniaController(ICompaniaDAL compania)
        {
            _compania = compania;
        }

        [HttpGet]
        public async Task<IActionResult> ObtenerCias()
        {
            var Claims = (GLOBALES)HttpContext.Items["User"];

            var C_GLOBALES_CIA = new C_GLOBALES_CIA();

            var FWCIAS_LOGIN = await _compania.ObtenerCias(Int32.Parse(Claims.US_CIDUSU));

            
            #region VALIDA SI EXISTE UNA LISTA DE COMPAÑIAS POR USUARIO
            if (!FWCIAS_LOGIN.Any())
            {
                var Error = FWMSGERR.ER_API_A104;
                C_GLOBALES_CIA.Status_code = Error[0];
                C_GLOBALES_CIA.Status_message = Error[1];
                return BadRequest(C_GLOBALES_CIA);
            }
            #endregion

            var FWCIAS_FCHAVEN = await _compania.ObtenerCiasFcha(Int32.Parse(Claims.US_CIDUSU));


            #region VALIDA SI LA COMPAÑIA HA VENCIDO
            if (!FWCIAS_FCHAVEN.Any())
            {
                var Error = FWMSGERR.ER_API_A108;
                C_GLOBALES_CIA.Status_code = Error[0];
                C_GLOBALES_CIA.Status_message = Error[1];
                return BadRequest(C_GLOBALES_CIA);
            }
            #endregion

            var FWCIAS_LISTAUACTIVOS = await _compania.ObtenerUsuariosActivos(FWCIAS_LOGIN[0].PUCODCIA);

            C_GLOBALES_CIA.GLOBALES_CIA = FWCIAS_LOGIN;
            C_GLOBALES_CIA.USUARIOS_ACTIVOS = FWCIAS_LISTAUACTIVOS;

            C_GLOBALES_CIA.Status_code = "0";

            return Ok(C_GLOBALES_CIA);
        }


        //[HttpGet]
        //public async Task<IActionResult> GetConfigCabFacturacion([FromQuery] FWFTCABF FWFTCABF)
        //{
        //    var CABECERA_FACTURACION = new CABECERA_FACTURACION();

        //    var LST_FWFTCABF = await _compania.GetConfigCabFacturacion(FWFTCABF.CF_CCODCIA, FWFTCABF.CF_CPROGRA);

        //    #region VALIDA SI EXISTE UNA LISTA
        //    if (LST_FWFTCABF.Count < 1)
        //    {
        //        var Error = FWMSGERR.ER_API_A106;
        //        CABECERA_FACTURACION.Status_code = Error[0];
        //        CABECERA_FACTURACION.Status_message = Error[1];
        //        return BadRequest(CABECERA_FACTURACION);
        //    }
        //    #endregion

        //    CABECERA_FACTURACION.LIST_FWFTCABF = LST_FWFTCABF;
        //    CABECERA_FACTURACION.Status_code = "0";

        //    return Ok(CABECERA_FACTURACION);
        //}

        [HttpPost]
        public async Task<ActionResult> GetListaFWTCABF([FromBody] RSFacLocal.ModelsView.FWFTCABF.ListaRequest request)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                List<FWFTCABF> response = await _compania.GetListaFWTCABF(request);

                return Ok(new ErrorR<ListaResponse> { Status_code = "0", Status_message = "OK", Data = new ListaResponse() { Lista = response, Total = response.Count } });
            }
            catch (System.Exception ex)
            {
                return BadRequest(new ErrorR<RSFacLocal.ModelsView.FWFTCABF.Transaccion> { Status_code = "400", Status_message = ex.Message, Data = new RSFacLocal.ModelsView.FWFTCABF.Transaccion() { Execute = false } });
            }

        }

        [HttpPost]
        public async Task<ActionResult> GetListaCombo([FromBody] RSFacLocal.ModelsView.FWFTCABF.ListaRequest request)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];
                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

                List<FWFTCABF> response = await _compania.GetListaCombo(request);

                return Ok(new ErrorR<ListaResponse> { Status_code = "0", Status_message = "OK", Data = new ListaResponse() { Lista = response, Total = response.Count } });
            }
            catch (System.Exception ex)
            {
                return BadRequest(new ErrorR<RSFacLocal.ModelsView.FWFTCABF.Transaccion> { Status_code = "400", Status_message = ex.Message, Data = new RSFacLocal.ModelsView.FWFTCABF.Transaccion() { Execute = false } });
            }

        }

        [HttpPost]
        public async Task<IActionResult> ObtenerTotalCias([FromBody] FWCIAS_REQUEST FWCIAS_REQUEST)
        {
            // var Claims = (GLOBALES)HttpContext.Items["User"];

            //var FWCIAS = await _compania.ObtenerTotalCias(FWCIAS_REQUEST.FWCCRUC, FWCIAS_REQUEST.FWCCRAZON, FWCIAS_REQUEST.FWCFACTIVO);

            //return Ok(FWCIAS);

            try
            {
                List<FWCIAS> response = await _compania.ObtenerTotalCias(FWCIAS_REQUEST);
                return Ok(new ErrorR<FWCIAS_RESPONSE> { Status_code="0", Status_message="OK", Data = new FWCIAS_RESPONSE() { Lista = response, Total = response.Count } });
            } catch (Exception e) { 
                return BadRequest(e.Message);
            }
        }
        [HttpPost]
        public async Task<IActionResult> ObtenerLog([FromBody] FELOG_REQUEST FELOG_REQUEST)
        {
            // var Claims = (GLOBALES)HttpContext.Items["User"];

            //var FWCIAS = await _compania.ObtenerTotalCias(FWCIAS_REQUEST.FWCCRUC, FWCIAS_REQUEST.FWCCRAZON, FWCIAS_REQUEST.FWCFACTIVO);

            //return Ok(FWCIAS);

            try
            {
                List<FELOG> response = await _compania.ObtenerLog(FELOG_REQUEST);
                return Ok(new ErrorR<FELOG_RESPONSE> { Status_code = "0", Status_message = "OK", Data = new FELOG_RESPONSE() { Lista = response, Total = response.Count } });
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }
        [HttpGet]
        public async Task<IActionResult> ObtenerCiaAsigUsu(string UC_CIDUSU)
        {
            try
            {
                var result = await _compania.ObtenerCiaAsigUsu(UC_CIDUSU);

                if (result == null || !result.Any())
                {
                    return NotFound("No se encontraron datos.");
                }

                return Ok(result);
            }
            catch (Exception ex)
            {
                // Manejo de errores
                return StatusCode(500, "Error interno del servidor.");
            }
        }

        [HttpGet]
        public async Task<IActionResult> ObtenerTotalUsuarios(string UC_CCODCIA)
        {
            //var Claims = (GLOBALES)HttpContext.Items["User"];

            var FWUSERS = await _compania.ObtenerTotalUsuarios(UC_CCODCIA);

            return Ok(FWUSERS);
        }

        [HttpGet]
        public async Task<IActionResult> ObtenerAgencias(string Id, string Cia  )
        {
            //var Claims = (GLOBALES)HttpContext.Items["User"];

            var FTAGEN = await _compania.ObtenerAgencias(Id, Cia);

            return Ok(FTAGEN);
        }

        [HttpGet]
        public async Task<IActionResult> ObtenerTotalCabeceraConfig(string UC_CCODCIA)
        {
            //var Claims = (GLOBALES)HttpContext.Items["User"];

            var FWFTCABF = await _compania.ObtenerTotalCabeceraConfig(UC_CCODCIA);

            return Ok(FWFTCABF);
        }

        [HttpGet]
        public async Task<IActionResult> ObtenerTotalUsuariosNoAsociado (string searchTerm, string UC_CCODCIA)
        {
            //var Claims = (GLOBALES)HttpContext.Items["User"];

            var FWUSERS = await _compania.ObtenerTotalUsuariosNoAsociado(searchTerm, UC_CCODCIA);

            return Ok(FWUSERS);
        }

        [HttpGet]
        public async Task<IActionResult> ObtenerTotalConfigUsuario(string US_CIDUSU, string US_CCODAMB)
        {
            //var Claims = (GLOBALES)HttpContext.Items["User"];

            var FWCNFG = await _compania.ObtenerTotalConfigUsuario(US_CIDUSU, US_CCODAMB);

            return Ok(FWCNFG);
        }

        [HttpPost]
        public async Task<IActionResult> RegistrarCia(FWCIAS FWCIAS)
        {
            var Error = new Error();

            try
            {
                var Existencia = await _compania.ValidarExistenciaCia(FWCIAS.CI_CRUC);

                if (Existencia > 0)
                {
                    Error.Status_code = "-1";
                    Error.Status_message = "El RUC ya se encuentra registrado.";
                }
                else
                {
                    Error = await _compania.RegistrarCia(FWCIAS);
                }

                return Ok(Error);
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                Error.Status_message = e.Message;
                return BadRequest(Error);
            }
        }

        [HttpPost]
        public async Task<IActionResult> ActualizarCia(FWCIAS FWCIAS)
        {
            var Error = new Error();

            try
            {
                Error = await _compania.ActualizarCia(FWCIAS);
                return Ok(Error);
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                Error.Status_message = e.Message;
                return BadRequest(Error);
            }
        }

        [HttpPost]
        public async Task<IActionResult> ActualizarModod(FWCIAS FWCIAS)
        {
            var Error = new Error();

            try
            {
                Error = await _compania.ActualizarModod(FWCIAS);
                return Ok(Error);
            }
            catch (Exception e)
            {
                Error.Status_code = "-1";
                Error.Status_message = e.Message;
                return BadRequest(Error);
            }
        }

        [HttpPost]
        public async Task<IActionResult> UsuarioCrear(FWUSERS FWUSERS)
        {
            var Error = new Error();
            try
            {
                if (await _compania.UsuarioEmailRepetido(FWUSERS))
                {
                    Error.Status_code = "1";
                    Error.Status_message = "El email " + FWUSERS.US_CEMAIL + " ya se encuentra registrado en el sistema.";
                    return Ok(Error);
                }
                else
                {
                    Error = await _compania.UsuarioCrear(FWUSERS);
                    Error.Status_message = "Se registró con éxito el usuario: " + FWUSERS.US_CEMAIL;
                    return Ok(Error);
                }
            }
            catch (Exception e)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = e.Message });
            }
        }

        [HttpPut]
        public async Task<IActionResult> UsuarioActualizar(FWUSERS FWUSERS)
        {
            var Error = new Error();
            try
            {
                Error = await _compania.UsuarioActualizar(FWUSERS);
                Error.Status_message = "Se actualizó con éxito el usuario: " + FWUSERS.US_CEMAIL;
                return Ok(Error);
            }
            catch
            {
                return BadRequest();
            }
        }

        [HttpGet]
        public async Task<IActionResult> UsuarioListar([FromQuery] FWUSERS_LISTA_REQUEST FWUSERS_LISTA_REQUEST)
        {
            try
            {
                var FWCIAS = await _compania.UsuarioListar(FWUSERS_LISTA_REQUEST);

                return Ok(FWCIAS);
            }
            catch (Exception e)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = e.Message });
            }
        }

        [HttpGet]
        public async Task<IActionResult> UsuarioEmailRepetido([FromQuery] FWUSERS FWUSERS)
        {
            try
            {
                bool estado = await _compania.UsuarioEmailRepetido(FWUSERS);

                return Ok(estado);
            }
            catch (Exception e)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = e.Message });
            }
        }


        // Impresion / Logo

        [HttpPost]
        public async Task<ActionResult> Actualizar([FromBody] FWCIAS impresion)
        {

            impresion.CI_CLOGOTKB64 = ConvertHelper.ToNonNullString(impresion.CI_CLOGOTKB64);

            if (impresion.CI_CLOGOTKB64 == "")
            {
                string msg = "No a seleccionado una imagen.";
                return BadRequest(new Error() { Status_code = "400", Status_message = msg });
            }

            if (await _compania.Actualizar(impresion) == false)
            {
                return BadRequest(new Error() { Status_code = "400", Status_message = "No realizo la inserción." });
            }
            return Ok(new Error() { Status_code = "0", Status_message = "Logo actualizado con exito." });
        }

        [HttpPost]
        public async Task<ActionResult> GetLista([FromBody] FWCIAS impresion)
        {

            var response = await _compania.GetLista(impresion);
            return Ok(new { response });
        }

        [HttpPost]
        public ActionResult UUIDPfx([FromBody] FWCIAS pfx)
        {

            var response = _compania.UUIDPfx(pfx);
            return Ok(response);
        }

        #region Obtener Pfx
        [HttpGet]
        public async Task<ActionResult> ObtenerPfx(string Id, string Cia)
        {
            try
            {
                //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];
                //var PUCODEMP = Claims.AC_CCIA;
                //var PUCBDFACCAR = Claims.CI_CDBFACCAR;
                var response = await _compania.ObtenerPfx(Id, Cia);
                return Ok(response);
            }
            catch (Exception e)
            {
                return BadRequest(e);
            }


        }
        #endregion

        [HttpGet]
        public async Task<ActionResult> GetListReporteRapidos([FromQuery]string ruc, string cia, string pcuser)
        {
            //var Claims = (ALCIAS_LOGIN)HttpContext.Items["User"];

            //var PUCODEMP = Claims.AC_CCIA;
            //var PUCBDFACCAR = Claims.CI_CDBFACCAR;

            List<REPORAPIDOS> response = await _compania.GetListReporteRapidos( ruc, cia, pcuser);

            return Ok(new ErrorR<ListaResponse<REPORAPIDOS>>
            {
                Status_code = "0",
                Status_message = "OK",
                Data = new ListaResponse<REPORAPIDOS>()
                {
                    Lista = response ?? new List<REPORAPIDOS>(),  // Evita nulos
                    Total = response?.Count ?? 0  // Evita error si response es null
                }
            });

        }

        [HttpGet]
        public async Task<IActionResult> GetCertificados()
        {
            // var Claims = (GLOBALES)HttpContext.Items["User"];

            //var FWCIAS = await _compania.ObtenerTotalCias(FWCIAS_REQUEST.FWCCRUC, FWCIAS_REQUEST.FWCCRAZON, FWCIAS_REQUEST.FWCFACTIVO);

            //return Ok(FWCIAS);

            try
            {
                List<FWCIAS> response = await _compania.GetCertificados();
                return Ok(new ErrorR<FWCIAS_RESPONSE> { Status_code = "0", Status_message = "OK", Data = new FWCIAS_RESPONSE() { Lista = response, Total = response.Count } });
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }

        [HttpGet]
        public async Task<IActionResult> ListarCiaPlan([FromQuery] string ruc)
        {
            try
            {
                List<FWCIAPLAN> response = await _compania.ListarCiaPlan(ruc);
                return Ok(new RESPONSE_CIA_PLANES { Planes = response, Total = response.Count });
            }
            catch (Exception e)
            {
                return BadRequest(e.Message);
            }
        }

        [HttpGet]
        public async Task<IActionResult> ListarModuloPlan([FromQuery] string plan)
        {
            try
            {
                // Validar que el parámetro no sea null o vacío
                if (string.IsNullOrWhiteSpace(plan))
                {
                    return BadRequest("El parámetro 'plan' es requerido y no puede estar vacío.");
                }

                List<FWCNFG> response = await _compania.ListarModuloPlan(plan);
                return Ok(new ListaReponse_FWCNFG { Lista = response, Total = response.Count });
            }
            catch (Exception e)
            {
                return BadRequest($"Error al obtener módulos del plan: {e.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> AsociarPlan([FromBody] REQUEST_CIA_PLANES request)
        {
            // Validar la entrada
            if (request == null)
            {
                return BadRequest(new { success = false, mensaje = "Los datos del plan son necesarios." });
            }

            try
            {
                // Llamar al servicio para cambiar el plan
                var resultado = await _compania.AsociarPlan(request);

                if (resultado)
                {
                    // Si todo es correcto, respondemos con un 200 OK
                    return Ok(new { success = true, mensaje = "El plan ha sido asociado correctamente." });
                }
                else
                {
                    // Si algo falló, respondemos con un 500 Internal Server Error
                    return StatusCode(500, new { success = false, mensaje = "Ocurrió un error al asociar el plan." });
                }
            }
            catch (Exception ex)
            {
                // En caso de excepción, devolvemos un error 500 con el mensaje de la excepción
                return StatusCode(500, new { success = false, mensaje = ex.Message });
            }
        }

        [HttpPost]
        public async Task<IActionResult> CambiarPlan([FromBody] REQUEST_CIA_PLANES request)
        {
            // Validar la entrada
            if (request == null)
            {
                return BadRequest(new { success = false, mensaje = "Los datos del plan son necesarios." });
            }

            try
            {
                // Llamar al servicio para cambiar el plan
                var resultado = await _compania.CambiarPlan(request);

                if (resultado)
                {
                    // Si todo es correcto, respondemos con un 200 OK
                    return Ok(new { success = true, mensaje = "El plan ha sido cambiado correctamente." });
                }
                else
                {
                    // Si algo falló, respondemos con un 500 Internal Server Error
                    return StatusCode(500, new { success = false, mensaje = "Ocurrió un error al cambiar el plan." });
                }
            }
            catch (Exception ex)
            {
                // En caso de excepción, devolvemos un error 500 con el mensaje de la excepción
                return StatusCode(500, new { success = false, mensaje = ex.Message });
            }
        }

        [HttpPost]
        public async Task<IActionResult> ActualizarLimites([FromBody] List<REQUEST_ACTUALIZAR_LIMITE> request)
        {
            if (request == null || request.Count == 0)
            {
                return BadRequest("Los datos son requeridos.");
            }

            try
            {
                bool resultado = await _compania.ActualizarLimites(request);

                if (resultado)
                {
                    return Ok(new { mensaje = "Los límites han sido actualizados correctamente." });
                }
                else
                {
                    return StatusCode(500, new { mensaje = "Error al actualizar los límites." });
                }
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { mensaje = ex.Message });
            }
        }

        [HttpGet]
        public async Task<IActionResult> ObtenerLimitacionesPorPlan(string planCodigo)
        {
            if (string.IsNullOrEmpty(planCodigo))
                return BadRequest("El código del plan es requerido.");

            try
            {
                var limitaciones = await _compania.ObtenerLimitacionesPorPlan(planCodigo);

                if (limitaciones == null || limitaciones.Count == 0)
                    return NotFound("No se encontraron limitaciones para este plan.");

                return Ok(limitaciones);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error en el controlador al obtener limitaciones: {ex.Message}");
                return StatusCode(500, "Ocurrió un error en el servidor.");
            }
        }




    }
}
