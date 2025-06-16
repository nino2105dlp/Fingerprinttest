using System.Collections.Generic;

namespace RSFacLocal.ModelsView.REPORTES
{
    public class ListaResponse<T>
    {
        public List<T> Lista { get; set; }
        public int Total { get; set; }
    }
}
