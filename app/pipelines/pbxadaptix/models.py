from pydantic import BaseModel
from datetime import datetime


# Pydantic model for gtim_pbx_adaptix_calls table
class GTIMPBXAdaptixCalls(BaseModel):
    record_id: str
    process_id: str
    fecha: datetime
    caller_id: str
    grupo_timbrado: str
    destino: str
    canal_origen: str
    codigo_cuenta: str
    canal_destino: str
    estado: str
    duracion: str
    duracion_seg: int
    extension: str
    created_at: datetime
    updated_at: datetime
