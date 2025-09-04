from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from decimal import Decimal


# Pydantic model for gtim_osticket table
class gtim_osticket(BaseModel):
    numero_ticket: str
    fecha_creacion: datetime
    asunto: Optional[str]
    usuario: Optional[str]
    usuario_correo: Optional[str]
    prioridad: Optional[str]
    departamento: Optional[str]
    tema_ayuda: Optional[str]
    fuente: Optional[str]
    estado_actual: Optional[str]
    ultima_actualizacion: Optional[datetime]
    fecha_expiracion_sla: Optional[datetime]
    plan_sla: Optional[str]
    fecha_vencimiento: Optional[datetime]
    fecha_cierre: Optional[datetime]
    agente_asignado: Optional[str]
    equipo_asignado: Optional[str]
    fecha_creacion_mx: Optional[datetime]
    fecha_cierre_mx: Optional[datetime]
    sla_elapsed_time_minutes: Optional[Decimal]
    sla_elapsed_time_hours: Optional[Decimal]
    sla_met: Optional[bool]
