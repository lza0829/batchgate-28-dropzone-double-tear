from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import dropzone_service
from app.services.dropzone_service import DropzoneError

router = APIRouter(tags=["dropzone"])


class DropzoneConfigBody(BaseModel):
    dir: str


def _guard(fn, *args):
    try:
        return fn(*args)
    except DropzoneError as exc:
        raise HTTPException(exc.status_code, str(exc)) from exc


@router.get("/dropzone/config")
def get_dropzone_config():
    return dropzone_service.get_config()


@router.put("/dropzone/config")
def put_dropzone_config(body: DropzoneConfigBody):
    return _guard(dropzone_service.save_config, body.dir)


@router.post("/dropzone/scan")
def scan_dropzone():
    return dropzone_service.scan_dropzone()


@router.get("/dropzone/batches")
def list_batches():
    return dropzone_service.list_batches()


@router.get("/dropzone/batches/{batch_id}/events")
def list_batch_events(batch_id: int):
    return _guard(dropzone_service.list_events, batch_id)


@router.post("/dropzone/batches/{batch_id}/trigger")
def trigger_batch(batch_id: int):
    return _guard(dropzone_service.confirm_trigger, batch_id)


@router.post("/dropzone/batches/{batch_id}/retrigger")
def retrigger_batch(batch_id: int):
    return _guard(dropzone_service.retrigger, batch_id)


@router.post("/dropzone/batches/{batch_id}/ignore")
def ignore_batch(batch_id: int):
    return _guard(dropzone_service.ignore_batch, batch_id)


@router.get("/dropzone/reconcile")
def reconcile_dropzone():
    return dropzone_service.reconcile()
