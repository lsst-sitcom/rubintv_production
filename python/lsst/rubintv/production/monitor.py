from typing import Union, Dict, List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
from pydantic import BaseModel
import json

from jinja2 import Environment, PackageLoader

from lsst.rubintv.production.utils import LocationConfig
from lsst.rubintv.production.redisUtils import RedisHelper
from lsst.rubintv.production.podDefinition  import PodFlavor
import lsst.daf.butler as dafButler


env = Environment(
    loader=PackageLoader("lsst.rubintv.production", "assets"),
)

app = FastAPI()

location = 'usdf'
instrument = 'LSSTCam'
locationConfig = LocationConfig(location)
butler = dafButler.Butler("embargo", instrument="LSSTCam",
                          collections=["LSSTCam/runs/nightlyValidation"])

helper = RedisHelper(butler, locationConfig)


# app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def front_page():

    sfm_workers = helper.getAllWorkers(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER)

    max_depth = max((worker.depth for worker in sfm_workers), default=0)

    per_depth_status = {}

    free_sfm_workers = helper.getFreeWorkers(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER)

    for depth in range(max_depth + 1):
        detector_numbers_all = set(x.detectorNumber for x in sfm_workers if x.depth == depth)
        detector_numbers_free = set(x.detectorNumber for x in free_sfm_workers if x.depth == depth)

        depth_status = {}
        for n in range(205):
            if(n in detector_numbers_free):
                depth_status[n] = "free"
            elif(n in detector_numbers_all):
                depth_status[n] = "busy"
            else:
                depth_status[n] = "missing"

        per_depth_status[depth] = depth_status

    template = env.get_template("front_page.html")

    return template.render(max_depth=max_depth, per_depth_status=per_depth_status)




