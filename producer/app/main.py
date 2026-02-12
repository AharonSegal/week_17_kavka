"""
THE PRODUCER APP
this gets data from mogodb 
and send it to kafka
    the sending is done in batches of 30 
    and during each batch sends each document individually in 0.5 second intervals
"""

from fastapi import FastAPI
from router import router

app = FastAPI()
app.include_router(router)