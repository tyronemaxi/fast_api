#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: tianzhichao
File: server.py
Time: 2024/5/13 1:37 PM
"""
from pydantic import BaseModel

import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from app.client.llm_cli import call_llm

app = FastAPI()


class StreamRequest(BaseModel):
    """
    Request body for streaming
    """
    message: str


@app.post("/chat")
def stream(body: StreamRequest):
    return StreamingResponse(call_llm(body.message), media_type="text/event-stream")


if __name__ == '__main__':
    uvicorn.run(app=app, port=8080, host='0.0.0.0')
