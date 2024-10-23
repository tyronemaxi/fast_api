#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: tianzhichao
File: server.py
Time: 2024/5/13 1:37 PM
"""
from typing import AsyncGenerator

from pydantic import BaseModel

import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from app.controller.chat.completions import chat_ctrl

app = FastAPI()


# 定义数据模型
class Item(BaseModel):
    query: str
    stream: bool = True


@app.post("/chat")
async def chat_endpoint(item: Item):
    async def event_stream() -> AsyncGenerator[str, None]:
        async for response in chat_ctrl.chat(
            item.query, "1111", "2222", "33333", item.stream
        ):
            yield f"data: {response}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(app=app, port=8080, host="0.0.0.0")
