#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: tyrone
File: llm_cli.py
Time: 2024/6/10
"""
import ujson
import asyncio
from typing import Dict, Any, List

from langchain.callbacks import AsyncIteratorCallbackHandler
from langchain_core.outputs import LLMResult
from langchain.schema import HumanMessage
from langchain_openai import ChatOpenAI

from conf.conf import OPENAI_API_KEY, OPENAI_API_BASE


class CustomAsyncStreamHandler(AsyncIteratorCallbackHandler):
    async def on_llm_start(
        self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any
    ) -> None:
        self.done.clear()

    async def on_llm_new_token(self, token: str, **kwargs: Any) -> None:
        if token is not None and token != "":
            self.queue.put_nowait(token)

    async def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        self.done.set()

    async def on_llm_error(self, error: BaseException, **kwargs: Any) -> None:
        self.done.set()


async def call_llm(message: str):
    msg = [HumanMessage(content=message)]
    _callback = CustomAsyncStreamHandler()

    llm = ChatOpenAI(
        streaming=True,
        callbacks=[_callback],
        api_key=OPENAI_API_KEY,
        base_url=OPENAI_API_BASE,
    )

    task = asyncio.create_task(llm.ainvoke(msg))

    async for token in _callback.aiter():
        data = {"text": token, "data_type": "text"}
        j_data = ujson.dumps(data, ensure_ascii=False)
        yield f"data: {j_data}\n\n"

    await task


async def main():
    message = "python 是什么"
    async for token in call_llm(message):
        print(token, end="", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
