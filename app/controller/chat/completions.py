#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: tyrone
File: completions.py
Time: 2024/10/23
"""
from components.all_in_one.chain import LLMChainBase
from components.all_in_one.cli import LLMClientAPI
from components.all_in_one.promt_template import SystemMessage, UserMessage
from components.all_in_one.tool import TokensCalTool, TimeCalculation

from conf.conf import api_base, api_key


class ChatCtrl(object):
    async def chat(
        self, query: str, conv_id: str, cp_id: str, user_id: str, stream: bool
    ):
        llm = LLMClientAPI(api_base=api_base, api_key=api_key)

        tools = [TokensCalTool(), TimeCalculation()]

        chain = LLMChainBase(llm, tools)

        system_prompt_template = SystemMessage(content=self._system_prompt())
        user_prompt_template = UserMessage(content=query)

        messages = [system_prompt_template, user_prompt_template]

        async for response in chain.ainvoke(
            conv_id=conv_id,
            cp_id=cp_id,
            user_id=user_id,
            messages=messages,
            model="gpt-4",
            max_tokens=1024,
            chat_mode="aichat",
            temperature=0.5,
            stream=stream,
        ):
            yield response

    @staticmethod
    def _system_prompt():
        return """
            你是由 tyrone 开发的百科全书式问答助手，你谦虚，幽默，富有诗人浪漫的魅力。
        """


chat_ctrl = ChatCtrl()
