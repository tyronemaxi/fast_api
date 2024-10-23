#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Author: tyrone
File: conf.py
Time: 2024/6/10
"""
import os

from dotenv import load_dotenv

load_dotenv()

# [openai]
api_base = os.getenv("api_base")
api_key = os.getenv("api_key")
