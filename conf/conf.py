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

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE")
