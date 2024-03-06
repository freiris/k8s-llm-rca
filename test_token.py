#!/usr/bin/env python

import os
import openai
import time
import json
import neo4j
from openai import OpenAI

from common.openai_generic_assistant import OpenAIGenericAssistant


instructions = "You are a personal math tutor. When asked a question, write and run Python code to answer the question."

name = 'math-tutor-2'


math_tutor = OpenAIGenericAssistant()

math_tutor.create_assistant(instructions, name, 'gpt-4')

math_tutor.create_thread()

print(math_tutor.assistant.id)

print(math_tutor.thread.id)

url = f"https://platform.openai.com/playground?assistant={math_tutor.assistant.id}&thread={math_tutor.thread.id}"

print(url)

messages = ["what is the area of a circle with diameter 4",
            "what is the result for x in 'x + 3 = 15'?"]

start_time = int(time.time())

for message in messages:
    math_tutor.add_message(message)
    math_tutor.run_assistant()
    print('run assistant ...')
    time.sleep(60)

finish_time = int(time.time())

token_cost = math_tutor.get_token_usage(start_time, finish_time, 5)

print(token_cost)


