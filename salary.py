import ast

from aiogram.types import Message

from config import WRONG_MESSAGE
from db import get_documents


def split_message(message, max_length=4096):
    return [message[i:i + max_length] for i in range(0, len(message), max_length)]


async def salary_proccessing(message: Message) -> None:
    try:
        json_message = ast.literal_eval(message.text)
        _ = json_message["dt_from"]
        _ = json_message["dt_upto"]
        _ = json_message["group_type"]
        answer = await get_documents(json_message)
        for part in split_message(str(answer)):
            await message.answer(part)
    except Exception:
        await message.answer(WRONG_MESSAGE)
