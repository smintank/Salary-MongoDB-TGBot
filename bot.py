import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

from config import NAME_LINK, TG_BOT_TOKEN
from salary import salary_proccessing

dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {html.link(message.from_user.full_name, NAME_LINK)}!")


@dp.message()
async def message_handler(message: Message) -> None:
    return await salary_proccessing(message)


async def main() -> None:
    bot = Bot(token=TG_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())

