import pika
import json
import asyncio
from telegram import Bot
import time


BOT_TOKEN = ""

YOUR_TELEGRAM_ID = 

bot = Bot(token=BOT_TOKEN)


def connect_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='lecture_queue', durable=True)
    return connection, channel


async def check_and_send_messages():
    try:
        connection, channel = connect_rabbitmq()


        while True:
            method_frame, header_frame, body = channel.basic_get(queue='lecture_queue', auto_ack=True)

            if method_frame:

                message_data = json.loads(body.decode())

                formatted_message = f"Викладач {message_data['username']} почав віддалену пару і {message_data['text']}"

                await bot.send_message(chat_id=YOUR_TELEGRAM_ID, text=formatted_message)
                print(f"Отправлено сообщение: {formatted_message}")
            else:

                break

        connection.close()

    except Exception as e:
        print(f"Ошибка при проверке очереди: {e}")


async def periodic_check():
    while True:
        print("Перевірка черги RabbitMQ...")
        await check_and_send_messages()
        print("Очікування 10с...")
        await asyncio.sleep(10)


def main():
    print("Бот 2 запущен...")
    asyncio.run(periodic_check())


if __name__ == '__main__':
    main()
