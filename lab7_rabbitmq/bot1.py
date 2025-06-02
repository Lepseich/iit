import pika
import json
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes


BOT_TOKEN = ""


def connect_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='lecture_queue', durable=True)
    return connection, channel


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Привіт! Надішли мені повідомлення, і я передам його через RabbitMQ.')


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:

        connection, channel = connect_rabbitmq()


        message_data = {
            'text': update.message.text,
            'user_id': update.effective_user.id,
            'username': update.effective_user.username or update.effective_user.first_name
        }


        channel.basic_publish(
            exchange='',
            routing_key='lecture_queue',
            body=json.dumps(message_data),
            properties=pika.BasicProperties(delivery_mode=2) 
        )

        connection.close()
        await update.message.reply_text('Повідомлення відправлено в чергу! RabbitMQ!')

    except Exception as e:
        await update.message.reply_text(f'Помилка: {str(e)}')


def main():
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Бот 1 запущен...")
    application.run_polling()


if __name__ == '__main__':
    main()
