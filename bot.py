import os
import logging
import pandas as pd
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters, ConversationHandler
)

# Load environment variables
load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

RATING, YEAR, POPULARITY = range(3)

BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
MOVIES_DB_PATH = os.getenv('MOVIES_DB_PATH', 'moviedb.csv')
MAX_OVERVIEW_LENGTH = int(os.getenv('MAX_OVERVIEW_LENGTH', '400'))


def require_data(func):
    async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if self.movies_df is None or self.movies_df.empty:
            text = "⚠️ База фильмов не загружена. Попробуйте позже."
            if update.message:
                await update.message.reply_text(text)
            elif update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(text)
            return ConversationHandler.END
        return await func(self, update, context)
    return wrapper


class MovieRecommendationBot:
    def __init__(self):
        self.movies_df = pd.DataFrame()
        self.load_movies()

    def load_movies(self):
        try:
            if not os.path.isfile(MOVIES_DB_PATH):
                raise FileNotFoundError(f"Файл {MOVIES_DB_PATH} не найден.")
            df = pd.read_csv(MOVIES_DB_PATH)
            required_cols = {'title', 'overview', 'release_date', 'popularity', 'vote_average', 'vote_count'}
            missing = required_cols - set(df.columns)
            if missing:
                raise ValueError(f"Отсутствуют колонки: {missing}")
            df['year'] = pd.to_datetime(df['release_date'], errors='coerce').dt.year
            df.dropna(subset=['year'], inplace=True)
            if df.empty:
                raise ValueError("Пустой DataFrame после обработки.")
            self.movies_df = df
            logger.info(f"Загружено фильмов: {len(df)}")
        except Exception as e:
            logger.error(f"Ошибка загрузки фильмов: {e}")
            raise

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("🎬 Случайный фильм", callback_data='random')],
            [InlineKeyboardButton("⭐ По рейтингу", callback_data='rating')],
            [InlineKeyboardButton("📅 По году", callback_data='year')],
            [InlineKeyboardButton("🔥 По популярности", callback_data='popularity')],
            [InlineKeyboardButton("ℹ️ О боте", callback_data='about')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.message:
            await update.message.reply_text(
                f"Привет, {update.effective_user.first_name}! Выбери критерий поиска:",
                reply_markup=reply_markup
            )
        elif update.callback_query:
            query = update.callback_query
            await query.answer()
            await query.edit_message_text(
                f"Привет, {update.effective_user.first_name}! Выбери критерий поиска:",
                reply_markup=reply_markup
            )

    async def about(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        text = (
            f"🎬 <b>Movie Recommendation Bot</b>\n\n"
            f"В базе {len(self.movies_df)} фильмов.\n"
            "Критерии поиска:\n- Случайный выбор\n- По рейтингу (IMDb)\n- По году\n- По популярности\n\n"
            "Используйте /start для начала."
        )
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    @require_data
    async def random_movie(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        movie = self.movies_df.sample(1).iloc[0]
        await self._send_movie_info(query, movie, "🎲 Случайная рекомендация:")

    async def choose_rating(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        min_rating = round(self.movies_df['vote_average'].min(), 1)
        max_rating = round(self.movies_df['vote_average'].max(), 1)
        await query.edit_message_text(
            f"Введите минимальный рейтинг ({min_rating} - {max_rating}):\nПример: <code>7.5</code>",
            parse_mode='HTML'
        )
        return RATING

    async def process_rating(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            rating = float(update.message.text)
        except ValueError:
            await update.message.reply_text("Введите корректное число, например 7.5")
            return RATING

        min_rating = self.movies_df['vote_average'].min()
        max_rating = self.movies_df['vote_average'].max()
        if not (min_rating <= rating <= max_rating):
            await update.message.reply_text(f"Введите число от {min_rating:.1f} до {max_rating:.1f}")
            return RATING

        filtered = self.movies_df[self.movies_df['vote_average'] >= rating]
        if filtered.empty:
            await update.message.reply_text("Фильмов с таким рейтингом не найдено.")
            return RATING

        movie = filtered.sample(1).iloc[0]
        await self._send_movie_info(update, movie, f"🎬 Фильмы с рейтингом {rating:.1f}+")
        return ConversationHandler.END

    async def choose_year(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("Введите год выпуска (например, 2020):")
        return YEAR

    async def process_year(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            year = int(update.message.text)
        except ValueError:
            await update.message.reply_text("Введите корректный год (например, 2020)")
            return YEAR

        filtered = self.movies_df[self.movies_df['year'] == year]
        if filtered.empty:
            await update.message.reply_text(f"Фильмов за {year} год не найдено.")
            return YEAR

        movie = filtered.sample(1).iloc[0]
        await self._send_movie_info(update, movie, f"🎬 Фильмы за {year} год:")
        return ConversationHandler.END

    async def choose_popularity(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("Введите минимальный уровень популярности:")
        return POPULARITY

    async def process_popularity(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            popularity = float(update.message.text)
        except ValueError:
            await update.message.reply_text("Введите корректное число для популярности.")
            return POPULARITY

        filtered = self.movies_df[self.movies_df['popularity'] >= popularity]
        if filtered.empty:
            await update.message.reply_text(f"Фильмов с популярностью от {popularity} не найдено.")
            return POPULARITY

        movie = filtered.sample(1).iloc[0]
        await self._send_movie_info(update, movie, f"🎬 Фильмы с популярностью от {popularity}:")
        return ConversationHandler.END

    async def _send_movie_info(self, update_obj, movie, prefix=""):
        text = (
            f"{prefix}\n\n"
            f"<b>{movie['title']}</b> ({int(movie['year'])})\n"
            f"⭐ Рейтинг: <b>{movie['vote_average']}</b>/10 (голосов: {movie['vote_count']})\n"
            f"🔥 Популярность: <b>{movie['popularity']:.1f}</b>\n\n"
        )
        overview = movie['overview']
        if len(overview) > MAX_OVERVIEW_LENGTH:
            overview = overview[:MAX_OVERVIEW_LENGTH] + "..."
        text += f"📝 <i>{overview}</i>"

        poster = movie.get('poster_path')
        if pd.notna(poster) and isinstance(poster, str):
            text += f"\n\n<a href='https://image.tmdb.org/t/p/w500{poster}'>🎞 Постер</a>"

        keyboard = [
            [InlineKeyboardButton("🔁 Еще один", callback_data='random')],
            [InlineKeyboardButton("🔙 Назад", callback_data='back')]
        ]
        markup = InlineKeyboardMarkup(keyboard)

        if hasattr(update_obj, 'message') and update_obj.message:
            await update_obj.message.reply_text(text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=False)
        else:
            await update_obj.edit_message_text(text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=False)

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        await update.message.reply_text("Действие отменено. Используйте /start для нового поиска.")
        return ConversationHandler.END

    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.error(f"Ошибка: {context.error}", exc_info=context.error)
        if update and hasattr(update, 'effective_message') and update.effective_message:
            await update.effective_message.reply_text("⚠️ Произошла ошибка. Пожалуйста, попробуйте позже.")


def main():
    if not BOT_TOKEN:
        logger.critical("Токен бота не найден! Проверьте .env файл.")
        return

    bot = MovieRecommendationBot()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler('start', bot.start))
    application.add_handler(CallbackQueryHandler(bot.random_movie, pattern='^random$'))
    application.add_handler(CallbackQueryHandler(bot.about, pattern='^about$'))
    application.add_handler(CallbackQueryHandler(bot.start, pattern='^back$'))

    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(bot.choose_rating, pattern='^rating$'),
            CallbackQueryHandler(bot.choose_year, pattern='^year$'),
            CallbackQueryHandler(bot.choose_popularity, pattern='^popularity$')
        ],
        states={
            RATING: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.process_rating)],
            YEAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.process_year)],
            POPULARITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, bot.process_popularity)],
        },
        fallbacks=[CommandHandler('cancel', bot.cancel)],
        allow_reentry=True
    )
    application.add_handler(conv_handler)

    application.add_error_handler(bot.error_handler)

    logger.info("Запуск бота...")
    application.run_polling()


if __name__ == '__main__':
    main()
