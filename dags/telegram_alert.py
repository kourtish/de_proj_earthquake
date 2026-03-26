import html
import logging
import os
import urllib.parse
import urllib.request
from typing import Any, Optional, Tuple

from airflow.models import Variable


def _get_telegram_config() -> Tuple[Optional[str], Optional[str]]:
    """
    Telegram config is read lazily (inside callback) to avoid
    unnecessary failures during DAG parsing.
    """

    token = Variable.get("telegram_bot_token") # setup in airflow variables
    chat_id = Variable.get("telegram_chat_id") # setup in airflow variables

    # Fallback to Airflow Variables (Admin -> Variables)
    if not token:
        try:
            token = Variable.get("telegram_bot_token")
        except Exception:
            token = None

    if not chat_id:
        try:
            chat_id = Variable.get("telegram_chat_id")
        except Exception:
            chat_id = None

    return token, chat_id


def _safe_truncate(text: str, limit: int = 250) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[:limit].rsplit(" ", 1)[0].rstrip() + "..."


def send_telegram_alert(context: dict[str, Any]) -> None:
    """
    Airflow callback for task failures.

    Context keys (depending on Airflow version/operator):
    - task_instance
    - dag
    - dag_run
    - exception
    - logical_date / execution_date
    """

    token, chat_id = _get_telegram_config()
    if not token or not chat_id:
        logging.warning(
            "Telegram alert skipped: missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID "
            "or Airflow Variables telegram_bot_token/telegram_chat_id."
        )
        return

    task_instance = context.get("task_instance")
    dag = context.get("dag")
    dag_run = context.get("dag_run")

    dag_id = getattr(task_instance, "dag_id", None) or getattr(dag, "dag_id", None) or context.get("dag_id")
    task_id = getattr(task_instance, "task_id", None) or context.get("task_id")
    exception = context.get("exception") or getattr(task_instance, "exception", None) or ""
    exception_text = _safe_truncate(str(exception)) if exception else ""

    logical_date = (
        context.get("logical_date")
        or context.get("execution_date")
        or getattr(dag_run, "logical_date", None)
        or getattr(dag_run, "execution_date", None)
    )

    log_url = getattr(task_instance, "log_url", None)

    # Telegram: use HTML and escape user content to avoid formatting issues.
    msg = [
        "🔴 <b>Task Failed</b>",
        f"DAG: <code>{html.escape(str(dag_id))}</code>",
        f"Task: <code>{html.escape(str(task_id))}</code>",
    ]
    if logical_date is not None:
        msg.append(f"Execution: <code>{html.escape(str(logical_date))}</code>")
    if exception_text:
        msg.append(f"Error: {html.escape(exception_text)}")
    if log_url:
        msg.append(f'<a href="{html.escape(str(log_url))}">See logs</a>')

    text = "\n".join(msg)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }

    try:
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            _ = resp.read().decode("utf-8", errors="replace")
    except Exception:
        # Never raise from alert callback: we don't want to mask the original task failure.
        logging.exception("Telegram alert failed")

