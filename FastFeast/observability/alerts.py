from __future__ import annotations

import atexit
import logging
import queue
import smtplib
import threading
import urllib.request
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utilities.helper_utils import now_iso

logger = logging.getLogger(__name__)

alert_queue = queue.Queue()


def worker():
    while True:
        task = alert_queue.get()
        if task is None:
            alert_queue.task_done()
            break
        alerts_cfg, smtp_password, subject, body = task
        try:
            send_all(alerts_cfg, smtp_password, subject, body)
        except Exception as exc:
            logger.error("[alerts] unexpected error in worker: %s", exc)
        finally:
            alert_queue.task_done()


worker_thread = threading.Thread(
    target=worker, name="fastfeast-alert-worker", daemon=True
)
worker_thread.start()


def shutdown(alerts_cfg):
    alert_queue.put(None)
    worker_thread.join(timeout=alerts_cfg.shutdown_timeout_sec)


atexit.register(shutdown)


def send_failure_alert(alerts_cfg, smtp_password, run_id, snapshot):
    if not alerts_cfg.on_fail:
        return
    subject = (
        f"[FastFeast]  Validation FAILED — run_id={run_id} | "
        f"{snapshot.get('files_failed', '?')} file(s) with issues"
    )
    enqueue(alerts_cfg, smtp_password, subject, snapshot)


def send_success_alert(alerts_cfg, smtp_password, run_id, snapshot):
    if alerts_cfg.on_fail:
        return
    subject = f"[FastFeast]  Validation CLEAN — run_id={run_id}"
    enqueue(alerts_cfg, smtp_password, subject, snapshot)


def enqueue(alerts_cfg, smtp_password, subject, snapshot):
    body = format_alert_body(snapshot)
    task = (alerts_cfg, smtp_password, subject, body)

    if alert_queue.qsize() >= alerts_cfg.max_queue_size:
        try:
            alert_queue.get_nowait()
            logger.warning("[alerts] queue full — oldest alert dropped")
        except queue.Empty:
            pass

    try:
        alert_queue.put_nowait(task)
    except queue.Full:
        logger.warning("[alerts] queue still full after drop — alert skipped")


def send_all(alerts_cfg, smtp_password, subject, body):
    sent = False

    if alerts_cfg.email:
        send_email(alerts_cfg, smtp_password, subject, body)
        sent = True

    if not sent:
        logger.error(
            "[alerts] An email is unconfigured — "
            "alert was lost entirely. Set alerts.email "
            "in config.yaml."
        )


def send_email(alerts_cfg, smtp_password, subject, body_html):
    try:
        msg = MIMEMultipart("alternative")
        msg["From"]    = alerts_cfg.smtp_user or "fastfeast@pipeline"
        msg["To"]      = alerts_cfg.email
        msg["Subject"] = subject

        msg.attach(MIMEText(body_html, "html", "utf-8"))

        with smtplib.SMTP(
            alerts_cfg.smtp_host,
            alerts_cfg.smtp_port,
            timeout=alerts_cfg.send_timeout_sec,
        ) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            if alerts_cfg.smtp_user:
                server.login(alerts_cfg.smtp_user, smtp_password)
            server.send_message(msg)

        logger.info("[alerts] email sent → %s", alerts_cfg.email)

    except smtplib.SMTPAuthenticationError:
        logger.error("[alerts] email auth failed — check FASTFEAST_SMTP_PASSWORD")
    except smtplib.SMTPException as exc:
        logger.error("[alerts] SMTP error (pipeline continues): %s", exc)
    except OSError as exc:
        logger.error("[alerts] network error reaching %s:%s — %s",
                     alerts_cfg.smtp_host, alerts_cfg.smtp_port, exc)
    except Exception as exc:
        logger.error("[alerts] email failed (pipeline continues): %s", exc)


def format_alert_body(snapshot):
    is_clean = snapshot.get("files_failed", 1) == 0
    color    = "#2e7d32" if is_clean else "#c62828"
    status   = "✓ CLEAN" if is_clean else "✗ FAILED"

    stats = [
        ("Total Files", snapshot.get('files_total',  0), "#222"),
        ("Clean",       snapshot.get('files_clean',  0), "#2e7d32"),
        ("Failed",      snapshot.get('files_failed', 0), "#c62828"),
        ("Total Rows",  f"{snapshot.get('total_rows', 0):,}", "#222"),
        ("Issues",      snapshot.get('total_issues', 0), "#222"),
    ]

    stat_cells = "".join(
        f"""<td style="width:20%;padding:16px 0;text-align:center;border-right:1px solid #eee">
            <div style="font-size:22px;font-weight:bold;color:{c}">{v}</div>
            <div style="font-size:12px;color:#888;margin-top:2px">{label}</div>
        </td>"""
        for label, v, c in stats
    )

    rows = "".join(
        f"""<tr style="background:{'#ffffff' if i % 2 == 0 else '#fafafa'}">
            <td style="width:40px;color:{'#2e7d32' if f.get('is_clean') else '#c62828'};font-weight:bold;padding:10px 10px 10px 20px">{'✓' if f.get('is_clean') else '✗'}</td>
            <td style="padding:10px;font-family:monospace">{f.get('file_name','?')}</td>
            <td style="padding:10px;text-align:right">{f.get('total_rows',0):,}</td>
            <td style="padding:10px;text-align:right;color:{'#c62828' if f.get('issue_count',0) else '#2e7d32'}">{f.get('issue_count',0)}</td>
        </tr>"""
        for i, f in enumerate(snapshot.get("per_file", []))
    )

    return f"""<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:auto;background:#f4f4f4;padding:20px">
    <div style="background:#ffffff;border-radius:8px;overflow:hidden;border:1px solid #ddd">

        <div style="background:{color};padding:20px 24px">
            <div style="font-size:22px;font-weight:bold;color:#fff">{status}</div>
            <div style="color:rgba(255,255,255,0.8);font-size:13px;margin-top:4px">Run ID: {snapshot.get('run_id','?')} &nbsp;·&nbsp; {snapshot.get('recorded_at', now_iso())}</div>
        </div>

        <table style="width:100%;border-collapse:collapse;border-bottom:1px solid #eee">
            <tr>{stat_cells}</tr>
        </table>

        <table style="width:100%;border-collapse:collapse;font-size:14px">
            <tr style="background:#f9f9f9;color:#888;font-size:12px;text-transform:uppercase">
                <th style="padding:10px 10px 10px 20px;text-align:left"></th>
                <th style="padding:10px;text-align:left">File</th>
                <th style="padding:10px;text-align:right">Rows</th>
                <th style="padding:10px;text-align:right">Issues</th>
            </tr>
            {rows}
        </table>

    </div>
    </body></html>"""