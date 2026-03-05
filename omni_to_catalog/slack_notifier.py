#!/usr/bin/env python3
"""
Sends Slack notification for Omni-Coalesce sync workflow status.
"""
import json
import logging
import urllib.request
import urllib.error
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


def build_slack_payload(status: str, stats: Dict[str, Any],
                        duration: str = "", error: Optional[str] = None,
                        upload_details: Optional[Dict[str, Any]] = None,
                        tag_details: Optional[Dict[str, Any]] = None) -> Dict:
    """Construct the Slack message payload."""
    if status == "success":
        header_text = "Omni-Coalesce Sync Successful"
        color = "#36a64f"
    else:
        header_text = "Omni-Coalesce Sync Failed"
        color = "#e01e5a"

    # Build upload section
    upload_lines = []
    if upload_details:
        for f in upload_details.get('files_uploaded', []):
            upload_lines.append(f"  *{f}*")
        for f in upload_details.get('files_failed', []):
            fname = f.get('file', 'unknown')
            err = f.get('error', '')
            # Extract just the HTTP error line from tracebacks
            if 'HTTPError' in err:
                for line in err.strip().split('\n'):
                    if 'HTTPError' in line or 'Error' in line.split(':')[0]:
                        err = line.strip()
                        break
            upload_lines.append(f"  *{fname}*\n     `{err}`")

    # Build tag section
    tag_lines = []
    if tag_details:
        tag_lines.append(f"*{tag_details.get('tags_synced', 0)}* tags synced")
        if tag_details.get('catalog_dashboards'):
            tag_lines.append(f"*{tag_details['catalog_dashboards']}* dashboards found in Catalog")
        if tag_details.get('skipped'):
            tag_lines.append(f"*{tag_details['skipped']}* dashboards skipped (not in Catalog)")
        if tag_details.get('error'):
            tag_lines.append(f"Error: {tag_details['error']}")

    # Build summary
    summary_parts = []
    if stats.get('dashboards'):
        summary_parts.append(f"*{stats['dashboards']}* Dashboards")
    if stats.get('models'):
        summary_parts.append(f"*{stats['models']}* Models")
    if stats.get('queries'):
        summary_parts.append(f"*{stats['queries']}* Queries")
    if stats.get('fields'):
        summary_parts.append(f"*{stats['fields']}* Fields")

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text, "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Status:*\n{'Success' if status == 'success' else 'Failed'}"},
                {"type": "mrkdwn", "text": f"*Duration:*\n{duration or 'N/A'}"}
            ]
        }
    ]

    if summary_parts:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Extracted:* {' | '.join(summary_parts)}"}
        })

    if upload_lines:
        uploaded_count = len(upload_details.get('files_uploaded', []))
        failed_count = len(upload_details.get('files_failed', []))
        upload_header = f"*Upload:* {uploaded_count} succeeded, {failed_count} failed" if failed_count else f"*Upload:* {uploaded_count} files uploaded"
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"{upload_header}\n" + "\n".join(upload_lines)}
        })

    if tag_lines:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Tags:*\n" + "\n".join(f"- {l}" for l in tag_lines)}
        })

    if status != "success" and error:
        # Truncate long errors for Slack
        err_display = error[:300] + "..." if len(error) > 300 else error
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Error:*\n```{err_display}```"}
        })

    return {
        "text": f"Omni-Coalesce Sync: {status}",
        "attachments": [{"color": color, "blocks": blocks}]
    }


def send_slack_notification(webhook_url: str, status: str,
                            stats: Dict[str, Any] = None,
                            duration: str = "",
                            error: Optional[str] = None,
                            upload_details: Optional[Dict[str, Any]] = None,
                            tag_details: Optional[Dict[str, Any]] = None) -> bool:
    """
    Send a Slack notification via webhook.

    Args:
        webhook_url: Slack incoming webhook URL
        status: 'success' or 'failure'
        stats: Dictionary with sync statistics
        duration: Duration string (e.g., '02:35')
        error: Error message if status is failure
        upload_details: Dict with 'files_uploaded' and 'files_failed' lists
        tag_details: Dict with tag sync details

    Returns:
        True if notification was sent successfully
    """
    if not webhook_url:
        logger.debug("No SLACK_WEBHOOK_URL configured, skipping notification")
        return False

    payload = build_slack_payload(
        status, stats or {}, duration, error,
        upload_details=upload_details,
        tag_details=tag_details
    )

    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )

    try:
        logger.info("Sending Slack notification...")
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            logger.info("Slack notification sent")
            return True
    except urllib.error.HTTPError as e:
        error_msg = e.read().decode('utf-8')
        logger.warning(f"Slack API error {e.code}: {error_msg}")
        return False
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")
        return False
