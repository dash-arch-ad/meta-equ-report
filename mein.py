import os
import json
import requests
import gspread
from zoneinfo import ZoneInfo
from datetime import datetime, date, timedelta
from oauth2client.service_account import ServiceAccountCredentials

META_API_VERSION = "v25.0"
JST = ZoneInfo("Asia/Tokyo")
DEFAULT_WORKSHEET_NAME = "gitreport"

AMOUNT_SPENT_MULTIPLIER = 1.25


def main():
    print("=== Start Meta cp_pl Export ===")

    config = load_secret()
    mask_sensitive_values(config)

    resolved = resolve_config(config)
    validate_config(resolved)

    since, until = get_target_date_range()
    print(f"Target range: {since} to {until}")

    rows = fetch_meta_cp_pl_rows(
        act_id=resolved["meta"]["account_id"],
        token=resolved["meta"]["token"],
        since=since,
        until=until,
    )

    print(f"Meta rows built: {len(rows)}")

    spreadsheet = connect_spreadsheet(
        sheet_id=resolved["sheet"]["spreadsheet_id"],
        google_creds_dict=resolved["sheet"]["google_service_account"],
    )

    write_to_sheet(
        spreadsheet=spreadsheet,
        sheet_name=resolved["sheet"]["worksheet_name"],
        rows=sort_rows(rows),
    )

    print(f"Total rows written: {len(rows)}")
    print("=== Completed ===")


def load_secret():
    secret_env = os.environ.get("APP_SECRET_JSON")
    if not secret_env:
        raise RuntimeError("APP_SECRET_JSON is not set")

    try:
        return json.loads(secret_env)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"APP_SECRET_JSON is invalid JSON: {e}") from e


def mask_sensitive_values(config):
    candidates = []

    def push(value):
        if value is None:
            return
        value = str(value).strip()
        if value and "\n" not in value:
            candidates.append(value)

    meta = config.get("meta", {})
    push(meta.get("token"))
    push(config.get("m_token"))
    push(meta.get("account_id"))
    push(config.get("m_act_id"))

    for value in sorted(set(candidates)):
        print(f"::add-mask::{value}")


def resolve_config(config):
    meta_conf = config.get("meta", {})
    sheets_conf = config.get("sheets", {})

    spreadsheet_id = sheets_conf.get("spreadsheet_id")
    if not spreadsheet_id:
        legacy_sheet_id = config.get("s_id")
        if isinstance(legacy_sheet_id, list):
            spreadsheet_id = legacy_sheet_id[0] if legacy_sheet_id else None
        else:
            spreadsheet_id = legacy_sheet_id

    worksheet_name = sheets_conf.get("worksheet_name") or DEFAULT_WORKSHEET_NAME

    google_service_account = config.get("gcp_service_account") or config.get("g_creds")
    google_service_account = normalize_google_service_account(google_service_account)

    return {
        "meta": {
            "token": meta_conf.get("token") or config.get("m_token"),
            "account_id": meta_conf.get("account_id") or config.get("m_act_id"),
        },
        "sheet": {
            "spreadsheet_id": spreadsheet_id,
            "worksheet_name": worksheet_name,
            "google_service_account": google_service_account,
        },
    }


def validate_config(resolved):
    required = {
        "meta.token": resolved["meta"]["token"],
        "meta.account_id": resolved["meta"]["account_id"],
        "sheet.spreadsheet_id": resolved["sheet"]["spreadsheet_id"],
        "sheet.google_service_account": resolved["sheet"]["google_service_account"],
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required config keys: {', '.join(missing)}")


def normalize_google_service_account(creds):
    if not creds:
        return None

    fixed = dict(creds)
    private_key = fixed.get("private_key", "")
    if private_key:
        fixed["private_key"] = private_key.replace("\\n", "\n")
    return fixed


def normalize_meta_act_id(raw_act_id):
    cleaned = (
        str(raw_act_id)
        .replace("act=", "")
        .replace("act_", "")
        .replace("act", "")
        .strip()
    )
    return f"act_{cleaned}"


def get_target_date_range():
    """
    GitHub Actionsの起動日時をJSTで見て、
    当月1日〜前日までを含めた過去6ヶ月分を取得。

    例：
    2026-04-27に実行
    → 2025-11-01〜2026-04-26
    """
    today_jst = datetime.now(JST).date()
    yesterday = today_jst - timedelta(days=1)

    this_month_start = date(today_jst.year, today_jst.month, 1)

    start_month = this_month_start
    for _ in range(5):
        previous_month_end = start_month - timedelta(days=1)
        start_month = date(previous_month_end.year, previous_month_end.month, 1)

    return start_month, yesterday


def fetch_meta_cp_pl_rows(act_id, token, since, until):
    normalized_act_id = normalize_meta_act_id(act_id)

    fields = [
        "campaign_name",
        "impressions",
        "inline_link_clicks",
        "spend",
        "actions",
    ]

    breakdowns = [
        "publisher_platform",
        "platform_position",
        "impression_device",
    ]

    insights = fetch_meta_insights(
        act_id=normalized_act_id,
        token=token,
        since=since,
        until=until,
        level="campaign",
        fields=fields,
        breakdowns=breakdowns,
        time_increment="1",
    )

    rows = []

    for item in insights:
        day = item.get("date_start", "")
        month = day[:7] if day else ""

        spend = to_float(item.get("spend"))
        adjusted_spend = spend * AMOUNT_SPENT_MULTIPLIER

        website_purchases = extract_website_purchases(item.get("actions", []))

        rows.append([
            "meta",                                      # A media
            "cp_pl",                                     # B scope
            month,                                       # C month
            day,                                         # D day
            item.get("campaign_name", ""),              # E campaign_name
            item.get("publisher_platform", ""),         # F platform
            item.get("platform_position", ""),          # G placement
            item.get("impression_device", ""),          # H device
            to_int(item.get("impressions")),             # I Impressions
            to_int(item.get("inline_link_clicks")),      # J Link clicks
            round(adjusted_spend, 2),                    # K Amount spent ×1.25
            website_purchases,                           # L Website purchases
        ])

    return rows


def fetch_meta_insights(
    act_id,
    token,
    since,
    until,
    level,
    fields,
    breakdowns,
    time_increment,
):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{act_id}/insights"

    params = {
        "access_token": token,
        "level": level,
        "time_range": json.dumps({
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }),
        "fields": ",".join(fields),
        "breakdowns": ",".join(breakdowns),
        "time_increment": time_increment,
        "action_report_time": "conversion",
        "limit": 5000,
    }

    all_rows = []

    while True:
        response = requests.get(url, params=params, timeout=120)

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(
                f"Meta API request failed. "
                f"status={response.status_code}, "
                f"body={truncate_text(response.text)}"
            ) from e

        data = response.json()

        if "error" in data:
            raise RuntimeError(
                f"Meta API error: {json.dumps(data['error'], ensure_ascii=False)}"
            )

        batch = data.get("data", [])
        all_rows.extend(batch)

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None

    return all_rows


def extract_website_purchases(actions):
    """
    Website purchases想定。
    主に offsite_conversion.fb_pixel_purchase を優先。
    念のため purchase も加算対象にしています。
    """
    if not isinstance(actions, list):
        return 0

    target_action_types = {
        "offsite_conversion.fb_pixel_purchase",
        "purchase",
    }

    total = 0

    for action in actions:
        action_type = action.get("action_type")
        if action_type in target_action_types:
            total += to_float(action.get("value"))

    return int(total) if total.is_integer() else total


def connect_spreadsheet(sheet_id, google_creds_dict):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            google_creds_dict,
            scope,
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        print("Google Sheets connected successfully")
        return spreadsheet
    except Exception as e:
        raise RuntimeError(f"Google Sheets connection error: {repr(e)}") from e


def write_to_sheet(spreadsheet, sheet_name, rows):
    header = [[
        "media",
        "scope",
        "month",
        "day",
        "campaign_name",
        "platform",
        "placement",
        "device",
        "Impressions",
        "Link clicks",
        "Amount spent",
        "Website purchases",
    ]]

    try:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=1000,
                cols=12,
            )

        worksheet.clear()
        output = header + rows
        worksheet.update("A1", output, value_input_option="USER_ENTERED")

        print(f"Write success: {sheet_name} ({len(rows)} rows)")
    except Exception as e:
        raise RuntimeError(f"Write error ({sheet_name}): {repr(e)}") from e


def sort_rows(rows):
    def sort_key(row):
        media = row[0]
        scope = row[1]
        month = row[2]
        day = row[3]
        campaign_name = row[4]
        platform = row[5]
        placement = row[6]
        device = row[7]

        return (
            media,
            scope,
            day,
            campaign_name,
            platform,
            placement,
            device,
        )

    return sorted(rows, key=sort_key)


def to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def truncate_text(value, limit=800):
    value = str(value)
    if len(value) <= limit:
        return value
    return value[:limit] + "...(truncated)"


if __name__ == "__main__":
    main()
