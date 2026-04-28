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

    rows = []

    for act_id in resolved["meta"]["account_ids"]:
        if not act_id:
            continue

        try:
            account_rows = fetch_meta_cp_pl_rows(
                act_id=act_id,
                token=resolved["meta"]["token"],
                since=since,
                until=until,
            )
            print(f"Meta account {act_id} rows built: {len(account_rows)}")
            rows.extend(account_rows)

        except Exception as e:
            print(f"Warning: Meta account {act_id} skipped: {repr(e)}")
            continue

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

    account_ids = meta.get("account_ids", [])
    if isinstance(account_ids, list):
        for account_id in account_ids:
            push(account_id)

    for value in sorted(set(candidates)):
        print(f"::add-mask::{value}")


def resolve_config(config):
    meta_conf = config.get("meta", {})
    sheets_conf = config.get("sheets", {})

    account_ids = meta_conf.get("account_ids")

    if isinstance(account_ids, str):
        account_ids = [account_ids]

    return {
        "meta": {
            "token": meta_conf.get("token"),
            "account_ids": account_ids or [],
        },
        "sheet": {
            "spreadsheet_id": sheets_conf.get("spreadsheet_id"),
            "worksheet_name": sheets_conf.get("worksheet_name") or DEFAULT_WORKSHEET_NAME,
            "google_service_account": normalize_google_service_account(
                config.get("gcp_service_account")
            ),
        },
    }


def validate_config(resolved):
    required = {
        "meta.token": resolved["meta"]["token"],
        "meta.account_ids": resolved["meta"]["account_ids"],
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
            "meta",
            "cp_pl",
            month,
            day,
            item.get("campaign_name", ""),
            item.get("publisher_platform", ""),
            item.get("platform_position", ""),
            item.get("impression_device", ""),
            to_int(item.get("impressions")),
            to_int(item.get("inline_link_clicks")),
            round(adjusted_spend, 2),
            website_purchases,
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

        if not response.ok:
            raise RuntimeError(
                f"Meta API request failed. "
                f"account={act_id}, "
                f"status={response.status_code}, "
                f"body={truncate_text(response.text)}"
            )

        data = response.json()

        if "error" in data:
            raise RuntimeError(
                f"Meta API error: {json.dumps(data['error'], ensure_ascii=False)}"
            )

        all_rows.extend(data.get("data", []))

        next_url = data.get("paging", {}).get("next")
        if not next_url:
            break

        url = next_url
        params = None

    return all_rows


def extract_website_purchases(actions):
    if not isinstance(actions, list):
        return 0

    target_action_types = {
        "offsite_conversion.fb_pixel_purchase",
        "onsite_conversion.purchase",
        "purchase",
    }

    total = 0.0

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

    output = header + rows
    required_rows = max(len(output) + 10, 1000)
    required_cols = 12

    try:
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=required_rows,
                cols=required_cols,
            )

        if worksheet.row_count < required_rows:
            worksheet.resize(rows=required_rows)

        if worksheet.col_count < required_cols:
            worksheet.resize(cols=required_cols)

        worksheet.clear()
        worksheet.update("A1", output, value_input_option="USER_ENTERED")

        print(f"Write success: {sheet_name} ({len(rows)} rows)")

    except Exception as e:
        raise RuntimeError(f"Write error ({sheet_name}): {repr(e)}") from e


def sort_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            row[0],
            row[1],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
        ),
    )


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
