import os
import datetime
import json
import logging
from typing import List, Optional, Dict, Any, Tuple

# ▼▼▼ 変更点1: Flask をインポート ▼▼▼
from flask import Flask, request

import requests
import google.auth
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ▼▼▼ 変更点2: Flaskアプリケーションを初期化 (変数名は必ず 'app') ▼▼▼
app = Flask(__name__)

# ---------------------------------------------------------------------------
# 設定・定数定義
# ---------------------------------------------------------------------------

# ログ設定: Cloud Runのログシステムに対応
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# タイムゾーン (JST)
JST = datetime.timezone(datetime.timedelta(hours=9), 'JST')

# EDINET API V2 エンドポイント
EDINET_API_BASE_URL = "https://disclosure.edinet-fsa.go.jp/api/v2"
EDINET_DOC_LIST_URL = f"{EDINET_API_BASE_URL}/documents.json"

# リクエストのタイムアウト設定 (秒)
REQUEST_TIMEOUT = 10.0

# ---------------------------------------------------------------------------
# ヘルパー関数
# ---------------------------------------------------------------------------

def get_session_with_retries() -> requests.Session:
    """
    リトライロジックを含むHTTPセッションを作成する。
    一時的なネットワークエラー（5xx系）に対する耐性を高める。
    """
    session = requests.Session()
    # backoff_factor=1 により、1秒, 2秒, 4秒と待機時間が増加
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def get_target_codes_from_sheet(sheet_id: str) -> List[str]:
    """
    スプレッドシートから監視対象のEDINETコードリストを取得する。
    Args:
        sheet_id (str): Google Spreadsheet ID
    Returns:
        List[str]: クリーニング済みのEDINETコードリスト
    """
    if not sheet_id:
        logger.error("Configuration Error: SPREADSHEET_ID is not set.")
        return []

    try:
        # Google Cloudの認証情報を自動取得 (Cloud RunのService Accountを使用)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        # default() は環境に応じて適切な認証情報を探索します
        creds, _ = google.auth.default(scopes=scopes)
        gc = gspread.authorize(creds)

        # シートを開く
        sh = gc.open_by_key(sheet_id)
        # ワークシート名の変更に強いように、存在チェックまたはインデックス参照も検討可能だが、
        # ここでは指定された運用通り名称指定とする
        worksheet = sh.worksheet("対象リスト")

        # A列(1列目)の値を全て取得
        codes = worksheet.col_values(1)

        # フィルタリング処理: 空白除去し、'E'から始まる正規のEDINETコードのみ抽出
        clean_codes = [
            str(c).strip() for c in codes 
            if c and str(c).strip().startswith('E')
        ]
        
        logger.info(f"Successfully loaded {len(clean_codes)} codes from sheet.")
        return clean_codes

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Spreadsheet not found. ID: {sheet_id}")
        return []
    except gspread.exceptions.WorksheetNotFound:
        logger.error("Worksheet '対象リスト' not found.")
        return []
    except Exception as e:
        logger.exception(f"Unexpected error loading sheet: {e}")
        return []

def fetch_edinet_documents(date_str: str, api_key: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    EDINET APIから指定日の書類一覧を取得する。
    Args:
        date_str (str): YYYY-MM-DD形式の日付
        api_key (str, optional): EDINET API Subscription-Key
    Returns:
        Optional[List[Dict]]: 書類情報のリスト。APIエラー時はNone。
    """
    params = {
        "date": date_str,
        "type": 2  # type=2: 既出の書類一覧を取得 (メタデータ)
    }
    if api_key:
        params["Subscription-Key"] = api_key

    try:
        session = get_session_with_retries()
        res = session.get(EDINET_DOC_LIST_URL, params=params, timeout=REQUEST_TIMEOUT)
        
        if res.status_code != 200:
            logger.error(f"EDINET API Error: {res.status_code} - {res.text}")
            return None

        data = res.json()
        results = data.get("results")
        
        return results if results is not None else []

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error connecting to EDINET API: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse EDINET API response: {e}")
        return None

def notify_slack(webhook_url: str, message: Dict[str, str]) -> bool:
    """
    Slackに通知を送信する。
    """
    if not webhook_url:
        logger.warning("Slack Webhook URL is missing.")
        return False

    try:
        session = get_session_with_retries()
        res = session.post(
            webhook_url, 
            json=message, 
            headers={'Content-Type': 'application/json'},
            timeout=REQUEST_TIMEOUT
        )
        if res.status_code != 200:
            logger.error(f"Slack Notification Failed: {res.status_code} - {res.text}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error sending Slack notification: {e}")
        return False

# ---------------------------------------------------------------------------
# メインロジック
# ---------------------------------------------------------------------------

# ▼▼▼ 変更点3: @app.route でWebアクセスを受け付けるように修正 ▼▼▼
# Cloud Scheduler (POST) やブラウザ確認 (GET) に対応
@app.route("/", methods=["POST", "GET"])
def check_edinet_and_notify() -> Tuple[str, int]:
    """
    Cloud Run Serviceのエントリーポイント。
    監視対象企業の新規開示情報をチェックし、Slackに通知する。
    """
    # 引数 `request` は削除しました（内部で使用されていないため）
    try:
        # 1. 環境変数の取得と検証
        sheet_id = os.environ.get('SPREADSHEET_ID')
        webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
        edinet_api_key = os.environ.get('EDINET_API_KEY')

        if not sheet_id or not webhook_url:
            msg = "Critical config missing: SPREADSHEET_ID or SLACK_WEBHOOK_URL."
            logger.critical(msg)
            return msg, 500

        # 2. 監視対象リストの取得
        target_edinet_codes = get_target_codes_from_sheet(sheet_id)
        if not target_edinet_codes:
            msg = "No target codes found in Spreadsheet. Aborting."
            logger.warning(msg)
            # 正常にシートは読めたが中身がない場合は200で終了する運用もアリだが、ここでは異常として警告
            return msg, 500
        
        target_codes_set = set(target_edinet_codes)

        # 3. 現在時刻と判定ロジックの設定
        now = datetime.datetime.now(JST)
        today_str = now.strftime('%Y-%m-%d')
        
        # 閾値設定: 15:45 (東証の大引け後、主要な開示が出揃うタイミング)
        threshold_time = now.replace(hour=15, minute=45, second=0, microsecond=0)
        is_night_run = now.hour >= 16

        logger.info(f"Start Check - Date: {today_str}, NightRun: {is_night_run}, Targets: {len(target_codes_set)}")

        # 4. EDINET APIからデータ取得
        results = fetch_edinet_documents(today_str, api_key=edinet_api_key)
        
        if results is None:
            return "Failed to fetch documents from EDINET API.", 500
        
        notification_count = 0

        # 5. フィルタリングと通知
        for doc in results:
            edinet_code = doc.get("edinetCode")
            
            # 対象リストに含まれるかチェック
            if edinet_code in target_codes_set:
                
                submit_str = doc.get("submitDateTime")
                if not submit_str: 
                    continue
                
                # 文字列をJSTの日時オブジェクトに変換
                try:
                    submit_dt = datetime.datetime.strptime(submit_str, '%Y-%m-%d %H:%M')
                    submit_dt = submit_dt.replace(tzinfo=JST)
                except ValueError:
                    logger.warning(f"Invalid date format from API: {submit_str}")
                    continue

                # 通知判定ロジック (重複防止用)
                should_notify = True
                if is_night_run and submit_dt <= threshold_time:
                    should_notify = False
                
                if should_notify:
                    doc_title = doc.get("docDescription", "不明な書類")
                    filer_name = doc.get("filerName", "不明な企業")
                    doc_id = doc.get("docID", "")
                    
                    # リンク生成
                    download_link = f"{EDINET_API_BASE_URL}/documents/{doc_id}?type=2"
                    
                    # Slackメッセージの構築
                    message = {
                        "text": (
                            f"📢 *開示情報 ({submit_str})*\n"
                            f"*企業名*: {filer_name}\n"
                            f"*書類*: {doc_title}\n"
                            f"*PDF*: {download_link}"
                        )
                    }
                    if notify_slack(webhook_url, message):
                        notification_count += 1
                        logger.info(f"Notified: {filer_name} - {doc_title}")
        
        # 6. 通知なしのハンドリング
        if notification_count == 0:
            time_label = "夜間チェック" if is_night_run else "日中チェック"
            logger.info(f"No new disclosures found for target companies ({time_label}).")
            
            # 通知ゼロのメッセージ
            no_data_message = {
                "text": (
                    f"✅ *開示なし ({today_str} {time_label})*\n"
                    f"監視対象({len(target_codes_set)}社)について、新規の開示はありませんでした。"
                )
            }
            notify_slack(webhook_url, no_data_message)

        result_msg = f"Success. Checked {len(results)} docs. Sent {notification_count} notifications."
        logger.info(result_msg)
        return result_msg, 200

    except Exception as e:
        logger.exception(f"Critical Internal Error: {e}")
        return f"Internal Error: {str(e)}", 500

# ▼▼▼ 変更点4: 起動スクリプトを追加 ▼▼▼
if __name__ == "__main__":
    # Cloud Run は環境変数 PORT (デフォルト8080) を指定してくるため、それに従う
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, host="0.0.0.0", port=port)
