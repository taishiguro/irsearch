import os
import datetime
import json
import logging
from typing import List, Optional, Dict, Any, Tuple

import requests
import google.auth
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# 設定・定数定義
# ---------------------------------------------------------------------------

# ログ設定: Cloud Runのログシステムに対応するため標準ロガーを使用
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# タイムゾーン (JST)
JST = datetime.timezone(datetime.timedelta(hours=9), 'JST')

# EDINET API V2 エンドポイント
EDINET_API_URL = "https://disclosure.edinet-fsa.go.jp/api/v2/documents.json"

# リクエストのタイムアウト設定 (秒)
REQUEST_TIMEOUT = 10

# ---------------------------------------------------------------------------
# ヘルパー関数
# ---------------------------------------------------------------------------

def get_session_with_retries() -> requests.Session:
    """
    リトライロジックを含むHTTPセッションを作成する。
    一時的なネットワークエラーに対する耐性を高める。
    """
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def get_target_codes_from_sheet(sheet_id: str) -> List[str]:
    """
    スプレッドシートからEDINETコードのリストを取得する。
    
    Args:
        sheet_id (str): Google Spreadsheet ID
    
    Returns:
        List[str]: クリーニング済みのEDINETコードリスト
    """
    if not sheet_id:
        logger.error("SPREADSHEET_ID is not set.")
        return []

    try:
        # Google Cloudの認証情報を自動取得 (Cloud RunのService Accountを使用)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds, _ = google.auth.default(scopes=scopes)
        gc = gspread.authorize(creds)

        # シートを開く
        sh = gc.open_by_key(sheet_id)
        worksheet = sh.worksheet("対象リスト")

        # A列(1列目)の値を全て取得
        codes = worksheet.col_values(1)

        # フィルタリング処理: 空白除去し、'E'から始まる正規のEDINETコードのみ抽出
        clean_codes = [
            c.strip() for c in codes 
            if c and isinstance(c, str) and c.strip().startswith('E')
        ]
        
        logger.info(f"Loaded {len(clean_codes)} codes from sheet.")
        return clean_codes

    except gspread.exceptions.SpreadsheetNotFound:
        logger.error("Spreadsheet not found. Check the ID and permissions.")
        return []
    except gspread.exceptions.WorksheetNotFound:
        logger.error("Worksheet '対象リスト' not found.")
        return []
    except Exception as e:
        logger.exception(f"Unexpected error loading sheet: {e}")
        return []

def fetch_edinet_documents(date_str: str, api_key: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    EDINET APIから書類一覧を取得する。
    
    Args:
        date_str (str): YYYY-MM-DD形式の日付
        api_key (str, optional): EDINET API Subscription-Key (V2利用時推奨)
    
    Returns:
        Optional[List[Dict]]: 
            - 成功時: 書類情報のリスト (0件の場合は空リスト [])
            - 失敗時: None
    """
    params = {
        "date": date_str,
        "type": 2  # 既出の書類一覧を取得
    }
    if api_key:
        params["Subscription-Key"] = api_key

    try:
        session = get_session_with_retries()
        # verify=Trueはデフォルトだが明示的に記載 (SSL検証)
        res = session.get(EDINET_API_URL, params=params, timeout=REQUEST_TIMEOUT)
        
        if res.status_code != 200:
            logger.error(f"EDINET API Error: {res.status_code} - {res.text}")
            return None # 明示的に失敗を示す

        data = res.json()
        results = data.get("results")
        
        # API仕様によりresultsがNoneの場合もあるため、空リストを保証する
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
    
    Returns:
        bool: 送信成功ならTrue
    """
    if not webhook_url:
        logger.warning("Slack Webhook URL is missing. Skipping notification.")
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

def check_edinet_and_notify(request) -> Tuple[str, int]:
    """
    Cloud Run Functionのエントリーポイント。
    
    Args:
        request: Cloud Functions / Cloud Run framework request object
    """
    try:
        # 1. 環境変数の取得と検証
        sheet_id = os.environ.get('SPREADSHEET_ID')
        webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
        edinet_api_key = os.environ.get('EDINET_API_KEY') # オプション

        if not sheet_id or not webhook_url:
            msg = "Critical config missing: SPREADSHEET_ID or SLACK_WEBHOOK_URL."
            logger.critical(msg)
            return msg, 500

        # 2. 監視対象リストの取得
        target_edinet_codes = get_target_codes_from_sheet(sheet_id)
        if not target_edinet_codes:
            msg = "No target codes found. Aborting."
            logger.warning(msg)
            return msg, 500
        
        # 高速化のためSetに変換
        target_codes_set = set(target_edinet_codes)

        # 3. 現在時刻と判定ロジックの設定
        now = datetime.datetime.now(JST)
        today_str = now.strftime('%Y-%m-%d')
        
        # 閾値設定: 15:45 (通常、日中の開示の区切り目安)
        threshold_time = now.replace(hour=15, minute=45, second=0, microsecond=0)
        is_night_run = now.hour >= 16

        logger.info(f"Start Check - Date: {today_str}, NightRun: {is_night_run}, Targets: {len(target_codes_set)}")

        # 4. EDINET APIからデータ取得
        results = fetch_edinet_documents(today_str, edinet_api_key)
        
        # API通信自体が失敗した場合のみ 500 エラーとする
        if results is None:
            return "Failed to fetch documents from EDINET API (Network or API Error).", 500
        
        # results が空リスト [] の場合は、正常系として処理を続行する

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
                    logger.warning(f"Invalid date format: {submit_str}")
                    continue

                # 通知判定ロジック
                # 夜間実行時のみ、15:45以降の開示に絞る (日中実行分との重複防止)
                should_notify = True
                if is_night_run and submit_dt <= threshold_time:
                    should_notify = False
                
                if should_notify:
                    doc_title = doc.get("docDescription", "不明な書類")
                    filer_name = doc.get("filerName", "不明な企業")
                    doc_id = doc.get("docID", "")
                    
                    # Slackメッセージの構築
                    message = {
                        "text": (
                            f"📢 *開示情報 ({submit_str})*\n"
                            f"*企業名*: {filer_name}\n"
                            f"*書類*: {doc_title}\n"
                            f"*リンク*: https://disclosure.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
                        )
                    }
                    if notify_slack(webhook_url, message):
                        notification_count += 1
        
        # 6. 通知が0件だった場合のサマリ通知
        # 監視対象企業に開示がない場合、またはEDINET全体の開示が0件の場合もここに来る
        if notification_count == 0:
            time_label = "夜間チェック" if is_night_run else "日中チェック"
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
        # キャッチされなかった予期せぬエラーの記録
        logger.exception(f"Critical Internal Error: {e}")
        return f"Internal Error: {str(e)}", 500
