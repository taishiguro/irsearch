import os
import datetime
import requests
import json
import google.auth
import gspread

# 設定
# タイムゾーン設定 (JST)
JST = datetime.timezone(datetime.timedelta(hours=9), 'JST')

def get_target_codes_from_sheet():
    """スプレッドシートからEDINETコードのリストを取得する"""
    sheet_id = os.environ.get('SPREADSHEET_ID')
    if not sheet_id:
        print("Error: SPREADSHEET_ID is not set.")
        return []

    try:
        # Google Cloudの認証情報を自動取得
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds, _ = google.auth.default(scopes=scopes)
        gc = gspread.authorize(creds)

        # シートを開く
        sh = gc.open_by_key(sheet_id)
        worksheet = sh.sheet1  # 1枚目のシートを使用

        # A列(1列目)の値を全て取得
        codes = worksheet.col_values(1)

        # ヘッダー(1行目)がある場合を除去（もし"code"や"E"で始まらない文字ならスキップなどの処理）
        # シンプルに「E」から始まるものだけを有効なコードとしてフィルタリングします
        clean_codes = [c.strip() for c in codes if c.strip().startswith('E')]
        
        print(f"Loaded codes from sheet: {clean_codes}")
        return clean_codes

    except Exception as e:
        print(f"Error loading sheet: {e}")
        return []

def check_edinet_and_notify(request):
    try:
        # 1. 環境変数チェック
        webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
        if not webhook_url:
            return "Slack Webhook URL is not set.", 500

        # ★ リストをシートから取得 (ここを変更)
        target_edinet_codes = get_target_codes_from_sheet()
        
        if not target_edinet_codes:
            return "No target codes found (check Sheet ID or Sheet data).", 500

        # 2. 現在時刻と判定ロジック
        now = datetime.datetime.now(JST)
        today_str = now.strftime('%Y-%m-%d')
        threshold_time = now.replace(hour=15, minute=45, second=0, microsecond=0)
        is_night_run = now.hour >= 16

        print(f"Running at: {now}, Night run: {is_night_run}, Targets: {len(target_edinet_codes)}")

        # 3. EDINET APIから「今日」の書類を取得
        url = f"https://disclosure.edinet-fsa.go.jp/api/v2/documents.json?date={today_str}&type=2"
        res = requests.get(url) 
        if res.status_code != 200:
            return f"Error connecting to EDINET: {res.status_code}", 500
        
        data = res.json()
        results = data.get("results", [])
        
        notification_count = 0

        # 4. フィルタリングと通知
        for doc in results:
            edinet_code = doc.get("edinetCode")
            
            # ★ 変数名を変更したリストでチェック
            if edinet_code in target_edinet_codes:
                
                submit_str = doc.get("submitDateTime")
                if not submit_str: continue
                    
                submit_dt = datetime.datetime.strptime(submit_str, '%Y-%m-%d %H:%M')
                submit_dt = submit_dt.replace(tzinfo=JST)

                should_notify = False
                if is_night_run:
                    if submit_dt > threshold_time: should_notify = True
                else:
                    should_notify = True
                
                if should_notify:
                    doc_title = doc.get("docDescription")
                    filer_name = doc.get("filerName")
                    doc_id = doc.get("docID")
                    
                    message = {
                        "text": f"📢 *開示情報 ({submit_str})*\n*企業名*: {filer_name}\n*書類*: {doc_title}\n*リンク*: https://disclosure.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
                    }
                    requests.post(webhook_url, json=message)
                    notification_count += 1
        
        # 5. 通知が0件だった場合
        if notification_count == 0:
            time_label = "夜間チェック" if is_night_run else "日中チェック"
            message = {
                "text": f"✅ *開示なし ({today_str} {time_label})*\n監視対象({len(target_edinet_codes)}社)について、当期間での開示はありませんでした。"
            }
            requests.post(webhook_url, json=message)

        return f"Checked {len(results)} docs against {len(target_edinet_codes)} targets. Sent {notification_count}.", 200

    except Exception as e:
        print(f"Error: {e}")
        return f"Internal Error: {e}", 500
