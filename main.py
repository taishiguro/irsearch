import os
import datetime
import requests
import json

# 設定
# 監視対象のEDINETコードリスト
TARGET_EDINET_CODES = [
    "E03316", # 株式会社ユナイテッドアローズ
    "E04426", # ソフトバンク株式会社
    "E04807", # 株式会社　ＴＫＣ
    "E04877", # 株式会社ミロク情報サービス
    "E04894", # ピー・シー・エー株式会社
    "E05025", # 株式会社オービック
    "E05048", # 株式会社オービックビジネスコンサルタント
    "E05147", # 株式会社電通総研
    "E30969", # 株式会社ＳＨＩＦＴ
    "E31878", # 株式会社ラクス
    "E33039", # 株式会社オロ
    "E33390", # 株式会社マネーフォワード
    "E35325", # フリー株式会社
    "E36658", # グローバルスタイル株式会社

]

# タイムゾーン設定 (JST)
JST = datetime.timezone(datetime.timedelta(hours=9), 'JST')

def check_edinet_and_notify(request):
    try:
        # 1. 環境変数チェック
        webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
        if not webhook_url:
            return "Slack Webhook URL is not set.", 500

        # 2. 現在時刻と判定ロジック
        now = datetime.datetime.now(JST)
        today_str = now.strftime('%Y-%m-%d')
        
        # 閾値となる時刻（15:45）を設定
        threshold_time = now.replace(hour=15, minute=45, second=0, microsecond=0)

        # 現在が「夜の実行(16時以降)」かどうか
        is_night_run = now.hour >= 16

        print(f"Running at: {now}, Is night run?: {is_night_run}")

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
            
            # 対象企業かチェック
            if edinet_code in TARGET_EDINET_CODES:
                
                # 提出時刻のチェック
                submit_str = doc.get("submitDateTime")
                if not submit_str:
                    continue
                    
                # 文字列をdatetimeオブジェクトに変換
                submit_dt = datetime.datetime.strptime(submit_str, '%Y-%m-%d %H:%M')
                submit_dt = submit_dt.replace(tzinfo=JST)

                # 重複防止ロジック
                should_notify = False

                if is_night_run:
                    # 夜(23:00)の実行なら、「15:45以降」に出たものだけ通知
                    if submit_dt > threshold_time:
                        should_notify = True
                else:
                    # 夕方(15:45)の実行なら、今日出たもの(ここまでの分)を全て通知
                    should_notify = True
                
                # 通知実行
                if should_notify:
                    doc_title = doc.get("docDescription")
                    filer_name = doc.get("filerName")
                    doc_id = doc.get("docID")
                    
                    message = {
                        "text": f"📢 *開示情報 ({submit_str})*\n*企業名*: {filer_name}\n*書類*: {doc_title}\n*リンク*: https://disclosure.edinet-fsa.go.jp/api/v2/documents/{doc_id}"
                    }
                    requests.post(webhook_url, json=message)
                    notification_count += 1
        
        # 5. 【追加】通知が0件だった場合の処理
        if notification_count == 0:
            time_label = "夜間チェック" if is_night_run else "日中チェック"
            message = {
                "text": f"✅ *開示なし ({today_str} {time_label})*\n指定された企業について、当期間での開示はありませんでした。"
            }
            requests.post(webhook_url, json=message)

        return f"Checked {len(results)} docs. Sent {notification_count} notifications (or no-data msg).", 200

    except Exception as e:
        print(f"Error: {e}")
        return f"Internal Error: {e}", 500
