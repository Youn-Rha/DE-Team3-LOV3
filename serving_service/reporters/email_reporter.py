"""
Email Reporter - 포트홀 일일 리포트 이메일 전송

daily_kpi, worsening_alert, repair_priority 뷰에서 데이터를 조회하여
Gmail SMTP로 일일 리포트 이메일을 전송.
"""

import os
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from sqlalchemy import text

from loaders.base_loader import BaseLoader


class EmailReporter(BaseLoader):
    """이메일 보고서 생성 및 전송"""

    def __init__(self, config_path="config.yaml"):
        super().__init__(config_path)
        self.gmail_address = os.environ.get("GMAIL_ADDRESS")
        self.gmail_password = os.environ.get("GMAIL_PASSWORD")

        if not self.gmail_address or not self.gmail_password:
            self.logger.warning(
                "Gmail credentials not set. "
                "Set GMAIL_ADDRESS and GMAIL_PASSWORD environment variables."
            )

    def get_daily_kpi(self):
        """daily_kpi 뷰에서 당일 KPI 조회"""
        query = "SELECT date, total_impacts, active_segments, impact_change, segment_change FROM daily_kpi"
        try:
            result = self.fetch_query(query)
            if result:
                row = result[0]
                return {
                    "date": str(row[0]),
                    "total_impacts": row[1],
                    "active_segments": row[2],
                    "impact_change": row[3],
                    "segment_change": row[4],
                }
            return None
        except Exception as e:
            self.logger.error(f"Failed to fetch daily KPI: {e}")
            raise

    def get_repair_priority(self, limit=5):
        """repair_priority 뷰 + segment_address JOIN으로 보수 우선순위 조회"""
        query = f"""
            SELECT
                rp.s_id,
                sa.road_name,
                rp.total_impacts,
                rp.detected_days,
                rp.impact_ratio
            FROM repair_priority rp
            LEFT JOIN segment_address sa ON rp.s_id = sa.s_id
            ORDER BY rp.total_impacts DESC
            LIMIT {limit}
        """
        try:
            result = self.fetch_query(query)
            segments = []
            for row in result:
                segments.append({
                    "s_id": row[0],
                    "road_name": row[1] or "-",
                    "total_impacts": row[2],
                    "detected_days": row[3],
                    "impact_ratio": float(row[4]) if row[4] else 0,
                })
            return segments
        except Exception as e:
            self.logger.error(f"Failed to fetch repair priority: {e}")
            raise

    def get_worsening_alerts(self, limit=5):
        """worsening_alert 뷰 + segment_address JOIN으로 악화 구간 조회"""
        query = f"""
            SELECT
                wa.s_id,
                sa.road_name,
                wa.pct_of_avg
            FROM worsening_alert wa
            LEFT JOIN segment_address sa ON wa.s_id = sa.s_id
            ORDER BY wa.pct_of_avg DESC
            LIMIT {limit}
        """
        try:
            result = self.fetch_query(query)
            alerts = []
            for row in result:
                alerts.append({
                    "s_id": row[0],
                    "road_name": row[1] or "-",
                    "pct_of_avg": float(row[2]),
                })
            return alerts
        except Exception as e:
            self.logger.error(f"Failed to fetch worsening alerts: {e}")
            raise

    def generate_email_body(self, date=None):
        """이메일 본문 생성"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        kpi = self.get_daily_kpi()
        alerts = self.get_worsening_alerts(limit=5)
        priority = self.get_repair_priority(limit=5)

        if not kpi:
            return f"<p>No data available for {date}</p>"

        # 변화량 부호 표시
        def sign(val):
            return f"+{val}" if val > 0 else str(val)

        html = f"""
        <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <h2>포트홀 일일 리포트 ({kpi['date']})</h2>
                <hr style="border: none; border-top: 2px solid #007bff;">

                <h3>Daily KPI</h3>
                <table style="width: 100%; border-collapse: collapse; margin: 10px 0;">
                    <tr style="background-color: #f8f9fa;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>탐지 건수</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; font-size: 18px;">
                            <strong>{kpi['total_impacts']}</strong>건
                            (전일 대비 {sign(kpi['impact_change'])})
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>활성 구간</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; font-size: 18px;">
                            <strong>{kpi['active_segments']}</strong>개
                            (전일 대비 {sign(kpi['segment_change'])})
                        </td>
                    </tr>
                </table>
        """

        # 악화 구간
        if alerts:
            html += """
                <h3>악화 구간</h3>
                <table style="width: 100%; border-collapse: collapse; margin: 10px 0;">
                    <thead style="background-color: #dc3545; color: white;">
                        <tr>
                            <th style="padding: 8px; text-align: left;">#</th>
                            <th style="padding: 8px; text-align: left;">세그먼트</th>
                            <th style="padding: 8px; text-align: left;">도로명</th>
                            <th style="padding: 8px; text-align: right;">7일 평균 대비</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for i, a in enumerate(alerts, 1):
                row_color = "#ffffff" if i % 2 == 1 else "#f8f9fa"
                html += f"""
                        <tr style="background-color: {row_color};">
                            <td style="padding: 8px; border: 1px solid #ddd;">{i}</td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{a['s_id']}</td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{a['road_name']}</td>
                            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">+{a['pct_of_avg']:.0f}%</td>
                        </tr>
                """
            html += """
                    </tbody>
                </table>
            """

        # 보수 우선순위
        if priority:
            html += """
                <h3>보수 우선순위 TOP 5</h3>
                <table style="width: 100%; border-collapse: collapse; margin: 10px 0;">
                    <thead style="background-color: #007bff; color: white;">
                        <tr>
                            <th style="padding: 8px; text-align: left;">#</th>
                            <th style="padding: 8px; text-align: left;">세그먼트</th>
                            <th style="padding: 8px; text-align: left;">도로명</th>
                            <th style="padding: 8px; text-align: right;">누적 건수</th>
                            <th style="padding: 8px; text-align: right;">탐지 일수</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for i, seg in enumerate(priority, 1):
                row_color = "#ffffff" if i % 2 == 1 else "#f8f9fa"
                html += f"""
                        <tr style="background-color: {row_color};">
                            <td style="padding: 8px; border: 1px solid #ddd;">{i}</td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{seg['s_id']}</td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{seg['road_name']}</td>
                            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{seg['total_impacts']}건</td>
                            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{seg['detected_days']}일</td>
                        </tr>
                """
            html += """
                    </tbody>
                </table>
            """

        html += """
                <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
                <footer style="font-size: 12px; color: #999; text-align: center;">
                    <p>이 보고서는 자동으로 생성되었습니다. | 포트홀 탐지 시스템</p>
                </footer>
            </body>
        </html>
        """

        return html

    def send_email(self, recipient_email, date=None):
        """Gmail SMTP로 이메일 전송"""
        if not self.gmail_address or not self.gmail_password:
            self.logger.error(
                "Gmail credentials not configured. "
                "Set GMAIL_ADDRESS and GMAIL_PASSWORD environment variables."
            )
            return False

        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            subject = f"[포트홀 탐지] 일일 리포트 - {date}"
            html_body = self.generate_email_body(date)

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.gmail_address
            msg["To"] = recipient_email

            part = MIMEText(html_body, "html")
            msg.attach(part)

            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(self.gmail_address, self.gmail_password)
                server.sendmail(self.gmail_address, recipient_email, msg.as_string())

            self.logger.info(f"Email sent to {recipient_email} for {date}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")
            return False


def send_daily_report(recipient_email, date=None, config_path="config.yaml"):
    """Airflow Task용 메인 함수"""
    reporter = EmailReporter(config_path)
    try:
        reporter.send_email(recipient_email, date)
    finally:
        reporter.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Send daily pothole report email")
    parser.add_argument("--email", required=True, help="Recipient email address")
    parser.add_argument("--date", help="Report date (YYYY-MM-DD)")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--test", action="store_true", help="Test mode: show email content without sending")

    args = parser.parse_args()

    reporter = EmailReporter(args.config)

    if args.test:
        print("\n=== 이메일 테스트 모드 ===\n")
        html_body = reporter.generate_email_body(args.date)
        print(html_body)
        print("\n=== 테스트 완료 ===\n")
    else:
        reporter.send_email(args.email, args.date)

    reporter.close()
