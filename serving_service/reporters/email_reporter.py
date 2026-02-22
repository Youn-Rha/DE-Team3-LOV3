"""
Email Reporter - 포트홀 일일 리포트 이메일 전송

mvw_dashboard_repair_priority, pothole_segments 테이블에서 데이터를 조회하여
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
        """당일 KPI: 탐지 건수, 활성 구간 수, 전일 대비 변화"""
        query = """
            WITH daily_stats AS (
                SELECT
                    date,
                    SUM(impact_count) AS total_impacts,
                    COUNT(DISTINCT s_id) AS active_segments
                FROM pothole_segments
                GROUP BY date
            ),
            latest_two AS (
                SELECT
                    date,
                    total_impacts,
                    active_segments,
                    LAG(total_impacts) OVER (ORDER BY date) AS prev_impacts,
                    LAG(active_segments) OVER (ORDER BY date) AS prev_segments
                FROM daily_stats
            )
            SELECT
                date,
                total_impacts,
                active_segments,
                total_impacts - COALESCE(prev_impacts, 0) AS impact_change,
                active_segments - COALESCE(prev_segments, 0) AS segment_change
            FROM latest_two
            ORDER BY date DESC
            LIMIT 1
        """
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
        """보수 우선순위 (mvw_dashboard_repair_priority MV 사용)"""
        query = f"""
            SELECT
                priority_rank,
                COALESCE(road_name, '-') AS road_name,
                COALESCE(district, '-') AS district,
                priority_score,
                complaint_count,
                centroid_lat,
                centroid_lon
            FROM mvw_dashboard_repair_priority
            ORDER BY priority_rank ASC
            LIMIT {limit}
        """
        try:
            result = self.fetch_query(query)
            segments = []
            for row in result:
                lat = round(row[5], 4) if row[5] else None
                lon = round(row[6], 4) if row[6] else None
                road_name = row[1]
                if lat and lon:
                    road_name = f"{road_name}({lat}, {lon})"
                segments.append({
                    "priority_rank": row[0],
                    "road_name": road_name,
                    "district": row[2],
                    "priority_score": float(row[3]) if row[3] else 0,
                    "complaint_count": row[4],
                })
            return segments
        except Exception as e:
            self.logger.error(f"Failed to fetch repair priority: {e}")
            raise

    def get_worsening_alerts(self, limit=5):
        """악화 구간: rolling 7일 평균 대비 150% 초과 구간"""
        query = f"""
            WITH daily AS (
                SELECT
                    s_id,
                    date,
                    impact_count,
                    AVG(impact_count) OVER (
                        PARTITION BY s_id
                        ORDER BY date
                        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                    ) AS rolling_7d_avg
                FROM pothole_segments
            )
            SELECT
                d.s_id,
                COALESCE(sa.road_name, '-') AS road_name,
                ROUND(
                    (d.impact_count::NUMERIC / NULLIF(d.rolling_7d_avg, 0)) * 100, 1
                ) AS pct_of_avg
            FROM daily d
            LEFT JOIN segment_address sa ON d.s_id = sa.s_id
            WHERE d.rolling_7d_avg > 0
              AND d.impact_count > d.rolling_7d_avg * 1.5
              AND d.date = (SELECT MAX(date) FROM pothole_segments)
            ORDER BY pct_of_avg DESC
            LIMIT {limit}
        """
        try:
            result = self.fetch_query(query)
            alerts = []
            for row in result:
                alerts.append({
                    "s_id": row[0],
                    "road_name": row[1],
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

        priority = self.get_repair_priority(limit=5)

        if not priority:
            return f"<p>No data available for {date}</p>"

        html = f"""
        <html>
            <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
                <h2>포트홀 보수 우선순위 리포트 ({date})</h2>
                <hr style="border: none; border-top: 2px solid #007bff;">

                <h3>보수 우선순위 TOP 5</h3>
                <table style="width: 100%; border-collapse: collapse; margin: 10px 0;">
                    <thead style="background-color: #007bff; color: white;">
                        <tr>
                            <th style="padding: 8px; text-align: left;">순위</th>
                            <th style="padding: 8px; text-align: left;">도로명</th>
                            <th style="padding: 8px; text-align: left;">관할 구역</th>
                            <th style="padding: 8px; text-align: right;">위험 점수</th>
                            <th style="padding: 8px; text-align: right;">민원</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        for i, seg in enumerate(priority, 1):
            row_color = "#ffffff" if i % 2 == 1 else "#f8f9fa"
            html += f"""
                        <tr style="background-color: {row_color};">
                            <td style="padding: 8px; border: 1px solid #ddd;">{seg['priority_rank']}</td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{seg['road_name']}</td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{seg['district']}</td>
                            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{seg['priority_score']:.1f}</td>
                            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{seg['complaint_count']}건</td>
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
