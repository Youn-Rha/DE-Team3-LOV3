"""
producer 설정

키네시스 스트림 명 작성
"""

import os

STREAM_NAME = os.environ.get("S3_STREAM_NAME")