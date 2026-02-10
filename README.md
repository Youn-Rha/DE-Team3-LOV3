# DE-Team3-LOV3

# 🚗 포트홀 조기 탐지 시스템 (Pothole Early Detection System)

> 차량 센서 데이터 기반 실시간 포트홀 위치 탐지 및 보수 우선순위 산정 시스템


##  1. 프로젝트 개요

### 배경 및 문제점

**여수공단(여수국가산업단지) 주변 지방도 863호선**은 화물차 통행량이 많아 무거운 차량 하중으로 인한 도로 피로 파손 및 포트홀이 빈번하게 발생합니다.

#### 현황
- **일간 교통량**: 약 5,000대 (화물차 비중 높음)
- **특징**: 위험 시설물 인접으로 사고 발생 시 2차 피해 가능성 큼
- **전라남도 포장도 유지관리 사업비**: 83.19억 원

#### 기존 방식의 한계
지방도는 **PMS(Pavement Management System)** 를 예산 문제로 사용하지 못해 포트홀 발견을 민원 신고에 의존하고 있습니다.

| 구분 | 비용 | 한계점 |
|------|------|--------|
| **정밀 장비 도입** | 5~9억 원 | 초기 투자 비용 과다 |
| **2~3년 주기 조사** | 약 6.8억 원 | 실시간 모니터링 불가 |
| **상시 운영 시** | 약 2,482억 원 (추정) | 재정적 부담 과다 |

### 목적 및 기대효과

**저비용 고효율 포트홀 탐지 시스템 구축**을 통해:
- ✅ 신속한 포트홀 위치 파악 (1일 이내)
- ✅ 보수 우선순위 자동 산정
- ✅ 사고 및 2차 피해 예방
- ✅ 예산 대비 최대 효율 달성


##  2. 솔루션

### 핵심 아이디어

**차량 센서 데이터**를 활용하여 포트홀을 탐지합니다.

1. **데이터 수집**: 차량의 가속도계, 자이로스코프, GPS 센서로 주행 중 충격 패턴 자동 감지
2. **위치 매핑**: GPS와 결합하여 포트홀 발생 위치를 정확히 수집
3. **통계 분석**: z-검정 기반 이상치 탐지로 진짜 포트홀 구간 식별
4. **시각화**: 대시보드에서 실시간 현황 확인 및 보수 우선순위 결정


##  3. 기술 스택

<table border="1" style="border-collapse: collapse;">
  <tr>
    <td align="center" width="150"><b>분류</b></td>
    <td align="center"><b>기술 스택</b></td>
  </tr>
  <tr>
    <td align="center"><b>데이터 처리</b></td>
    <td>
      <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
      <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
      <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
    </td>
  </tr>
  <tr>
    <td align="center"><b>시각화</b></td>
    <td>
      <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white"/>
      <img src="https://img.shields.io/badge/Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white"/>
      <img src="https://img.shields.io/badge/Folium-77B829?style=for-the-badge&logo=leaflet&logoColor=white"/>
    </td>
  </tr>
  <tr>
    <td align="center"><b>인프라</b></td>
    <td>
      <img src="https://img.shields.io/badge/AWS%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white"/>
      <img src="https://img.shields.io/badge/AWS%20EMR-FF9900?style=for-the-badge&logo=amazon&logoColor=white"/>
      <img src="https://img.shields.io/badge/AWS%20RDS-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/>
      <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
    </td>
  </tr>
</table>



##  4. 데이터 파이프라인




##  5. 팀원 소개

<table border="1" style="border-collapse: collapse;">
  <tr>
    <td align="center"><a href="https://github.com/Youn-Rha"><b>라연</b></a></td>
    <td align="center"><a href="https://github.com/statjhw"><b>장현우</b></a></td>
    <td align="center"><a href="https://github.com/Jo-Hyeonu"><b>조현우</b></a></td>
  </tr>
  <tr>
    <td align="center"><img src="https://github.com/Youn-Rha.png" width="150px;" alt=""/></td>
    <td align="center"><img src="https://github.com/statjhw.png" width="150px;" alt=""/></td>
    <td align="center"><img src="https://github.com/Jo-Hyeonu.png" width="150px;" alt=""/></td>
  </tr>
  <tr>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
    <td align="center"><b>DE</b></td>
  </tr>
</table>





