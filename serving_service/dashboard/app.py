import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from sqlalchemy import create_engine
import plotly.express as px

# -------------------------------------------------------------------------
# 1. 페이지 기본 설정 (가장 위에 와야 함)
# -------------------------------------------------------------------------
st.set_page_config(page_title="포트홀 안전 대시보드", page_icon="🛣️", layout="wide")


# -------------------------------------------------------------------------
# 2. DB 연결 및 데이터 로딩 (캐싱 적용으로 속도 최적화)
# -------------------------------------------------------------------------
@st.cache_resource
def init_connection():
    db_info = st.secrets["postgres"]
    engine = create_engine(
        f"postgresql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['dbname']}"
    )
    return engine


engine = init_connection()


# 데이터 갱신을 위해 1시간(3600초)마다 캐시를 지우고 새로 불러옵니다.
@st.cache_data(ttl=3600)
def load_data(query):
    return pd.read_sql(query, engine)


# 구체화된 뷰(Materialized View)에서 데이터 가져오기
with st.spinner('데이터를 불러오는 중입니다...'):
    df_heatmap = load_data("SELECT * FROM mvw_dashboard_heatmap;")
    df_priority = load_data("SELECT * FROM mvw_dashboard_repair_priority ORDER BY priority_rank ASC;")
    df_weekly = load_data("SELECT * FROM mvw_dashboard_weekly_stats ORDER BY dow_num ASC;")

# -------------------------------------------------------------------------
# 3. 대시보드 UI 구성 시작
# -------------------------------------------------------------------------
st.title("🛣️ 포트홀 안전 통합 대시보드")
st.markdown("도로 포트홀 센서 데이터와 시민 민원을 융합한 실시간 위험 구간 분석 대시보드입니다.")
st.divider()

# 최상단: 핵심 지표 (KPI)
kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    st.metric(label="현재 가장 위험한 도로", value=df_priority.iloc[0]['road_name'],
              delta=f"1순위 ({df_priority.iloc[0]['district']})", delta_color="inverse")
with kpi2:
    total_complaints = int(df_priority['complaint_count'].sum())
    st.metric(label="누적 시민 민원 건수", value=f"{total_complaints}건")
with kpi3:
    total_impacts = int(df_priority['total_impacts'].sum())
    st.metric(label="누적 포트홀 충격 감지", value=f"{total_impacts:,}회")

st.write("")  # 여백

# -------------------------------------------------------------------------
# 중간 영역: 1. 위험도 맵 / 2. 우선순위 리스트 (1:1 비율로 넓게 화면 분할)
# -------------------------------------------------------------------------
col1, col2 = st.columns(2)  # [2, 1]에서 2(즉 1:1 비율)로 변경하여 리스트 영역 확장

# [STEP 1] 오른쪽 표(Dataframe)를 먼저 렌더링해서 클릭 이벤트 받아오기
with col2:
    st.subheader("🚨 보수 우선순위 랭킹")
    st.markdown("표의 행을 클릭하면 지도가 해당 위치로 이동합니다. 👆")

    # 화면에 보여줄 컬럼만 추출
    display_df = df_priority[['priority_rank', 'road_name', 'district', 'priority_score', 'complaint_count']].copy()
    display_df.columns = ['순위', '도로명', '관할 구역', '위험 점수', '민원(건)']

    # 표 렌더링 및 클릭(on_select) 활성화
    event = st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=600,  # 세로 길이를 450에서 600으로 늘려 더 많은 행 표시
        on_select="rerun",
        selection_mode="single-row"
    )

    # 사용자가 클릭한 행의 인덱스 번호 가져오기
    selected_rows = event.selection.rows

# [STEP 2] 왼쪽 지도(Map) 렌더링하기 (클릭된 위경도 반영)
with col1:
    st.subheader("📍 도로 보수 시급도 맵")

    # 기본 중심 좌표 (서울) 및 줌 레벨
    center_lat, center_lon = 37.5665, 126.9780
    zoom_level = 11

    # 사용자가 표에서 특정 행을 클릭했다면 좌표 변경
    if selected_rows:
        target_index = selected_rows[0]
        center_lat = df_priority.iloc[target_index]['centroid_lat']
        center_lon = df_priority.iloc[target_index]['centroid_lon']
        zoom_level = 16
        target_road = df_priority.iloc[target_index]['road_name']

    # 지도 생성
    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_level, tiles="CartoDB positron")

    # 전체 우선순위 데이터 마커 추가
    for _, row in df_priority.iterrows():
        score = row['priority_score']

        # 우선순위 점수에 따른 색상 및 크기 분기 (데이터에 맞게 커트라인 조절)
        if score >= 1000:
            color, radius = 'red', 12
        elif score >= 300:
            color, radius = 'orange', 8
        else:
            color, radius = 'green', 5

        tooltip_html = f"""
        <b>{row['road_name']} ({row['district']})</b><br>
        - 우선순위 순위: <b>{row['priority_rank']}위</b><br>
        - 보수 시급 점수: {score}점<br>
        - 접수된 민원: {row['complaint_count']}건
        """

        folium.CircleMarker(
            location=[row['centroid_lat'], row['centroid_lon']],
            radius=radius, color=color, fill=True, fill_opacity=0.6, tooltip=tooltip_html
        ).add_to(m)

    # 클릭된 곳에 특별한 강조 마커 추가
    if selected_rows:
        folium.Marker(
            location=[center_lat, center_lon],
            popup=f"선택됨: {target_road}",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(m)

    # Streamlit 화면에 지도 출력 (표 높이에 맞춰 지도 높이도 600으로 늘림)
    st_folium(m, use_container_width=True, height=600, returned_objects=[])

st.divider()

# 하단 영역: 요일별 통계 차트 (Plotly 활용)
st.subheader("📊 최근 7일 요일별 통계")
if not df_weekly.empty:
    # 요일별 충격 횟수와 통행량을 함께 보여주는 복합 차트 생성
    fig = px.bar(
        df_weekly,
        x='day_of_week',
        y=['impact_count', 'total_count'],
        barmode='group',
        labels={'value': '건수 / 통행량', 'day_of_week': '요일', 'variable': '지표'},
        color_discrete_map={'impact_count': '#EF553B', 'total_count': '#636EFA'}
    )
    # 한글 범례로 변경
    newnames = {'impact_count': '포트홀 충격 횟수', 'total_count': '차량 통행량'}
    fig.for_each_trace(lambda t: t.update(name=newnames[t.name],
                                          legendgroup=newnames[t.name],
                                          hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name])))

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("최근 7일간의 통계 데이터가 없습니다.")