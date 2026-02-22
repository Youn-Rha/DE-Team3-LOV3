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
# 2. DB 연결 및 데이터 로딩
# -------------------------------------------------------------------------
@st.cache_resource
def init_connection():
    db_info = st.secrets["postgres"]
    engine = create_engine(
        f"postgresql://{db_info['user']}:{db_info['password']}@{db_info['host']}:{db_info['port']}/{db_info['dbname']}"
    )
    return engine

engine = init_connection()

@st.cache_data(ttl=3600)
def load_data(query):
    return pd.read_sql(query, engine)

with st.spinner('데이터를 불러오는 중입니다...'):
    df_heatmap = load_data("SELECT * FROM mvw_dashboard_heatmap;")
    df_priority = load_data("SELECT * FROM mvw_dashboard_repair_priority ORDER BY priority_rank ASC;")
    df_weekly = load_data("SELECT * FROM mvw_dashboard_weekly_stats ORDER BY dow_num ASC;")

# -------------------------------------------------------------------------
# 3. 시연용 더미(Dummy) 상태 초기화 및 콜백 함수
# -------------------------------------------------------------------------
if 'dummy_requests' not in st.session_state:
    initial_dummy_data = {}
    if len(df_priority) > 5:
        initial_dummy_data[df_priority.iloc[0]['s_id']] = 2
        initial_dummy_data[df_priority.iloc[1]['s_id']] = 1
    st.session_state.dummy_requests = initial_dummy_data

def handle_repair_request(sid, road_name):
    st.session_state.dummy_requests[sid] = 2
    st.toast(f"[{road_name}] 보수 요청이 성공적으로 서버에 전달되었습니다!", icon="✅")

# -------------------------------------------------------------------------
# 4. 대시보드 UI 구성 시작
# -------------------------------------------------------------------------
st.header("🛣️ 포트홀 안전 통합 대시보드")

# 집계 날짜 표기 (크게 키운 디자인 적용)
if not df_priority.empty and 'date' in df_priority.columns:
    latest_date = str(df_priority.iloc[0]['date'])[:10]
    st.subheader("📅 데이터 기준일 : " + latest_date + " (전일)")
else:
    st.subheader("📅 데이터 기준일 : 실시간")

st.markdown("<br>자동차의 센서 데이터와 시민 민원을 융합한 포트홀 탐지 및 보수 우선순위 분석 대시보드입니다.", unsafe_allow_html=True)

# =========================================================================
# [상단 영역] 보수 우선순위 리스트 (전체 너비 사용)
# =========================================================================
st.subheader("🚨 보수 우선순위 랭킹")
st.markdown("표에서 행을 선택하여 상세 위치를 확인하고, **보수 요청**을 진행하세요. 👆")

display_df = df_priority[['priority_rank', 'road_name', 'district', 'priority_score', 'complaint_count', 's_id']].copy()
display_df['request'] = display_df['s_id'].apply(lambda x: st.session_state.dummy_requests.get(x, 0))

def get_status_text(val):
    if val == 1: return "✅ 완료"
    elif val == 2: return "🚧 진행중"
    else: return "❌ 미완료"
    
display_df['진행 상태'] = display_df['request'].apply(get_status_text)

show_df = display_df[['priority_rank', 'road_name', 'district', 'priority_score', 'complaint_count', '진행 상태']]
show_df.columns = ['순위', '도로명', '관할 구역', '위험 점수', '민원(건)', '진행 상태']

# 표 렌더링 (가로 전체를 쓰므로 세로 높이는 300 정도로 살짝 줄여서 한눈에 들어오게 함)
event = st.dataframe(
    show_df, 
    width='stretch', 
    hide_index=True, 
    height=300,
    on_select="rerun",           
    selection_mode="single-row",
    key="priority_table"
)

selected_rows = event.selection.rows

# [상단 제어(버튼) 영역] 화면을 꽉 채우지 않도록 2개의 컬럼으로 나누어 깔끔하게 배치
btn_col1, btn_col2 = st.columns([7, 3])

with btn_col1:
    if not selected_rows:
        st.info("💡 위 표에서 행을 선택하면 여기에 보수 요청 버튼이 나타납니다.")
    else:
        target_index = selected_rows[0]
        selected_road = display_df.iloc[target_index]['road_name'] 
        current_status = display_df.iloc[target_index]['request']  
        
        if current_status == 0:
            st.info(f"현재 **{selected_road}** 구간은 보수 접수가 되지 않았습니다.")
        elif current_status == 2:
            st.warning(f"🚧 **{selected_road}** 구간은 현재 보수 공사가 진행 중입니다.")
        elif current_status == 1:
            st.success(f"✅ **{selected_road}** 구간은 보수 공사가 완료되었습니다.")

with btn_col2:
    if not selected_rows:
        st.button("🚨 보수 요청하기", disabled=True, width='stretch', key="btn_default")
    else:
        target_index = selected_rows[0]
        selected_sid = display_df.iloc[target_index]['s_id']       
        selected_road = display_df.iloc[target_index]['road_name'] 
        current_status = display_df.iloc[target_index]['request']
        
        if current_status == 0:
            st.button(
                f"🚨 {selected_road} 보수 요청", 
                type="primary", 
                width='stretch', 
                on_click=handle_repair_request,
                args=(selected_sid, selected_road),
                key=f"btn_req_{selected_sid}"
            )
        elif current_status == 2:
            st.button(f"요청 완료 (진행중)", disabled=True, width='stretch', key=f"btn_dis_2_{selected_sid}")
        elif current_status == 1:
            st.button(f"요청 완료 (완료됨)", disabled=True, width='stretch', key=f"btn_dis_1_{selected_sid}")

st.markdown("<br>", unsafe_allow_html=True)
st.divider()

# =========================================================================
# [하단 영역] 좌측: 지도 (60%) / 우측: KPI 및 통계 차트 (40%)
# =========================================================================
col_map, col_stats = st.columns([6, 4])

# [하단 좌측] 지도 영역
with col_map:
    st.subheader("📍 도로 보수 시급도 맵") 
    
    center_lat, center_lon = 37.5665, 126.9780
    zoom_level = 11
    target_road = None
    
    if selected_rows:
        target_index = selected_rows[0]
        center_lat = df_priority.iloc[target_index]['centroid_lat']
        center_lon = df_priority.iloc[target_index]['centroid_lon']
        zoom_level = 16 
        target_road = df_priority.iloc[target_index]['road_name']

    m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_level, tiles="CartoDB positron")
    
    for _, row in df_priority.iterrows():
        score = row['priority_score']
        if score >= 1000:       
            color, radius = 'red', 12
        elif score >= 300:      
            color, radius = 'orange', 8
        else:                   
            color, radius = 'green', 5
            
        status_val = st.session_state.dummy_requests.get(row['s_id'], 0)
        status_text = "❌ 미완료" if status_val == 0 else ("🚧 진행중" if status_val == 2 else "✅ 완료")

        tooltip_html = f"""
        <b>{row['road_name']} ({row['district']})</b><br>
        - 우선순위 순위: <b>{row['priority_rank']}위</b><br>
        - 보수 시급 점수: {score}점<br>
        - 접수 민원: {row['complaint_count']}건<br>
        - <b>진행 상태: {status_text}</b>
        """
        
        folium.CircleMarker(
            location=[row['centroid_lat'], row['centroid_lon']],
            radius=radius, color=color, fill=True, fill_opacity=0.6, tooltip=tooltip_html
        ).add_to(m)

    if selected_rows and target_road:
        folium.Marker(
            location=[center_lat, center_lon],
            popup=f"선택됨: {target_road}",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(m)

    st_folium(m, width='stretch', height=600, returned_objects=[], key="pothole_map")

# [하단 우측] KPI 요약 및 차트 영역
with col_stats:
    st.subheader("📈 핵심 지표 요약")
    if not df_priority.empty:
        # 가장 중요도 높은 1순위 도로를 강조
        st.metric(
            label="현재 가장 위험한 도로", 
            value=df_priority.iloc[0]['road_name'], 
            delta=f"1순위 ({df_priority.iloc[0]['district']})", 
            delta_color="inverse"
        )
        
        # 민원과 충격 감지 건수를 나란히 배치
        kpi1, kpi2 = st.columns(2)
        with kpi1:
            total_complaints = int(df_priority['complaint_count'].sum())
            st.metric(label="누적 시민 민원", value=f"{total_complaints}건")
        with kpi2:
            total_impacts = int(df_priority['total_impacts'].sum())
            st.metric(label="누적 충격 감지", value=f"{total_impacts:,}회")
            
    st.divider()

    st.subheader("📊 최근 7일 요일별 통계")
    if not df_weekly.empty:
        fig = px.bar(
            df_weekly, 
            x='day_of_week', 
            y=['impact_count', 'total_count'],
            barmode='group',
            labels={'value': '건수 / 통행량', 'day_of_week': '요일', 'variable': '지표'},
            color_discrete_map={'impact_count': '#EF553B', 'total_count': '#636EFA'}
        )
        newnames = {'impact_count':'포트홀 충격 횟수', 'total_count': '차량 통행량'}
        fig.for_each_trace(lambda t: t.update(name = newnames[t.name],
                                            legendgroup = newnames[t.name],
                                            hovertemplate = t.hovertemplate.replace(t.name, newnames[t.name])))
        
        # 차트가 우측 하단 여백에 딱 맞게 들어가도록 여백(margin) 최적화
        fig.update_layout(margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("최근 7일간의 통계 데이터가 없습니다.")