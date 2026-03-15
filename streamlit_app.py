"""
Streamlit 主程序
"""
import streamlit as st

st.set_page_config(
    page_title="Streamlit 应用",
    page_icon="🚀",
    layout="wide",
)

st.title("🚀 Streamlit 应用")
st.write("欢迎使用 Streamlit！这是一个简单的示例应用。")

# 侧边栏
with st.sidebar:
    st.header("设置")
    user_name = st.text_input("你的名字", value="访客")
    show_code = st.checkbox("显示示例代码", value=False)

# 主内容区
st.subheader(f"你好，{user_name}！")

col1, col2, col3 = st.columns(3)
with col1:
    if st.button("点击我"):
        st.success("按钮被点击了！")
with col2:
    number = st.number_input("输入一个数字", min_value=0, max_value=100, value=42)
with col3:
    st.metric("当前数值", number, delta=1)

if show_code:
    st.code("""
import streamlit as st
st.write("Hello, Streamlit!")
""", language="python")

st.divider()
st.caption("使用 Streamlit 构建 · 修改 streamlit_app.py 开始自定义")
