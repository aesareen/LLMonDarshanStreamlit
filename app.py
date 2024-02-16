import streamlit as st
import streamlit.components.v1 as components
from io import StringIO
from parse_trace import parse_to_df, create_prompt
from chatUtils import open_client, generate_analysis, ISSUE_LABELS
import os

#Title
st.set_page_config(page_title="ION: I/O Navigator")

# Sidebar
st.sidebar.title("Options")
openai_api_key = st.sidebar.text_input("OpenAI API Key", type="password")
os.environ["OPENAI_API_KEY"] = openai_api_key

chat_client = open_client()

st.sidebar.header("Issues to Analyze: ")

small_io = st.sidebar.checkbox("Small I/O")
random_io = st.sidebar.checkbox("Random I/O")
load_imbalanced_io = st.sidebar.checkbox("Load Imbalance")
shared_file_io = st.sidebar.checkbox("Shared File I/O")

issues = {
    ISSUE_LABELS["small_io"]: small_io,
    ISSUE_LABELS["random_io"]: random_io,
    ISSUE_LABELS["load_imbalanced_io"]: load_imbalanced_io,
    ISSUE_LABELS["shared_file_io"]: shared_file_io
}


def parse_file(uploaded_file) -> str:
    """
    Verifies that a proper text file is uploaded and then parses the log file into a CSV file, returning the file path
    :param uploaded_file:
    :return:
    """
    if uploaded_file is None:
        st.warning("Please make sure you uploaded a proper Darshan trace!", icon="⚠")
    else:
        if not uploaded_file.name.endswith(".txt"):
            st.warning("Please make sure you upload a proper .txt file!", icon="⚠")
        else:
            try:
                stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
                string_data = stringio.read()
                df, trace_start_time, full_runtime = parse_to_df(string_data)
                file_path = f'csv/{uploaded_file.name.split(".")[0]}.csv'
                df.to_csv(file_path, index=False)

                st.success("File successfully parsed and saved!", icon="✅")
                return file_path
            except Exception as e:
                st.exception(f"I am sorry. Something wrong occurred, please try again: {e}")


# Render HTML
with open("./assets/app.html") as f:
    page = f.read()
    components.html(page)

# File Upload Form
new_file = None
with st.form("main_form"):
    uploaded_file = st.file_uploader("Please enter your Darshan DXT Trace (txt files only)")
    submit = st.form_submit_button("Analyze Darshan trace!")

    if not openai_api_key.startswith("sk-"):
        st.warning("Please make sure a proper OpenAI API Key is entered!", icon="⚠")

    if openai_api_key.startswith("sk-") and uploaded_file is not None:
        new_file = parse_file(uploaded_file)
        print(new_file)

    if submit and new_file is not None:
        #Extract selected issues from checklist
        selected_issues = [issue for issue, value in issues.items() if value]
        print(selected_issues)
        diagnoses, summary, failures = generate_analysis(chat_client, new_file, selected_issues)
        print(summary)
        for i, issue in enumerate(diagnoses.keys()):
            with st.expander(f"{selected_issues[i]} Diagnosis: "):
                st.markdown(diagnoses[issue])
        if summary:
            st.markdown(f"## Summary: \n{summary}")



