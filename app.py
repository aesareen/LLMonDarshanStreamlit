import streamlit as st
import streamlit.components.v1 as components
from io import StringIO
from parse_trace import parse_to_df, create_prompt

# Sidebar
st.sidebar.title("Options")
openai_api_key = st.sidebar.text_input("OpenAI API Key", type="password")

st.sidebar.header("Issues to Analyze: ")

small_io = st.sidebar.checkbox("Small I/O")
random_io = st.sidebar.checkbox("Random I/O")
load_imbalanced_io = st.sidebar.checkbox("Load Imbalance")
shared_file_io = st.sidebar.checkbox("Shared File I/O")


def parse_file(uploaded_file, issues):
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
                df.to_csv(f'csv/{uploaded_file.name.split(".")[0]}.csv', index=False)

                for issue, value in issues.items():
                    if value:
                        prompt = create_prompt(string_data, df, issue)
                        prompt_name = f"{issue} - {uploaded_file.name.split('.')[0]}"

                        with open(f"prompts/{prompt_name}", 'w') as f:
                            f.write(prompt)

                st.success("File successfully parsed and saved!", icon="✅")
            except Exception as e:
                st.exception(f"I am sorry. Something wrong occurred, please try again: {e}")

components.html(
    """
     <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
    <div class="text-center">
        <h1>ION: I/O Navigator</h1>
        <h3> <em>Guiding Users to Optimal I/O performance</em> </h3>
    </div>
    """
)

with st.form("main_form"):
    uploaded_file = st.file_uploader("Please enter your Darshan DXT Trace (txt files only)")
    submit = st.form_submit_button("Analyze Darshan trace!")

    if not openai_api_key.startswith("sk-"):
        st.warning("Please make sure a proper OpenAI API Key is entered!", icon="⚠")

    if openai_api_key.startswith("sk-") and submit:
        issues = {
            "small_io": small_io,
            "random_io": random_io,
            "load_imbalanced_io": load_imbalanced_io,
            "shared_file_io_extended": shared_file_io
        }
        parse_file(uploaded_file, issues)
