import streamlit as st
import streamlit.components.v1 as components
from io import StringIO
from parse_trace import parse_to_df, create_prompt
from chatUtils import open_client, setup_chat, generate_summary, \
    query_summary_run, get_final_summary, get_all_diagnoses, \
    query_diagnosis_runs, get_final_diagnoses, ISSUE_LABELS, FINAL_STATUS, FAILED_STATUS
import os
import time

# Title
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
high_metadata_io = st.sidebar.checkbox("High Metadata I/O")

issues = {
    ISSUE_LABELS["small_io"]: small_io,
    ISSUE_LABELS["random_io"]: random_io,
    ISSUE_LABELS["load_imbalanced_io"]: load_imbalanced_io,
    ISSUE_LABELS["shared_file_io"]: shared_file_io,
    ISSUE_LABELS["high_metadata_io"]: high_metadata_io
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


def display_diagnosis_status(diagnosis_run_status, selected_issues):
    for i, issue in enumerate(diagnosis_run_status.keys()):
        with st.expander(f"Analyzing {selected_issues[i]} ..."):
            if diagnosis_run_status[issue] == "in_progress":
                st.markdown(f"Analysis in progress...")
            elif diagnosis_run_status[issue] in FAILED_STATUS:
                st.error(f"Analysis failed! Please try again.")
            else:
                st.success(f"Analysis complete! Awaiting full Diagnosis...")


# Render HTML
with open("./assets/app.html") as f:
    page = f.read()
    components.html(page)

# File Upload Form
new_file = None

uploaded_file = st.file_uploader("Please enter your Darshan DXT Trace (txt files only)")
submit = st.button("Analyze Darshan trace!")

if not openai_api_key.startswith("sk-"):
    st.warning("Please make sure a proper OpenAI API Key is entered!", icon="⚠")

if openai_api_key.startswith("sk-") and uploaded_file is not None:
    new_file = parse_file(uploaded_file)

if submit and new_file is not None:
    # Extract selected issues from checklist
    selected_issues = [issue for issue, value in issues.items() if value]
    assistant, chat_file, chat_formatted_issues = setup_chat(chat_client, new_file, selected_issues)
    tabs = st.tabs(chat_formatted_issues)
    tabs = {chat_formatted_issues[i]: tabs[i] for i in range(len(chat_formatted_issues))}

    progress_bars = {}
    for issue in tabs:
        with tabs[issue]:
            progress_bars[issue] = st.progress(0)

    diagnosis_runs, diagnosis_run_status, diagnosis_threads = get_all_diagnoses(chat_client, assistant, chat_file.id,
                                                                                chat_formatted_issues)
    # start a new async thread to check the status of the runs
    in_progress_threads = diagnosis_threads.copy()
    in_progress_runs = diagnosis_runs.copy()
    completed_diagnosis_threads = {}
    completed_diagnosis_runs = {}
    final_diagnoses = {}
    for timeout_step in range(200):
        run_status = query_diagnosis_runs(chat_client, in_progress_threads, in_progress_runs)
        for i, issue in enumerate(run_status.keys()):
            with tabs[issue]:
                if run_status[issue] == "in_progress":
                    cur_val = timeout_step / 199
                    progress_bars[issue].progress(cur_val)
                elif run_status[issue] == "completed":
                    progress_bars[issue].progress(100)
                    completed_diagnosis_runs[issue] = in_progress_runs[issue]
                    completed_diagnosis_threads[issue] = in_progress_threads[issue]
                    # remove the completed runs from the in_progress list
                    in_progress_runs.pop(issue)
                    in_progress_threads.pop(issue)
                elif run_status[issue] in FAILED_STATUS:
                    progress_bars[issue].progress(100)
                    st.error(f"Analysis failed! Please try again.")
                    in_progress_runs.pop(issue)
                    in_progress_threads.pop(issue)

        if len(completed_diagnosis_runs) > 0:
            new_diagnoses, failed_runs = get_final_diagnoses(chat_client, completed_diagnosis_threads,
                                                             completed_diagnosis_runs)
            for i, issue in enumerate(new_diagnoses.keys()):
                with tabs[issue]:
                    with st.expander(f"code"):
                        for input in new_diagnoses[issue]['code_inputs']:
                            st.code(input, language="python", line_numbers=True)
                            st.download_button(
                                label="Download Code",
                                data=input[0],
                                file_name=f"{issue}.py",
                                key=input
                            )
                        for output in new_diagnoses[issue]['code_results']:
                            st.code(output, language="python", line_numbers=True)

                    with st.expander(f"steps"):
                        all_steps = ""
                        for step_num, step in enumerate(new_diagnoses[issue]['steps']):
                            st.markdown(f"**Step: {step_num + 1}**")
                            st.markdown(f"{step}")
                            all_steps += f"\n **Step {step_num + 1}**: \n {step}"

                        st.download_button(
                            label="Download Steps",
                            data=all_steps,
                            file_name=f"{issue}_steps.md",
                            key=f"{issue}_steps"
                        )

                    with st.expander(f"summary"):
                        st.markdown(f"{new_diagnoses[issue]['text']}")

                        st.download_button(
                            label="Download Summary",
                            data=new_diagnoses[issue]['text'],
                            file_name=f"{issue}_summary.md",
                            key=f"{issue}_summary"
                        )
                        if len(new_diagnoses[issue]['images']) > 0:
                            for image_index, image in enumerate(new_diagnoses[issue]['images']):
                                st.image(image['local_path'])
                                st.download_button(
                                    label="Download Image",
                                    data=image['local_path'],
                                    file_name=f"{issue}_image{image_index}.png",
                                    key=image_index
                                )

            final_diagnoses.update(new_diagnoses)
            completed_diagnosis_runs = {}
            completed_diagnosis_threads = {}

        if len(in_progress_runs) == 0:
            break
        time.sleep(1)

    summary_thread, summary_run = generate_summary(chat_client, assistant, final_diagnoses)
    summary_status = query_summary_run(chat_client, summary_thread, summary_run)
    progress_bar = st.progress(0)
    for timeout_step in range(100):
        if summary_status in FINAL_STATUS:
            break
        else:
            progress_bar.progress(timeout_step / 100)
        summary_status = query_summary_run(chat_client, summary_thread, summary_run)
        time.sleep(1)
    summary = get_final_summary(chat_client, summary_thread, summary_run)
    progress_bar.progress(100)
    st.markdown(f"## Summary: \n{summary['text']}")
    st.download_button(
        label="Download Summary",
        data=summary['text'],
        file_name=f"summary.md",
    )
    for image_index, image in enumerate(summary['images']):
        st.image(image['local_path'])
        st.download_button(
            label="Download Image",
            data=summary['images'],
            file_name=f"summary.md",
            key=f"summary_image_{image_index}"
        )
