from openai import OpenAI
import requests
import time


ISSUES = {
    'small_io': "HPC I/O works in the following way: All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The I/O requests will be transferred using RPC (remote procedure call).  If an I/O request is smaller than the RPC size, it will be aggregated with others before being sent to the OSTs if they are sequential meaning the issue of small requests is largely mitigated automatically. Otherwise, it may lead to inefficient use of the RPC channel since a RPC transfer includes connection building and destroying overheads. The small I/O can mostly be ignored if the application only accesses a file once or twice via small I/O requests, because it is common for an application to load small configuration file, which tends to create small I/O requests. However, repetitive I/O requests to the same file which are significantly smaller than the RPC size may be an issue. Note that, the system on which the trace was collected is configured with a page size of 4kb and max_pages_per_rpc set to 1024, which indicates that the maximum RPC size is 4MB.\n\
                    To diagnose the issue, first analyze the size of the I/O requests, then check to see if any of the requests identified as small are accessing the same files multiple times, and finally check if any of the small requests might be aggregated based on their access patterns. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                    Diagnosis: <summary of your diagnosis>",
    'random_io': "HPC I/O works in the following way: All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The I/O requests will be transferred using RPC (remote procedure call). When applications send I/O requests they are usually buffered in local memory and may be aggregated with other I/O requests if they target nearby pages. If the I/O requests, aggregated or not, are equal to or larger than the maximum RPC size, they fully utilize the RPC channel and the server-side buffer. However, I/O requests which are significantly smaller than the RPC size after aggregation do not use the RPC channel effectively and may lead to I/O performance issues. Additionally, this issue is only of concern when the described behavior is highly repetitive as it does not impact performance if such I/O patterns are only used to access a file a small number of times. Specifically, this type of issue is often referred to as a random access pattern issue.. Note that, the system on which the trace was collected is configured with a page size of 4kb and max_pages_per_rpc set to 1024, which indicates that the maximum RPC size is 4MB.\n\
                    Please use the provided information and think step by step to diagnose whether the attached trace file contains any random access patterns which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                    Diagnosis: <summary of your diagnosis>",
    'load_imbalanced_io': "HPC I/O works in the following way: All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The I/O requests will be transferred using RPC (remote procedure call). If multiple ranks are requesting I/O operations concurrently but their sizes are unbalanced, it may lead some ranks to issue more I/O requests in a given time interval which may become a bottleneck. This is known as a load balance issue. However, in some cases, a set of ranks may process much faster than others so it is beneficial for  these ranks to account for a larger share of I/O requests so it is very important to roughly measure the speed of the various ranks by analyzing throughput or I/O operations per second for each one prior to making any judgements regarding any load balance issues.\n\
                        Please use the provided information and think step by step to diagnose whether the attached trace file contains any load imbalanced I/O behavior which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                        Diagnosis: <summary of your diagnosis>",
    'shared_file_io': "HPC I/O works in the following way. All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The OST servers split data files into chunks which are known as stripes, where each stripe has a size of stripe_size the stripes of a file are stored across stripe_count different OST servers. If the I/O requests target different files, then they are called independent file accesses. If they target different regions of the same file, then they are called shared file accesses. Generally, independent file accesses are more efficient because each process conducts I/O independently towards its own file. But, if there are too many processes reading and writing to too many independent files, the metadata load may become an issue as the load on the metadata servers may be very high. Since shared file accesses only access one file, the metadata servers will not have a very high load, but many processes accessing the same file may complicate data access for the OSTs. This is because these processes may access overlapping areas of the file, introducing conflicts and lock overheads. Even if these requests are not overlapped, they may be accessing the same data stripe of the file (i.e. two request offsets are within the range of the stripe size of the file). This will also lead to lower performance as they introduce conflicts. The I/Os will be sent to the same OST server as well, reducing the parallelism. The files accessed in the application trace have a stripe_size of 1MB and a stripe_count of 1.\n\
                        To identify if an application has shared file issue, we need to take following items into consideration\n\
                            1. Many or all processes access the same file. \n\
                            2. Accessing the same file happens repeatedly.\n\
                            3. Requests from different processes are overlapped in terms of time.\n\
                            4. Requests from different processes fall into the same file stripe, i.e., the offsets of these requests are within the stripe size of the file. The stripe size is 1MB.\n\
                        Please use the provided information and think step by step to diagnose whether the attached trace file contains any shared file I/O behavior which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                        Diagnosis: <summary of your diagnosis>",
    'high_metadata_io': "HPC I/O works in the following way. All the ranks (processes) running on different computing nodes will issue metadata I/O requests to different MDS servers, which are the metadata servers. If an application has a high metadata issue, it means that the application is issuing a large number of metadata I/O requests. If the amount metadata operations approaches the total size or amount of regular I/O operations and these are in quick succession, it may cause uneccessary strain on the metadata servers, and in extreme cases, it may create a bottleneck for the application an the system.\n\
                        To identify if an application has a high metadata issue, we need to take following items into consideration\n\
                            1. The amount of time conducting metadata operations is similar to that of regular I/O operations\n\
                            2. The number of metadata operations is comparable to regular I/O operations and both are significant.\n\
                            3. The amount of time conducting metadata operations is significant to the total runtime of the application.\n\
                        Please use the provided information and think step by step to diagnose whether the attached trace file contains any high metadata I/O behavior which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                        Diagnosis: <summary of your diagnosis>"
                            
}
ISSUE_LABELS = {
    'small_io': "Small I/O",
    'random_io': "Random I/O",
    'load_imbalanced_io': "Load Imbalanced I/O",
    'shared_file_io': "Shared File I/O",
    'high_metadata_io': "High Metadata I/O"
}
COLUMN_DESCRIPTION = {
        "file_id": "unique ID assigned to each file",
        "file_name": "Path and name of the file",
        "api": "I/O library being used",
        "rank": "MPI rank from which the operation was called",
        "operation": "type of I/O call ('read', 'write', 'open', 'stat')",
        "segment": "portion of a file that is accessed during an I/O operation",
        "offset": "position within a file where a particular I/O operation begins",
        "size": "amount of data read from or written to a file during an I/O operation in bytes",
        "start": "unix timestamp of the start of the I/O operation",
        "end": "unix timestamp of the end of the I/O operation",
        "ost": "lustre OST used by the I/O operation",
        "consec": "boolean to indicate if current offset is greater than the previous offset+size",
        "seq": "boolean to indicate if current offset is equal to the previous offset + size"
}
FINAL_STATUS = ['completed', 'expired', 'cancelled', 'failed']
FAILED_STATUS = ['expired', 'cancelled', 'failed']

SUMMARY_TEMPLATE = "You are an expert in HPC I/O performance analysis. You will be given a list of diagnosis summaries for a number of different I/O related issues originating from the same application trace log. Your job is to carefully analyze each of these summaries and form a conclusion which indicates the most prominent I/O performance issues for the underlying application. Here is the list of summaries, organized by issue type: \n"

def format_prompt(issue):
    #header = parse_darshan_log_header(file)

    prompt = f"""
        I have attached a csv file which you can load into a dataframe using pandas. The csv contains I/O trace information from an application run on an HPC system and the data was collected using darshan. The data contains the following columns:

        {COLUMN_DESCRIPTION}

        {ISSUES[issue]}
    """
    return prompt

def create_selected_issues(issues):
    selected_issues = []
    for key, value in ISSUE_LABELS.items():
        if value in issues:
            selected_issues.append(key)
    return selected_issues

def open_client():
    client = OpenAI()
    return client

def create_assistant(client, file_id):
    assistant = client.beta.assistants.create(
        instructions="Please diagnose the attached I/O trace file for any issues",
        model='gpt-4-1106-preview',
        tools=[{"type": "code_interpreter"}],
        file_ids=[file_id]
    )
    return assistant

def add_file(client, file_path):
    file = client.files.create(
        file=open(file_path, "rb"),
        purpose='assistants'
    )
    return file

def get_thread_status(client, thread_id, run_id):
    status = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id).status
    return status

def run_threads(client, assistant, threads):
    runs = {}
    run_status = {}
    for issue in threads:
        runs[issue] = (client.beta.threads.runs.create(
            thread_id=threads[issue].id,
            assistant_id=assistant.id
        ))
        run_status[issue] = get_thread_status(client, threads[issue].id, runs[issue].id) 

    return runs, run_status

def create_diagnosis_prompt(issue, file_id):
    prompt = format_prompt(issue)
    message = {
        'role': 'user',
        'content': prompt,
        'file_ids': [file_id]
    }
    return message

def get_all_diagnoses(client, assistant, file_id, selected_issues):
    threads = {}
    for issue in selected_issues:
        message = create_diagnosis_prompt(issue, file_id)
        threads[issue] = client.beta.threads.create(
            messages=[message]
        )

    runs, run_status = run_threads(client, assistant, threads)

    return runs, run_status, threads

def query_diagnosis_runs(client, threads, runs):
    run_status = {issue: get_thread_status(client, threads[issue].id, runs[issue].id) for issue in threads}
    return run_status

def extract_code_from_run_steps(run_steps):
    code_inputs = []
    code_results = []
    # iterate through the steps in reverse order
    for step in run_steps[::-1]:
        if step.step_details.type == 'tool_calls':
            for tool_call in step.step_details.tool_calls:
                failed = False
                if tool_call.type == 'code_interpreter':
                    input_code = tool_call.code_interpreter.input
                    results = ""
                    outputs = tool_call.code_interpreter.outputs
                    for output in outputs:
                        if output.type == 'logs':
                            if "Traceback" in output.logs:
                                failed = True
                                break
                            results += output.logs
                    if not failed:
                        code_inputs.append(input_code)
                        code_results.append(results)
    return code_inputs, code_results

def get_final_diagnoses(client, threads, runs):
    diagnoses = {}
    failed_runs = {}
    run_status = {issue: get_thread_status(client, threads[issue].id, runs[issue].id) for issue in threads}
    for issue in threads:
        diagnoses[issue] = {}
        if run_status[issue] in FAILED_STATUS:
            failed_runs[issue] = runs[issue]
        else:
            run_steps = client.beta.threads.runs.steps.list(thread_id=threads[issue].id, run_id=runs[issue].id).data
            code_inputs, code_results = extract_code_from_run_steps(run_steps)
            thread_messages = client.beta.threads.messages.list(thread_id=threads[issue].id).data
            final_message = thread_messages[0]
            images = []
            for message_content in final_message.content:
                #print(message_content)
                if message_content.type == 'text':
                    if "Diagnosis:" in message_content.text.value:
                        diagnoses[issue]['text'] = message_content.text.value.split("Diagnosis:")[1]
                    else:
                        diagnoses[issue]['text'] = message_content.text.value
                else:
                    # message is a file
                    file_id = message_content.image_file.file_id
                    image_data = client.files.content(file_id)
                    image_data = image_data.read()
                    local_path = f'images/{file_id}'
                    # extract the actual image content
                    with open(local_path, 'wb') as f:
                        f.write(image_data)
                    image_dict = {}
                    image_dict['local_path'] = local_path
                    image_dict['id'] = file_id
                    images.append(image_dict)

            diagnoses[issue]['images'] = images
            diagnoses[issue]['code_inputs'] = code_inputs
            diagnoses[issue]['code_results'] = code_results

            
    return diagnoses, failed_runs

def format_summary(diagnoses):
    summary = SUMMARY_TEMPLATE
    files = []
    for issue in diagnoses:
        summary += f"{ISSUE_LABELS[issue]}: {diagnoses[issue]['text']}\n"
        if len(diagnoses[issue]['images']) > 0:
            for image in diagnoses[issue]['images']:
                files.append(image['id'])
    return summary, files

def create_summary_prompt(diagnoses):
    summary, files = format_summary(diagnoses)
    message = {
        'role': 'user',
        'content': summary,
        'file_ids': files
    }
    return message

def generate_summary(client, assistant, diagnoses):
    message = create_summary_prompt(diagnoses)
    summary_thread = client.beta.threads.create(
            messages=[message]
    )
    summary_run = client.beta.threads.runs.create(
        thread_id=summary_thread.id,
        assistant_id=assistant.id
    )
    return summary_thread, summary_run

def query_summary_run(client, summary_thread, summary_run):
    status = get_thread_status(client, summary_thread.id, summary_run.id)
    return status

def get_final_summary(client, summary_thread, summary_run):
    status = get_thread_status(client, summary_thread.id, summary_run.id)
    summary = {}
    if status == 'completed':
        summary_content = client.beta.threads.messages.list(thread_id=summary_thread.id).data[0].content
        images = []
        for message_content in summary_content:
            if message_content.type == 'text':
                summary['text'] = message_content.text.value
            else:
                # message is a file
                file_id = message_content.image_file.file_id
                image_data = client.files.content(file_id)
                image_data = image_data.read()
                local_path = f'images/{file_id}'
                # extract the actual image content
                with open(local_path, 'wb') as f:
                    f.write(image_data)
                image_dict = {}
                image_dict['local_path'] = local_path
                image_dict['id'] = file_id
                images.append(image_dict)
        summary['images'] = images
        return summary
    else:
        return None

def setup_chat(client, file_path, selected_issues):
    selected_issues = create_selected_issues(selected_issues)
    file = add_file(client, file_path)
    assistant = create_assistant(client, file.id)
    return assistant, file, selected_issues



def generate_analysis(client, file_path, selected_issues):
    selected_issues = create_selected_issues(selected_issues)
    file = add_file(client, file_path)
    assistant = create_assistant(client, file.id)
    diagnoses, failed_runs = get_all_diagnoses(client, assistant, file.id, selected_issues)
    summary = generate_summary(client, assistant, diagnoses)
    return diagnoses, summary, failed_runs


if __name__ == "__main__":
    client = open_client()
    file_path = 'csv/ior-easy_api_MPIIO_blockSize_1073741824_transferSize_2K_filePerProc_False_uniqueDir_True_REDUCED - Copy.csv'
    diagnoses, summary, failed_runs = generate_analysis(client, file_path)
    print(summary)
    print(failed_runs)



    


