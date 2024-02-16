import pandas as pd
import json
import re

ISSUES = {
    'small_io': "HPC I/O works in the following way: All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The I/O requests will be transferred using RPC (remote procedure call). If an I/O request is smaller than the RPC size it may be aggregated with others if they are sequential, but otherwise it may lead to inefficient use of the RPC channel since a RPC transfer includes connection building and destroying overheads. The small I/O can mostly be ignored if the application only accesses a file once or twice via small I/O requests, because it is common for an application to load small configuration file, which tends to create small I/O requests. However, repetitive I/O requests to the same file which are significantly smaller than the RPC size may be an issue. Note that, the system on which the trace was collected is configured with a page size of 4kb and max_pages_per_rpc set to 1024, which indicates that the maximum RPC size is 4MB.\n\
                    To diagnose the issue, first analyze the size of the I/O requests, then check to see if any of the requests identified as small are accessing the same files multiple times, and finally check if any of the small requests might be aggregated based on their access patterns. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                    Diagnosis: <summary of your diagnosis>",
    'random_io': "HPC I/O works in the following way: All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The I/O requests will be transferred using RPC (remote procedure call). When applications send I/O requests they are usually buffered in local memory and may be aggregated with other I/O requests if they target nearby pages. If the I/O requests, aggregated or not, are equal to or larger than the maximum RPC size, they fully utilize the RPC channel and the server-side buffer. However, I/O requests which are significantly smaller than the RPC size after aggregation do not use the RPC channel effectively and may lead to I/O performance issues. Additionally, this issue is only of concern when the described behavior is highly repetitive as it does not impact performance if such I/O patterns are only used to access a file a small number of times. Specifically, this type of issue is often referred to as a random access pattern issue.. Note that, the system on which the trace was collected is configured with a page size of 4kb and max_pages_per_rpc set to 1024, which indicates that the maximum RPC size is 4MB.\n\
                    Please use the provided information and think step by step to diagnose whether the attached trace file contains any random access patterns which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                    Diagnosis: <summary of your diagnosis>",
    'misaligned_io': "",
    'load_imbalanced_io': "HPC I/O works in the following way: All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The I/O requests will be transferred using RPC (remote procedure call). If multiple ranks are requesting I/O operations concurrently but their sizes are unbalanced, it may lead some ranks to issue more I/O requests in a given time interval which may become a bottleneck. This is known as a load balance issue. However, in some cases, a set of ranks may process much faster than others so it is beneficial for  these ranks to account for a larger share of I/O requests so it is very important to roughly measure the speed of the various ranks by analyzing throughput or I/O operations per second for each one prior to making any judgements regarding any load balance issues.\n\
                        Please use the provided information and think step by step to diagnose whether the attached trace file contains any load imbalanced I/O behavior which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                        Diagnosis: <summary of your diagnosis>",
    'shared_file_io': "HPC I/O works in the following way. All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The OST servers split data files into chunks which are known as stripes, where each stripe has a size of stripe_size the stripes of a file are stored across stripe_count different OST servers. If the I/O requests target different files, then they are called independent file accesses. If they target different regions of the same file, then they are called shared file accesses. Generally, independent file accesses are more efficient because each process conducts I/O independently towards its own file. But, if there are too many processes reading and writing to too many independent files, the metadata load may become an issue as the load on the metadata servers may be very high. Since shared file accesses only access one file, the metadata servers will not have a very high load, but many processes accessing the same file may complicate data access for the OSTs. This is because these processes may access overlapping areas of the file, introducing conflicts and lock overheads. Even if these requests are not overlapped, they may be accessing the same data stripe of the file (i.e. two request offsets are within the range of the stripe size of the file). This will also lead to lower performance as they introduce conflicts. The I/Os will be sent to the same OST server as well, reducing the parallelism. The files accessed in the application trace have a stripe_size of 1MB and a stripe_count of 1.\n\
                        Please use the provided information and think step by step to diagnose whether the attached trace file contains any shared file I/O behavior which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                        Diagnosis: <summary of your diagnosis>",
    'shared_file_io_extended': "HPC I/O works in the following way. All the ranks (processes) running on different computing nodes will issue multiple I/O requests to different OST servers, which are the storage servers. The OST servers split data files into chunks which are known as stripes, where each stripe has a size of stripe_size the stripes of a file are stored across stripe_count different OST servers. If the I/O requests target different files, then they are called independent file accesses. If they target different regions of the same file, then they are called shared file accesses. Generally, independent file accesses are more efficient because each process conducts I/O independently towards its own file. But, if there are too many processes reading and writing to too many independent files, the metadata load may become an issue as the load on the metadata servers may be very high. Since shared file accesses only access one file, the metadata servers will not have a very high load, but many processes accessing the same file may complicate data access for the OSTs. This is because these processes may access overlapping areas of the file, introducing conflicts and lock overheads. Even if these requests are not overlapped, they may be accessing the same data stripe of the file (i.e. two request offsets are within the range of the stripe size of the file). This will also lead to lower performance as they introduce conflicts. The I/Os will be sent to the same OST server as well, reducing the parallelism. The files accessed in the application trace have a stripe_size of 1MB and a stripe_count of 1.\n\
                        To identify if an application has shared file issue, we need to take following items into consideration\n\
                            1. Many or all processes access the same file. \n\
                            2. Accessing the same file happens repeatedly.\n\
                            3. Requests from different processes are overlapped in terms of time.\n\
                            4. Requests from different processes fall into the same file stripe, i.e., the offsets of these requests are within the stripe size of the file. The stripe size is 1MB.\n\
                        Please use the provided information and think step by step to diagnose whether the attached trace file contains any shared file I/O behavior which may be cause for concern. Following your analysis, write a brief summary of your diagnosis in the following format:\n\
                        Diagnosis: <summary of your diagnosis>"

}

def extract_seq_consec_ops(df):
    # sort by rank and start time
    df.sort_values(by=['rank', 'index'], inplace=True)
    df['shifted_operation'] = df['operation'].shift(1)
    df['shifted_offset'] = df['offset'].shift(1)
    df['shifted_size'] = df['size'].shift(1)
    # if operations are of same type and offset is greater than previous end then they are consecutive
    df['consec'] = df.apply(lambda x: True if x['operation'] == x['shifted_operation'] and x['offset'] >= x['shifted_offset']+x['shifted_size'] else False, axis=1)
    # if offset is equal to previous offset+size then they are sequential
    df['seq'] = df.apply(lambda x: True if x['offset'] == x['shifted_offset']+x['shifted_size'] else False, axis=1)
    # remove shifted columns
    df.drop(columns=['shifted_operation', 'shifted_offset', 'shifted_size'], inplace=True)

    return df


def parse_darshan_txt(txt_output):
    # Lists to store extracted data
    file_ids = []
    file_names = []
    apis = []
    ranks = []
    operations = []
    segments = []
    offsets = []
    sizes = []
    starts = []
    ends = []
    osts = []
    # Variables to hold temporary data
    current_file_id = None
    current_file_name = None
    current_rank = None
    current_api = 'POSIX'
    trace_start_time = None
    
    for line in txt_output.splitlines():
        # Extract start time
        if line.startswith("# start_time:"):
            trace_start_time = float(line.split(':')[1].strip())
        
        if line.startswith("# run time:"):
            full_runtime = float(line.split(':')[1].strip())

        # Extract file_id
        if line.startswith("# DXT, file_id:"):
            current_file_id = line.split(':')[1].split(',')[0].strip()
            current_file_name = line.split(':')[2].strip()
        

            
        # Extract rank
        if line.startswith("# DXT, rank:"):
            current_rank = line.split(':')[1].split(',')[0].strip()
            
        # Extract IO operation details
        if not line.startswith("#") and current_file_id and current_rank:
            parts = line.split()
            # Check if the line has the expected number of fields
            if len(parts) < 8:
                continue
            operations.append(parts[2])
            ranks.append(current_rank)
            file_ids.append(current_file_id)
            file_names.append(current_file_name)
            apis.append(current_api)
            segments.append(int(parts[3]))
            if parts[4] == 'N/A':
                offsets.append(0)
            else:
                offsets.append(int(parts[4]))
            if parts[5] == 'N/A':
                sizes.append(0)
            else:
                sizes.append(int(parts[5]))
            starts.append(float(parts[6]) + trace_start_time)
            ends.append(float(parts[7]) + trace_start_time)
            if len(parts) >= 9:
                ost_info = ','.join(parts[9:]).replace(']', '')
                osts.append(ost_info)
            else:
                osts.append('')
                
    # Create DataFrame
    df = pd.DataFrame({
        'file_id': file_ids,
        'file_name': file_names,
        'api': apis,
        'rank': ranks,
        'operation': operations,
        'segment': segments,
        'offset': offsets,
        'size': sizes,
        'start': starts,
        'end': ends,
        'ost': osts
    })
    df = pd.DataFrame.from_dict(df).sort_values(by=['start'])
    df.reset_index(inplace=True)
    # keep only 1000 operations per rank and operation type
    df = df.groupby(['rank', 'operation']).head(10000)
    
    return df, trace_start_time, full_runtime

def create_prompt(file, df, issue):
    column_description = {
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
    #header = parse_darshan_log_header(file)

    prompt = f"""
        I have attached a csv file which you can load into a dataframe using pandas. The csv contains I/O trace information from an application run on an HPC system and the data was collected using darshan. The data contains the following columns:

        {column_description}

        {ISSUES[issue]}
    """
    return prompt


def parse_darshan_log_header(log_file):
    data = {}
    metadata = []
    log_file_regions = []
    #mounted_file_systems = []
    
    with open(log_file, 'r') as f:
        log_text = f.read()
    lines = log_text.strip().split('\n')
    for line in lines:
        if "# DXT_POSIX module data" in line:
            break
        if line.startswith('#'):
            # Splitting only on the first occurrence of ': ' to handle cases where the value contains ': '
            parts = line[2:].split(': ', 1)
            if len(parts) == 2:
                key, value = parts
                # Processing different types of lines
                if key == 'metadata':
                    meta_parts = value.split(' = ')
                    if len(meta_parts) == 2:
                        meta_key, meta_value = meta_parts
                        metadata.append({meta_key: meta_value})
                elif 'module' in key:
                    module_parts = value.split(', ')
                    module_info = module_parts[0]
                    version_info = module_parts[1] if len(module_parts) > 1 else None
                    module_key = key.replace(' ', '_')
                    module_dict = {module_key: module_info}
                    if version_info:
                        version = int(version_info.split('=')[1])
                        module_dict['ver'] = version
                    log_file_regions.append(module_dict)
                else:
                    # Convert numeric values to integers or floats
                    if value.isdigit():
                        value = int(value)
                    elif re.match(r"^\d+\.\d+$", value):
                        value = float(value)
                    data[key.replace(' ', '_')] = value
                """
                elif key.startswith('mount entry'):
                    # Adjusting the parsing for mount entries
                    mount_info = value.rsplit('\t', 1)
                    if len(mount_info) == 2:
                        mount_entry, fs_type = mount_info
                        mounted_file_systems.append({"mount_entry": mount_entry.strip(), "fs_type": fs_type.strip()})
                """

    data['metadata'] = metadata
    data['log_file_regions'] = log_file_regions
    #data['mounted_file_systems'] = mounted_file_systems

    return json.dumps(data, indent=4)


def parse_to_df(log_file):
    df, trace_start_time, full_runtime = parse_darshan_txt(log_file)
    df = extract_seq_consec_ops(df)
    return df, trace_start_time, full_runtime


if __name__ == '__main__':
    # Read txt file
    file_name = 'ior-easy_api_POSIX_blockSize_1073741824_transferSize_2k_filePerProc_True_uniqueDir_True__0.txt'
    file = open(file_name, 'r')
    txt_output = file.read()
    file.close()

    # Parse txt file
    df, trace_start_time, full_runtime = parse_darshan_txt(txt_output)
    # Extract consecutive operations
    df = extract_seq_consec_ops(df)
    print(df)
    # save to csv
    df.to_csv(f'csv/{file_name.split(".")[0]}.csv', index=False)
    # create prompt
    prompt = create_prompt(file_name, df, 'shared_file_io_extended')
    print(prompt)

