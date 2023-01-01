import os
import json

data_dir_path = "../match_data"

matches = []

matches_file_name = "matches.json"

read_files = "read_file.txt"

if os.path.isfile(matches_file_name):
    with open(matches_file_name, "r") as f:
        lines = f.readlines()

        matches = [json.loads(line) for line in lines]
read_names = []
if os.path.isfile(read_files):
    with open(read_files, "r") as f:
        read_names = [line.rstrip() for line in f.readlines()]
print(f"read names length ===> {len(read_names)}")
files_names = os.listdir(data_dir_path)
print(f"number of matches ===> {len(files_names)}")
files_names = [os.path.join(data_dir_path, name)
               for name in files_names if os.path.join(data_dir_path, name) not in read_names]
print(f"number of matches after clean ===> {len(files_names)}")
with open(read_files, "w") as file:
    file.writelines([n+"\n" for n in read_names])
    for file_name in files_names:
        with open(file_name, "r") as f:
            match = json.load(f)
            matches.append(match["info"])

            file.write(file_name+"\n")
        f.close()
    file.close()

with open(matches_file_name, "w") as f:
    for i in range(len(matches)):
        f.write(json.dumps(matches[i])+"\n")


print("done dumping")
with open(matches_file_name, "r") as f:
    ms = f.readlines()
f.close()
print(len(ms))
with open("sample.json", "w") as f:
    f.writelines(ms[:200])
