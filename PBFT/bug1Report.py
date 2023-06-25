import os

folder_path = 'results/TwoRequests/Bug1'
report_path = os.path.join(folder_path, 'REPORT.txt')

def count_problem_splits(file_path):
    count = 0
    br = 0
    first = -1
    with open(file_path, 'r') as file:
        content = file.read()
        splits = content.split('----')
        for split in splits:
            br += 1
            if 'Problem' in split:
                count += 1
                if first == -1:
                    first = br
            elif split.strip() == '':
                count += 1
    return (count-1, first)

with open(report_path, "w") as file:
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".txt") and file_name.startswith("REPORT") == False:
            file_path = os.path.join(folder_path, file_name)
            (count, firstBug) = count_problem_splits(file_path)
            file.write("Exploration: " + file_name[:-4] + ", found bugs: " + str(count) + ". First bug at: " + str(firstBug) + "\n" )
            # print(file_name)
            # print(count_problem_splits(file_path))
        # with open(file_path, "r") as file:
        #     # Process the file contents
        #     for line in file:
        #         # Do something with each line
        #         print(line)

# # Provide the file path here
# file_path = 'results/OneRequest/Bug1/RW.txt'
# problem_count = count_problem_splits(file_path)
# print(f"Number of splits containing 'Problem': {problem_count}")