import os

folder_path = 'results/OneRequest/Bug2'
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
            if "Faulty" in split:
                count += 1
                if first == -1:
                    first = br
    return (count,first)

with open(report_path, "w") as file:
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".txt") and file_name.startswith("REPORT") == False:
            file_path = os.path.join(folder_path, file_name)
            (count, firstBug) = count_problem_splits(file_path)
            file.write("Exploration: " + file_name[:-4] + ", found bugs: " + str(count) + ". First bug at: " + str(firstBug) + "\n" )
